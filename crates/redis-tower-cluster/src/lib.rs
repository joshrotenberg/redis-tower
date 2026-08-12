//! Redis Cluster support for redis-tower.
//!
//! This crate provides cluster-aware routing that directs commands to the
//! correct node based on the key's hash slot. It ships a serialized client, a
//! high-concurrency client, and a cached high-concurrency wrapper; pick one
//! based on your workload.
//!
//! # Which client to use
//!
//! | You need... | Use |
//! |---|---|
//! | Simple one-task-at-a-time usage, lowest moving parts | [`ClusterClient`] |
//! | High-concurrency sharing across many tokio tasks | [`MultiplexedClusterClient`] |
//! | High-concurrency reads with server-assisted local caching | [`CachedMultiplexedClusterClient`] |
//! | Automatic per-node reconnect on failover | [`MultiplexedClusterClient`] |
//! | Credential rotation across reconnects | [`MultiplexedClusterClient`] |
//! | Per-node background auto-pipelining of concurrent requests | [`MultiplexedClusterClient`] |
//!
//! # Pipelines and transactions
//!
//! [`ClusterPipeline`] preserves the typed [`redis_tower::Pipeline`] result
//! surface while executing through [`MultiplexedClusterClient`]. It pins
//! commands to masters from one topology snapshot, preserves submission order
//! within each node batch, sends different node batches concurrently, and
//! restores the original result order. There is no total execution order
//! across slots. A redirect retries only the affected command; a transport
//! failure or cancellation after dispatch is ambiguous because another node's
//! batch may already have executed.
//!
//! Cross-slot `MGET`, `MSET`, and `DEL` are available through the explicit
//! [`MultiplexedClusterClient::mget_split`],
//! [`MultiplexedClusterClient::mset_split`], and
//! [`MultiplexedClusterClient::del_split`] helpers. `MSET` and `DEL` are atomic
//! only within one hash-slot group and can partially apply if another group
//! fails.
//!
//! [`ClusterConnection`] and [`ClusterClient`] implement
//! [`redis_tower::TransactionExecutor`]. They validate every WATCH and command
//! key before I/O, reject mixed slots, and pin the complete MULTI/EXEC exchange
//! to the owning master. [`redis_tower::Transaction::watch`] can protect a body
//! that is already known. The closure-based `redis_tower::transaction` helpers
//! are rejected before I/O because their separate WATCH/read/build calls cannot
//! reserve one node connection, so read/compute/build optimistic locking is
//! unsupported. Transactions are not currently exposed on
//! [`MultiplexedClusterClient`]; use [`ClusterConnection`] or [`ClusterClient`].
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
//! ## [`CachedMultiplexedClusterClient`]
//!
//! Wraps [`MultiplexedClusterClient`] with one clone-shared, slot-aware local
//! cache and a RESP3 invalidation receiver for every current master. Cache use
//! fails closed during node loss, redirects, and topology/coverage rebuilds.
//! This initial surface is master-only and rejects replica read preferences.
//! It also requires a finite `CachedClientConfig::client_ttl` so an unobserved
//! slot-owner change cannot leave an old cache entry unbounded.
//!
//! # Slot Routing
//!
//! Redis Cluster partitions the keyspace into 16384 hash slots. All clients
//! compute the slot for each command's key (respecting
//! `{hash_tag}` notation) and route the command to the node that owns
//! that slot. See [`slot`] for the hashing utilities.
//!
//! # Topology Discovery
//!
//! The ordinary clients discover the cluster layout by issuing `CLUSTER SLOTS`
//! to a seed node, then maintain connections to each master (and optionally
//! replica) node. The cached wrapper uses the same discovery and refresh path
//! while requiring complete master coverage. Topology is refreshed
//! automatically on MOVED redirects. See [`topology`] for the discovery types.
//!
//! # Redirect Handling
//!
//! Ordinary commands and cluster pipelines handle MOVED and ASK redirects
//! transparently. MOVED triggers a topology patch and retries against the new
//! owner. ASK sends `ASKING` followed by the command on the same connection -- for
//! [`MultiplexedClusterClient`], that happens via
//! [`AutoPipelineService::call_pipeline`](redis_tower::AutoPipelineService::call_pipeline),
//! which guarantees the two frames land contiguously on the wire with
//! no interleaving from other concurrent callers. Transactions never replay a
//! redirect because Redis may already have observed WATCH or MULTI; MOVED still
//! updates topology for a freshly built future transaction before the error is
//! returned.
//!
//! # Cluster-wide SCAN
//!
//! `SCAN` iterates one node's keyspace and carries no key, so slot routing
//! sends it to the default node and it returns a fraction of the cluster's
//! keys. [`ScanClusterStream`] runs the cursor loop against every master in
//! turn and yields each key tagged with its source node; [`ClusterScan`] is the
//! configurable form, which can page several masters at once and can re-check
//! cluster membership as it goes. See [`scan_stream`] for what ordering and
//! resharding guarantees each gives.
//!
//! # Read Preference
//!
//! [`ReadPreference`] controls whether read-only commands are routed to
//! masters, replicas, or replicas with a master fallback. [`ClusterClient`]
//! and [`MultiplexedClusterClient`] honor it. The cached client currently
//! requires [`ReadPreference::Master`] and rejects the replica variants.
//! [`ReadPreference::Replica`] is strict and returns an error when no usable
//! replica is available for the key's slot; [`ReadPreference::PreferReplica`]
//! falls back to the master. Writes always use the master.
//!
//! # Read Routing Strategy
//!
//! When reads are directed to replicas, the [`ReadRoutingStrategy`] trait
//! determines which replica is selected. Built-in strategies include
//! [`RoundRobinRouting`] (default), [`RandomRouting`], and
//! [`FirstReplicaRouting`]. Custom strategies can be provided through the two
//! ordinary clients' builders; the master-only cached builder does not expose
//! replica routing.
//!
//! # Authentication
//!
//! [`MultiplexedClusterClient`] and [`CachedMultiplexedClusterClient`] accept a
//! [`CredentialProvider`](redis_tower::credentials::CredentialProvider) via
//! `.credentials(provider)` on its builder. The provider is consulted on
//! initial connect and on every reconnect, so credential rotation flows
//! through automatically.
//!
//! # Protocol configuration
//!
//! The ordinary cluster builders accept a complete
//! [`ConnectionConfig`](redis_tower_core::ConnectionConfig) and an explicit
//! `.protocol(...)` override. The cached builder accepts the same connection
//! config but forces RESP3 for invalidation pushes. Protected nodes
//! authenticate while still in RESP2, then negotiate the requested protocol
//! before role-specific setup; the same ordering is replayed for discovery,
//! redirects, refreshes, and reconnects.
//!
//! # TLS
//!
//! [`MultiplexedClusterClient`] and [`CachedMultiplexedClusterClient`] support
//! TLS behind the `tls-rustls` or `tls-native-tls` feature. Pass a `TlsConfig` (from
//! `redis_tower_core::tls`) via `.tls(config)` on the builder -- the
//! seed connection used for topology discovery as well as every per-node
//! factory will speak TLS on each (re)connect. The SNI hostname is taken
//! from the host portion of each node's address; combine with
//! `.host_override(host)` if your nodes report IPs that don't match your
//! certificate.

#![forbid(unsafe_code)]

mod caching;
mod client;
mod connection;
pub mod key_extractor;
mod multiplexed;
pub mod pipeline;
mod pubsub;
pub mod scan_stream;
pub mod slot;
pub mod topology;

pub use caching::{CachedMultiplexedClusterClient, CachedMultiplexedClusterClientBuilder};
pub use client::ClusterClient;
pub use connection::{
    ClusterConnection, ClusterConnectionBuilder, FirstReplicaRouting, RandomRouting,
    ReadPreference, ReadRoutingStrategy, RoundRobinRouting,
};
pub use multiplexed::{MultiplexedClusterClient, MultiplexedClusterClientBuilder};
pub use pipeline::ClusterPipeline;
pub use pubsub::{ClusterPubSubConnection, ShardedClusterPubSubConnection};
pub use scan_stream::{
    ClusterScan, ClusterScanItem, MAX_MEMBERSHIP_ROUNDS, MAX_SCAN_CONCURRENCY, ScanClusterStream,
};
pub use slot::{SLOT_COUNT, extract_hash_tag, slot_for_key};
pub use topology::{ClusterTopology, NodeAddr, SlotRange};
