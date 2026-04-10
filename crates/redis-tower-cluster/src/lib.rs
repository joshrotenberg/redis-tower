//! Redis Cluster support for redis-tower.
//!
//! This crate provides cluster-aware routing that directs commands to the
//! correct node based on the key's hash slot.
//!
//! # Slot Routing
//!
//! Redis Cluster partitions the keyspace into 16384 hash slots. This crate
//! computes the slot for each command's key (respecting `{hash_tag}` notation)
//! and routes the command to the node that owns that slot. See [`slot`] for
//! the hashing utilities.
//!
//! # Topology Discovery
//!
//! [`ClusterConnection`] discovers the cluster layout by issuing
//! `CLUSTER SLOTS` to a seed node, then maintains persistent connections to
//! each master (and optionally replica) node. The topology is refreshed
//! automatically when redirects indicate slot ownership has changed. See
//! [`topology`] for the discovery types.
//!
//! # Redirect Handling
//!
//! MOVED and ASK redirects are handled transparently. A MOVED response
//! triggers a topology refresh and retries the command on the new owner. An
//! ASK response sends the command to the indicated node after issuing an
//! ASKING command, without refreshing the topology.
//!
//! # Read Preference
//!
//! [`ReadPreference`] controls whether read-only commands are routed to
//! masters, replicas, or replicas with a master fallback.
//!
//! # Read Routing Strategy
//!
//! When reads are directed to replicas, the [`ReadRoutingStrategy`] trait
//! determines which replica is selected. Built-in strategies include
//! [`RoundRobinRouting`] (default), [`RandomRouting`], and
//! [`FirstReplicaRouting`]. Custom strategies can be provided via the
//! builder's [`ClusterConnectionBuilder::read_routing`] method.

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
