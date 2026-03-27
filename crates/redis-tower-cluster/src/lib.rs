//! Redis Cluster support for redis-tower.
//!
//! This crate provides cluster-aware routing that directs commands to
//! the correct node based on the key's hash slot.

mod connection;
pub mod key_extractor;
pub mod slot;
pub mod topology;

pub use connection::ClusterConnection;
pub use slot::{SLOT_COUNT, extract_hash_tag, slot_for_key};
pub use topology::{ClusterTopology, NodeAddr, SlotRange};
