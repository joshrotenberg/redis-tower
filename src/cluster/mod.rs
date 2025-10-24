//! Redis Cluster support
//!
//! Provides a cluster-aware client that automatically:
//! - Discovers cluster topology
//! - Routes commands to the correct node based on key slot
//! - Handles MOVED and ASK redirects
//! - Maintains connection pools to all cluster nodes
//! - Supports read-from-replica for improved read throughput

pub mod client;
pub mod commands;
pub mod key_extractor;
pub mod read_preference;
pub mod slots;

pub use client::{ClusterClient, KeyExtractor};
pub use commands::{Asking, ClusterInfo, ClusterNodes, ClusterSlots, NodeInfo, SlotRange};
pub use read_preference::{ReadOnly, ReadPreference};
pub use slots::{SlotAssignment, SlotMap, slot_for_key};
