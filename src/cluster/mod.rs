//! Redis Cluster support
//!
//! Provides a cluster-aware client that automatically:
//! - Discovers cluster topology
//! - Routes commands to the correct node based on key slot
//! - Handles MOVED and ASK redirects
//! - Maintains connection pools to all cluster nodes

pub mod client;
pub mod commands;
pub mod key_extractor;
pub mod slots;

pub use client::{ClusterClient, KeyExtractor};
pub use commands::{Asking, ClusterInfo, ClusterNodes, ClusterSlots, NodeInfo, SlotRange};
pub use slots::{SlotMap, slot_for_key};
