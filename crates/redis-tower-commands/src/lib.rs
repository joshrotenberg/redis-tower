//! Typed Redis command implementations for redis-tower.
//!
//! Each command is a struct implementing [`redis_tower_core::Command`] with a
//! strongly-typed `Response`. Commands are organized by category into modules
//! mirroring the Redis command groups.
//!
//! # Organization
//!
//! Commands are grouped by category:
//!
//! - **strings** -- `Get`, `Set`, `Incr`, `Append`, `MGet`, `MSet`, etc.
//! - **keys** -- `Del`, `Exists`, `Expire`, `Ttl`, `Rename`, `Type`, etc.
//! - **hashes** -- `HGet`, `HSet`, `HGetAll`, `HDel`, `HIncrBy`, etc.
//! - **lists** -- `LPush`, `RPush`, `LPop`, `RPop`, `LRange`, `LLen`, etc.
//! - **sets** -- `SAdd`, `SMembers`, `SRem`, `SIsMember`, `SUnion`, etc.
//! - **sorted_sets** -- `ZAdd`, `ZRange`, `ZRank`, `ZScore`, etc.
//! - **streams** -- `XAdd`, `XRead`, `XRange`, `XAck`, `XGroup`, etc.
//! - **pubsub** -- `Publish`, `Subscribe`, `Unsubscribe`
//! - **scripting** -- `Eval`, `EvalSha`, `ScriptLoad`, `ScriptExists`
//! - **server** -- `Ping`, `FlushDb`, `FlushAll`, `DbSize`, `Info`, etc.
//! - **geo** -- `GeoAdd`, `GeoSearch`, `GeoDist`, etc.
//! - **hyperloglog** -- `PfAdd`, `PfCount`, `PfMerge`
//! - **bitmap** -- `SetBit`, `GetBit`, `BitCount`, `BitOp`, etc.
//! - **blocking** -- `BLPop`, `BRPop`, `BLMove`, `BZPopMin`, etc.
//! - **scan** -- `Scan`, `HScan`, `SScan`, `ZScan`
//! - **acl** -- ACL management commands
//! - **cluster** -- `ClusterInfo`, `ClusterSlots`, etc.
//! - **diagnostics** -- `SlowlogGet`, `DebugSleep`, `MemoryUsage`, etc.
//! - **bloom** / **sketch** / **tdigest** / **json** / **search** /
//!   **timeseries** / **vector_sets** -- Redis module commands
//!
//! # Builder Pattern
//!
//! Commands with optional parameters use builder methods that return `&mut Self`
//! for fluent configuration:
//!
//! ```ignore
//! use redis_tower::commands::Set;
//!
//! let cmd = Set::new("key", "value")
//!     .ex(60)       // expire in 60 seconds
//!     .nx();        // only set if key does not exist
//! ```
//!
//! All command structs are re-exported at the crate root for convenience.

// -- Core Redis commands (always available) --
mod acl;
mod bitmap;
mod blocking;
mod cluster;
mod diagnostics;
mod geo;
mod hashes;
mod hyperloglog;
mod keys;
mod lists;
mod pubsub;
mod raw;
mod scan;
mod scripting;
mod server;
mod sets;
mod sorted_sets;
mod streams;
mod strings;

pub use acl::*;
pub use bitmap::*;
pub use blocking::*;
pub use cluster::*;
pub use diagnostics::*;
pub use geo::*;
pub use hashes::*;
pub use hyperloglog::*;
pub use keys::*;
pub use lists::*;
pub use pubsub::*;
pub use raw::*;
pub use scan::*;
pub use scripting::*;
pub use server::*;
pub use sets::*;
pub use sorted_sets::*;
pub use streams::*;
pub use strings::*;

// -- Redis Stack module commands (feature-gated) --
#[cfg(feature = "bloom")]
mod bloom;
#[cfg(feature = "bloom")]
pub use bloom::*;

#[cfg(feature = "json")]
mod json;
#[cfg(feature = "json")]
pub use json::*;

#[cfg(feature = "search")]
mod search;
#[cfg(feature = "search")]
pub use search::*;
#[cfg(feature = "search")]
mod search_util;
#[cfg(feature = "search")]
pub use search_util::*;

#[cfg(feature = "sketch")]
mod sketch;
#[cfg(feature = "sketch")]
pub use sketch::*;

#[cfg(feature = "tdigest")]
mod tdigest;
#[cfg(feature = "tdigest")]
pub use tdigest::*;

#[cfg(feature = "timeseries")]
mod timeseries;
#[cfg(feature = "timeseries")]
pub use timeseries::*;

#[cfg(feature = "vector-sets")]
mod vector_sets;
#[cfg(feature = "vector-sets")]
pub use vector_sets::*;
