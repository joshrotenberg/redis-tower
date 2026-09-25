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
//! - [`strings`] and [`keys`] -- values, counters, expiry, and key lifecycle.
//! - [`hashes`], [`lists`], [`sets`], and [`sorted_sets`] -- collection data.
//! - [`streams`] and [`blocking`] -- logs, consumer groups, and isolated waits.
//! - [`scan`] -- cursor-based iteration without blocking the server.
//! - [`transaction`] and [`scripting`] -- atomic batches and server-side logic.
//! - [`pubsub`] -- publishing and introspection; subscriptions use the
//!   higher-level `redis_tower::PubSubConnection` API.
//! - [`server`], [`acl`], [`cluster`], and [`diagnostics`] -- administration
//!   and diagnostics.
//! - [`geo`], [`hyperloglog`], [`bitmap`], and [`mod@array`] -- specialized native
//!   data structures (arrays require Redis 8.8+).
//! - Feature-gated [Bloom], [sketch], [T-Digest], [JSON], [Search],
//!   [TimeSeries], and [Vector Set] pages -- Redis Stack and Redis 8 families.
//!
//! Every category page explains its response shapes and prerequisites and has
//! a compiling example. The repository's [command cookbook] connects those
//! command builders to client choice, binary data, transactions, streams,
//! custom commands, and dedicated session ownership.
//!
//! [command cookbook]: https://github.com/joshrotenberg/redis-tower/blob/main/docs/COMMAND-COOKBOOK.md
//! [Bloom]: https://docs.rs/redis-tower-commands/latest/redis_tower_commands/bloom/
//! [sketch]: https://docs.rs/redis-tower-commands/latest/redis_tower_commands/sketch/
//! [T-Digest]: https://docs.rs/redis-tower-commands/latest/redis_tower_commands/tdigest/
//! [JSON]: https://docs.rs/redis-tower-commands/latest/redis_tower_commands/json/
//! [Search]: https://docs.rs/redis-tower-commands/latest/redis_tower_commands/search/
//! [TimeSeries]: https://docs.rs/redis-tower-commands/latest/redis_tower_commands/timeseries/
//! [Vector Set]: https://docs.rs/redis-tower-commands/latest/redis_tower_commands/vector_sets/
//!
//! # Builder Pattern
//!
//! Commands with optional parameters use builder methods that take and return
//! `Self` for fluent configuration:
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower_commands::Set;
//! use redis_tower_core::RedisConnection;
//!
//! let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
//!
//! let cmd = Set::new("key", "value")
//!     .ex(60)       // expire in 60 seconds
//!     .nx();        // only set if key does not exist
//!
//! let previous = conn.execute(cmd).await?;
//! # let _ = previous;
//! # Ok(())
//! # }
//! ```
//!
//! All command structs are re-exported at the crate root for convenience.
//!
//! # Binary-safe inputs
//!
//! Redis keys and stored data are arbitrary bytes. Opaque arguments across the
//! core and feature-gated command families accept strings or bytes through
//! [`CommandArg`]:
//!
//! ```
//! use redis_tower_commands::{Get, HSet, Set};
//! use redis_tower_core::Command;
//!
//! let key = b"user:\xff".as_slice();
//! let set = Set::new(key, vec![0x00, 0xfe, 0xff]);
//! let get = Get::new(key);
//! let hash = HSet::new(key, b"field".as_slice(), b"value\xff".as_slice());
//!
//! // Builders preserve exact bytes in their RESP frames.
//! let _ = (set.to_frame(), get.to_frame(), hash.to_frame());
//! ```
//!
//! Owned strings and vectors move into the command, [`bytes::Bytes`] shares
//! storage, and borrowed inputs are copied once. Redis grammar such as cursors,
//! JSONPath, Search query syntax, stream IDs, addresses, and SHA-1 digests stays
//! text- or number-typed. [`RawCommand`] remains the byte-oriented escape hatch
//! for extension commands outside the typed surface.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

// -- Core Redis commands (always available) --
pub mod acl;
mod arg;
pub mod array;
pub mod bitmap;
pub mod blocking;
pub mod cluster;
pub mod diagnostics;
pub mod geo;
pub mod hashes;
mod help;
pub mod hyperloglog;
pub mod keys;
pub mod lists;
pub mod pubsub;
pub mod raw;
pub mod scan;
pub mod scripting;
pub mod server;
pub mod sets;
pub mod sorted_sets;
pub mod streams;
pub mod strings;
pub mod transaction;

pub use acl::*;
pub use arg::*;
pub use array::*;
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
pub use transaction::*;

// -- Redis Stack module commands (feature-gated) --
#[cfg(feature = "bloom")]
pub mod bloom;
#[cfg(feature = "bloom")]
#[cfg_attr(docsrs, doc(cfg(feature = "bloom")))]
pub use bloom::*;

#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "json")]
#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
pub use json::*;

#[cfg(feature = "search")]
pub mod search;
#[cfg(feature = "search")]
#[cfg_attr(docsrs, doc(cfg(feature = "search")))]
pub use search::*;
#[cfg(feature = "search")]
mod search_util;

#[cfg(feature = "sketch")]
pub mod sketch;
#[cfg(feature = "sketch")]
#[cfg_attr(docsrs, doc(cfg(feature = "sketch")))]
pub use sketch::*;

#[cfg(feature = "tdigest")]
pub mod tdigest;
#[cfg(feature = "tdigest")]
#[cfg_attr(docsrs, doc(cfg(feature = "tdigest")))]
pub use tdigest::*;

#[cfg(feature = "timeseries")]
pub mod timeseries;
#[cfg(feature = "timeseries")]
#[cfg_attr(docsrs, doc(cfg(feature = "timeseries")))]
pub use timeseries::*;

#[cfg(feature = "vector-sets")]
pub mod vector_sets;
#[cfg(feature = "vector-sets")]
#[cfg_attr(docsrs, doc(cfg(feature = "vector-sets")))]
pub use vector_sets::*;

#[cfg(test)]
mod clone_coverage {
    //! Every command builder derives `Clone` so typed commands can flow through
    //! Tower `Retry`/`Hedge` layers, which require `Req: Clone`. This asserts a
    //! representative command from each core group; a missing derive on any of
    //! them fails to compile here.
    use crate::{
        ArGet, BitCount, ClusterInfo, Del, Get, HGet, LPush, Ping, SAdd, Scan, Set, XAdd, ZAdd,
    };

    fn assert_clone<T: Clone>() {}

    #[test]
    fn command_builders_are_clone() {
        assert_clone::<Get>();
        assert_clone::<Set>();
        assert_clone::<Del>();
        assert_clone::<HGet>();
        assert_clone::<LPush>();
        assert_clone::<SAdd>();
        assert_clone::<ZAdd>();
        assert_clone::<XAdd>();
        assert_clone::<Ping>();
        assert_clone::<Scan>();
        assert_clone::<ClusterInfo>();
        assert_clone::<BitCount>();
        assert_clone::<ArGet>();
    }
}
