//! redis-tower
//!
//! An experimental Tower-based Redis client with strong typing and composable middleware.

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod client;
pub mod cluster;
pub mod codec;
pub mod commands;
pub mod pipeline;
pub mod pool;
pub mod pubsub;
pub mod sentinel;
pub mod transaction;
pub mod types;

// Redis Stack modules (feature-gated)
#[cfg(feature = "modules")]
pub mod modules;

// Re-exports for convenience
pub use client::RedisClient;
pub use commands::Command;
pub use pipeline::{Pipeline, PipelineResults};
pub use pubsub::{PubSubConnection, PubSubMessage};
pub use transaction::{Discard, Exec, Multi, Transaction, Unwatch, Watch};
pub use types::{RedisError, RedisValue};
