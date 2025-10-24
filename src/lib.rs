//! redis-tower
//!
//! An experimental Tower-based Redis client with strong typing and composable middleware.

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod client;
pub mod cluster;
pub mod codec;
pub mod commands;
pub mod pool;
pub mod pubsub;
pub mod transaction;
pub mod types;

// Re-exports for convenience
pub use client::RedisClient;
pub use commands::Command;
pub use pubsub::{PubSubConnection, PubSubMessage};
pub use transaction::{Transaction, Unwatch, Watch};
pub use types::{RedisError, RedisValue};
