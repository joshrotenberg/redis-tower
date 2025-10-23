//! redis-tower
//!
//! An experimental Tower-based Redis client with strong typing and composable middleware.

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod client;
pub mod codec;
pub mod commands;
pub mod types;

// Re-exports for convenience
pub use client::RedisClient;
pub use commands::Command;
pub use types::{RedisError, RedisValue};
