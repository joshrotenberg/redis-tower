//! Redis Sentinel support for redis-tower.
//!
//! Provides automatic master discovery and failover handling via Redis
//! Sentinel. Commands are always sent to the current master; when the
//! master changes due to failover, the connection is automatically
//! redirected.

mod client;
mod connection;
pub mod discovery;

pub use client::SentinelClient;
pub use connection::SentinelConnection;
