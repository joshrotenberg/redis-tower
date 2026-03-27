//! A Tower-based Redis client with strong typing and composable middleware.
//!
//! redis-tower provides a Redis client where every connection is a
//! `tower::Service`, commands are typed request/response pairs, and
//! resilience is composed via standard Tower layers.
//!
//! # Quick Start
//!
//! ```ignore
//! use redis_tower::{RedisClient, commands::*};
//!
//! let client = RedisClient::connect("127.0.0.1:6379").await?;
//! client.execute(Set::new("key", "value")).await?;
//! let value = client.execute(Get::new("key")).await?;
//! ```
//!
//! # Tower Middleware
//!
//! Since [`RedisConnection`] implements `Service`, you can compose it with
//! any Tower middleware:
//!
//! ```ignore
//! use tower::ServiceBuilder;
//! use tower::timeout::TimeoutLayer;
//! use tower::buffer::BufferLayer;
//! use redis_tower::RedisConnection;
//!
//! let conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let svc = ServiceBuilder::new()
//!     .layer(BufferLayer::new(64))
//!     .layer(TimeoutLayer::new(Duration::from_secs(5)))
//!     .service(conn);
//! ```

mod client;
pub mod pipeline;
pub mod pubsub;
pub mod reconnect;
mod resilient;
pub mod transaction;

pub use client::RedisClient;
pub use pipeline::{Pipeline, PipelineResults};
pub use pubsub::{MessageKind, PubSubConnection, PubSubMessage};
pub use reconnect::ResilientConnection;
pub use resilient::ResilientRedisClient;
pub use transaction::{Transaction, TransactionResult};

// Re-export core types.
pub use redis_tower_core::{Command, Frame, RedisConnection, RedisError, RedisStream, RespCodec};

// Re-export TLS config when a TLS backend is enabled.
#[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
pub use redis_tower_core::tls::TlsConfig;

// Re-export commands under a `commands` module.
pub mod commands {
    pub use redis_tower_commands::*;
}
