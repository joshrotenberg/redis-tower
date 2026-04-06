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

pub mod auto_pipeline;
pub mod cache_layer;
pub mod caching;
mod client;
pub mod command_adapter;
pub mod consumer;
mod executor;
pub mod metrics_layer;
pub mod pipeline;
pub mod pubsub;
pub mod reconnect;
pub mod reconnect_layer;
mod resilient;
pub mod script;
pub mod tracing_layer;
pub mod transaction;

pub use auto_pipeline::{AutoPipelineConfig, AutoPipelineService};
pub use cache_layer::CacheService;
pub use consumer::{ConsumerConfig, StreamConsumer, StreamMessage};
pub use caching::CachedClient;
pub use client::RedisClient;
pub use command_adapter::CommandAdapter;
pub use executor::RedisExecutor;
pub use pipeline::{Pipeline, PipelineResults};
pub use pubsub::{MessageKind, PubSubConnection, PubSubMessage};
pub use metrics_layer::{MetricsLayer, MetricsRecorder, MetricsService};
pub use reconnect::ResilientConnection;
pub use reconnect_layer::ReconnectService;
pub use resilient::ResilientRedisClient;
pub use script::Script;
pub use tracing_layer::{TracingLayer, TracingService};
pub use transaction::{Transaction, TransactionResult};

// Re-export core types.
pub use redis_tower_core::{
    Command, Frame, FrameService, FromRedisBytes, RedisConnection, RedisConvert, RedisError,
    RedisStream, RedisValueExt, RespCodec,
};

// Re-export TLS config when a TLS backend is enabled.
#[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
pub use redis_tower_core::tls::TlsConfig;

// Re-export commands under a `commands` module.
pub mod commands {
    pub use redis_tower_commands::*;
}
