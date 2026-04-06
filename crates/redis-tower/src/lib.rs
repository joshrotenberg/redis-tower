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
//!
//! # Features
//!
//! - **Typed commands** -- every Redis command is a struct with compile-time
//!   type safety for both request encoding and response parsing (see
//!   [`commands`]).
//! - **Pipelines and transactions** -- batch commands with [`Pipeline`] or
//!   wrap them in MULTI/EXEC with [`Transaction`].
//! - **Pub/Sub** -- dedicated [`PubSubConnection`] for subscribe/publish
//!   workflows.
//! - **Scripting** -- [`Script`] handles EVALSHA with automatic EVAL fallback
//!   and script loading.
//! - **Auto-pipelining** -- [`AutoPipelineService`] transparently batches
//!   concurrent requests into Redis pipelines.
//! - **Resilience layers** -- [`ReconnectService`] for automatic reconnection,
//!   [`MetricsLayer`] and [`TracingLayer`] for observability, and
//!   [`CacheService`] for client-side caching.
//! - **TLS** -- enable `tls-native-tls` or `tls-rustls` features for
//!   encrypted connections.
//!
//! # Crate Structure
//!
//! This is the facade crate. It re-exports types from:
//! - `redis-tower-protocol` -- RESP3 frame types and codec
//! - `redis-tower-core` -- [`Command`] trait, [`RedisConnection`], transport
//! - `redis-tower-commands` -- typed command implementations (via [`commands`])

pub mod auto_pipeline;
pub mod cache_layer;
pub mod caching;
mod client;
pub mod command_adapter;
pub mod consumer;
pub mod credentials;
mod executor;
pub mod metrics_layer;
pub mod pipeline;
pub mod pool;
pub mod pubsub;
pub mod reconnect;
pub mod reconnect_layer;
mod resilient;
pub mod script;
pub mod tracing_layer;
pub mod transaction;

#[cfg(feature = "serde")]
pub mod json_api;
#[cfg(feature = "serde")]
pub mod search_api;

pub use auto_pipeline::{AutoPipelineConfig, AutoPipelineService};
pub use cache_layer::CacheService;
pub use caching::CachedClient;
pub use client::RedisClient;
pub use command_adapter::CommandAdapter;
pub use consumer::{ConsumerConfig, StreamConsumer, StreamMessage};
pub use credentials::{
    AuthenticatedConnection, CredentialProvider, Credentials, StaticCredentials,
};
pub use executor::RedisExecutor;
pub use metrics_layer::{MetricsLayer, MetricsRecorder, MetricsService};
pub use pipeline::{Pipeline, PipelineResults};
pub use pool::{ConnectionPool, DispatchStrategy, PoolConfig};
pub use pubsub::{MessageKind, PubSubConnection, PubSubMessage};
pub use reconnect::ResilientConnection;
pub use reconnect_layer::ReconnectService;
pub use resilient::ResilientRedisClient;
pub use script::Script;
pub use tracing_layer::{TracingLayer, TracingService};
pub use transaction::{Transaction, TransactionResult};

#[cfg(feature = "serde")]
pub use json_api::Json;
#[cfg(feature = "serde")]
pub use search_api::{Search, SearchDoc, SearchResults, SortDir};

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
