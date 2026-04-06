//! A Tower-native Redis client for Rust with typed commands, composable
//! middleware, and complete Redis Stack coverage.
//!
//! Every connection is a `tower::Service`. Commands are typed request/response
//! pairs with compile-time safety. Resilience, caching, and observability are
//! composed via standard Tower layers.
//!
//! # Quick Start
//!
//! ```ignore
//! use redis_tower::{RedisClient, commands::*};
//!
//! let client = RedisClient::connect("127.0.0.1:6379").await?;
//! client.execute(Set::new("key", "value")).await?;
//! let val: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
//! ```
//!
//! # Connection Types
//!
//! redis-tower provides several connection types for different use cases:
//!
//! - [`RedisConnection`] -- the foundational type. Implements
//!   `tower::Service<Cmd>` with `&mut self`, giving you direct exclusive
//!   access. Use with `tower::buffer::Buffer` for sharing.
//! - [`RedisClient`] -- wraps a connection in `Arc<Mutex<>>` for easy
//!   cross-task sharing. Good for scripts and simple applications.
//! - [`ResilientRedisClient`] -- shared client with automatic reconnection
//!   and exponential backoff. Best for long-running services.
//! - [`ConnectionPool`] -- manages N connections with configurable dispatch
//!   via [`DispatchStrategy`] (round-robin, random, or least-connections).
//!
//! # Commands
//!
//! The [`commands`] module contains 360+ typed command structs spanning core
//! Redis and feature-gated Redis Stack modules. Each command implements the
//! [`Command`] trait, which defines serialization to a RESP [`Frame`] via
//! `to_frame()` and response parsing via `parse_response()`.
//!
//! Commands with optional parameters use builder methods:
//!
//! ```ignore
//! use redis_tower::commands::*;
//!
//! let cmd = Set::new("key", "value").ex(60).nx();
//! let cmd = ZAdd::new("leaderboard").member(100.0, "alice").member(200.0, "bob");
//! ```
//!
//! Use [`RedisValueExt::parse_into`] for ergonomic type conversion from
//! command responses:
//!
//! ```ignore
//! use redis_tower::{RedisClient, RedisValueExt, commands::*};
//!
//! let client = RedisClient::connect("127.0.0.1:6379").await?;
//! client.execute(Set::new("counter", "42")).await?;
//! let raw = client.execute(Get::new("counter")).await?;
//! let count: i64 = raw.parse_into()?;
//! ```
//!
//! # Middleware
//!
//! Since [`RedisConnection`] implements `tower::Service`, you can compose it
//! with any Tower middleware. redis-tower also ships built-in layers:
//!
//! - [`TracingLayer`] -- emits tracing spans for each command.
//! - [`MetricsLayer`] -- records command latency and counts via a pluggable
//!   [`MetricsRecorder`].
//! - [`CacheService`] -- client-side frame caching with invalidation.
//! - [`ReconnectService`] -- automatic reconnection with configurable backoff.
//!
//! ```ignore
//! use tower::ServiceBuilder;
//! use tower::timeout::TimeoutLayer;
//! use tower::buffer::BufferLayer;
//! use redis_tower::{RedisConnection, TracingLayer};
//!
//! let conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let svc = ServiceBuilder::new()
//!     .layer(BufferLayer::new(64))
//!     .layer(TimeoutLayer::new(std::time::Duration::from_secs(5)))
//!     .layer(TracingLayer)
//!     .service(conn);
//! ```
//!
//! # Resilience with tower-resilience
//!
//! For production fault tolerance, compose with the
//! [tower-resilience](https://crates.io/crates/tower-resilience) crate family
//! for circuit breaking, retry with backoff, rate limiting, and bulkhead
//! isolation:
//!
//! ```ignore
//! use tower::ServiceBuilder;
//! use tower_resilience_circuitbreaker::circuit_breaker_builder;
//! use tower_resilience_retry::RetryLayer;
//! use redis_tower::{FrameService, CommandAdapter, TracingLayer};
//!
//! let cb = circuit_breaker_builder()
//!     .failure_rate_threshold(50.0)
//!     .wait_duration_in_open(Duration::from_secs(30))
//!     .build();
//!
//! let retry = RetryLayer::<Frame, Frame, RedisError>::builder()
//!     .max_attempts(3)
//!     .exponential_backoff(Duration::from_millis(100))
//!     .retry_on(|err: &RedisError| err.is_retryable())
//!     .build();
//!
//! let svc = CommandAdapter::new(
//!     ServiceBuilder::new()
//!         .layer(retry)
//!         .layer(cb)
//!         .layer(TracingLayer::new())
//!         .service(FrameService::connect("127.0.0.1:6379").await?)
//! );
//! ```
//!
//! [`RedisError::is_retryable`] distinguishes transient connection errors
//! (worth retrying) from command errors like WRONGTYPE (not worth retrying).
//!
//! # Auto-Pipelining
//!
//! [`AutoPipelineService`] collects concurrent requests from multiple tasks
//! and sends them as a single Redis pipeline, returning individual responses
//! to each caller. Configure batch size and window via
//! [`AutoPipelineConfig`].
//!
//! ```ignore
//! use redis_tower::{AutoPipelineService, AutoPipelineConfig, CommandAdapter, RedisConnection};
//! use redis_tower::commands::*;
//! use tower::Service;
//!
//! let conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let mut svc = CommandAdapter::new(
//!     AutoPipelineService::new(conn, AutoPipelineConfig::default()),
//! );
//! let val: Option<bytes::Bytes> = svc.call(Get::new("key")).await?;
//! ```
//!
//! # Pipeline and Transactions
//!
//! [`Pipeline`] batches multiple commands into a single roundtrip.
//! [`Transaction`] wraps commands in MULTI/EXEC with optional WATCH support
//! for optimistic locking.
//!
//! ```ignore
//! use redis_tower::{Pipeline, Transaction, RedisConnection};
//! use redis_tower::commands::*;
//!
//! let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
//!
//! let results = Pipeline::new()
//!     .push(Set::new("a", "1"))
//!     .push(Get::new("a"))
//!     .execute(&mut conn)
//!     .await?;
//!
//! let result = Transaction::new()
//!     .watch(["key"])
//!     .push(Incr::new("key"))
//!     .execute(&mut conn)
//!     .await?;
//! ```
//!
//! # Pub/Sub
//!
//! [`PubSubConnection`] provides a dedicated subscribe/publish interface.
//! Messages are delivered as a [`futures::Stream`] of [`PubSubMessage`]
//! values with channel and payload fields.
//!
//! ```ignore
//! use redis_tower::{PubSubConnection, RedisConnection};
//!
//! let conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let mut pubsub = PubSubConnection::from_connection(conn)?;
//! pubsub.subscribe(&["events"]).await?;
//!
//! while let Some(msg) = pubsub.next().await {
//!     let msg = msg?;
//!     println!("{}: {:?}", msg.channel, msg.payload);
//! }
//! ```
//!
//! # Streams
//!
//! [`StreamConsumer`] wraps XREADGROUP into a Rust [`futures::Stream`] with
//! automatic acknowledgement and consumer group management. Configure batch
//! size, block timeout, and auto-ack behavior via [`ConsumerConfig`].
//!
//! ```ignore
//! use redis_tower::consumer::{StreamConsumer, ConsumerConfig};
//! use redis_tower::RedisConnection;
//! use tokio_stream::StreamExt;
//!
//! let conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let consumer = StreamConsumer::new("my-group", "worker-1", ["my-stream"])
//!     .config(ConsumerConfig { batch_size: 20, auto_ack: true, ..Default::default() });
//!
//! let mut stream = consumer.into_stream(conn);
//! while let Some(msg) = stream.next().await {
//!     let msg = msg?;
//!     println!("{}: {} fields", msg.id, msg.fields.len());
//! }
//! ```
//!
//! # Scripting
//!
//! [`Script`] pre-computes the SHA1 digest at construction time and provides
//! an [`execute`](Script::execute) method that tries EVALSHA first, falling
//! back to EVAL on NOSCRIPT. This avoids sending the full script text on
//! every call.
//!
//! ```ignore
//! use redis_tower::Script;
//!
//! let script = Script::new("return redis.call('GET', KEYS[1])");
//! let result = script.execute(&mut conn, &["mykey"], &[]).await?;
//! ```
//!
//! # JSON and Search APIs (serde feature)
//!
//! When the `serde` feature is enabled, two high-level APIs are available:
//!
//! - `Json` -- typed wrapper around RedisJSON commands with automatic
//!   serde serialization and deserialization.
//! - `Search` -- query builder for RediSearch with automatic result
//!   deserialization into user-defined types.
//!
//! ```ignore
//! use redis_tower::Json;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct User { name: String, age: u32 }
//!
//! let mut json = Json::new(&mut conn);
//! json.set("user:1", "$", &User { name: "Alice".into(), age: 30 }).await?;
//! let user: User = json.get("user:1", "$").await?;
//! ```
//!
//! # Feature Flags
//!
//! - `commands-stack` (default) -- all Redis Stack module commands
//! - `commands-json` -- RedisJSON commands
//! - `commands-search` -- RediSearch commands
//! - `commands-bloom` -- Bloom filter commands
//! - `commands-sketch` -- Count-Min Sketch commands
//! - `commands-tdigest` -- t-digest commands
//! - `commands-timeseries` -- TimeSeries commands
//! - `commands-vector-sets` -- Vector Set commands
//! - `serde` -- `Json` and `Search` high-level APIs
//! - `tls-native-tls` -- TLS via native-tls backend
//! - `tls-rustls` -- TLS via rustls backend
//!
//! # Crate Structure
//!
//! This is the facade crate. It re-exports types from the workspace:
//!
//! - `redis-tower-protocol` -- RESP3 frame types and codec
//! - `redis-tower-core` -- [`Command`] trait, [`RedisConnection`],
//!   [`FrameService`], transport, URL parsing, and value conversion
//! - `redis-tower-commands` -- 360+ typed command implementations
//!   (re-exported via [`commands`])
//! - `redis-tower-cluster` -- cluster routing, MOVED/ASK, read preference
//! - `redis-tower-sentinel` -- Sentinel discovery and failover
//! - `redis-tower-sync` -- blocking wrapper with internal tokio runtime

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
