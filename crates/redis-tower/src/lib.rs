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
//! use redis_tower::{MultiplexedClient, commands::*};
//!
//! // MultiplexedClient is the recommended default: one auto-pipelined
//! // connection, cheap to clone and share across tasks.
//! let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
//! client.execute(Set::new("key", "value")).await?;
//! let val: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
//! ```
//!
//! # Choosing a client
//!
//! | Client | When to use |
//! |--------|-------------|
//! | [`MultiplexedClient`] | **The default.** One connection, concurrent commands auto-pipelined; cheap to `clone` and share across tasks. |
//! | [`RedisConnection`] | A single exclusive connection (`&mut self`), or as a building block for the others. |
//! | [`RedisClient`] | `Arc<Mutex<RedisConnection>>` -- a simple shared handle, but it serializes commands through one lock (lower throughput than `MultiplexedClient`). |
//! | [`ResilientRedisClient`] | A shared handle with automatic reconnection and exponential backoff, for long-running services. |
//! | [`ConnectionPool`] | N connections with configurable [`DispatchStrategy`] -- use for blocking commands (`BLPOP`) or CPU-bound reply parsing, where one multiplexed connection would head-of-line block. |
//! | `MultiplexedClusterClient` (`redis-tower-cluster`) | Redis Cluster, high concurrency. |
//! | `MultiplexedSentinelClient` (`redis-tower-sentinel`) | Sentinel-managed failover, high concurrency. |
//! | `SyncClient` (`redis-tower-sync`) | Blocking (non-`async`) contexts. |
//!
//! Reach for `RedisClient` only when you specifically want serialized,
//! exclusive access; for most workloads `MultiplexedClient` is both simpler and
//! faster. A naive benchmark of `RedisClient` will under-report throughput
//! because every command waits on the mutex.
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
//! for circuit breaking, rate limiting, and bulkhead isolation. Automatic
//! reconnection with exponential backoff is built in via
//! [`ResilientRedisClient`]:
//!
//! ```ignore
//! use tower::ServiceBuilder;
//! use tower_resilience_circuitbreaker::circuit_breaker_builder;
//! use redis_tower::{FrameService, CommandAdapter, TracingLayer};
//!
//! let cb = circuit_breaker_builder()
//!     .failure_rate_threshold(50.0)
//!     .wait_duration_in_open(Duration::from_secs(30))
//!     .build();
//!
//! let svc = CommandAdapter::new(
//!     ServiceBuilder::new()
//!         .layer(cb)
//!         .layer(TracingLayer::new())
//!         .service(FrameService::connect("127.0.0.1:6379").await?)
//! );
//! ```
//!
//! [`RedisError::is_retryable`] classifies which errors are worth retrying
//! (transient connection errors) versus command errors like WRONGTYPE that are
//! not. Pair it with [`ResilientRedisClient`] for idempotent-aware
//! reconnection.
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

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod auto_pipeline;
pub mod cache_layer;
mod cache_state;
pub mod caching;
pub mod circuit_breaker;
mod client;
pub mod command_adapter;
pub mod command_timeout;
pub mod consumer;
pub mod credentials;
mod executor;
pub mod metrics_layer;
pub mod multiplexed;
pub mod pipeline;
pub mod pool;
pub mod pubsub;
pub mod reconnect;
pub mod reconnect_layer;
mod resilient;
pub mod scan_stream;
pub mod script;
pub mod tracing_layer;
pub mod transaction;

#[cfg(feature = "serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde")))]
pub mod json_api;
#[cfg(feature = "serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde")))]
pub mod search_api;

pub use auto_pipeline::{AutoPipelineConfig, AutoPipelineService};
pub use cache_layer::CacheService;
pub use cache_state::CacheState;
pub use caching::CachedClient;
pub use circuit_breaker::{CircuitBreakerConfig, CircuitBreakerLayer, CircuitBreakerService};
pub use client::RedisClient;
pub use command_adapter::CommandAdapter;
pub use command_timeout::CommandTimeoutLayer;
pub use consumer::{ConsumerConfig, StreamConsumer, StreamMessage};
pub use credentials::{
    AuthenticatedConnection, CredentialProvider, Credentials, RotatingAuthClient, StaticCredentials,
};
pub use executor::{ExecutorService, RedisExecutor};
pub use metrics_layer::{MetricsLayer, MetricsRecorder, MetricsService};
pub use multiplexed::MultiplexedClient;
pub use pipeline::{Pipeline, PipelineExecutor, PipelineResults};
pub use pool::{ConnectionPool, DispatchStrategy, PoolConfig};
pub use pubsub::{MessageKind, PubSubConnection, PubSubMessage};
pub use reconnect::ResilientConnection;
pub use reconnect_layer::ReconnectService;
pub use resilient::ResilientRedisClient;
pub use scan_stream::ScanStream;
pub use script::Script;
pub use tracing_layer::{TracingLayer, TracingService};
pub use transaction::{Transaction, TransactionExecutor, TransactionResult};

#[cfg(feature = "serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde")))]
#[allow(deprecated)]
pub use json_api::Json;
#[cfg(feature = "serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde")))]
#[allow(deprecated)]
pub use search_api::{Search, SearchDoc, SearchResults, SortDir};

// Re-export core types.
pub use redis_tower_core::{
    Command, Frame, FrameService, FromRedisBytes, ProtocolVersion, RedisConnection, RedisConvert,
    RedisError, RedisStream, RedisValueExt, RespCodec,
};

// Re-export TLS config when a TLS backend is enabled.
#[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
pub use redis_tower_core::tls::TlsConfig;

/// All typed Redis command structs, re-exported from `redis-tower-commands`.
///
/// Import everything with `use redis_tower::commands::*` for convenient access
/// to the full command set including both core Redis and Redis Stack commands.
pub mod commands {
    pub use redis_tower_commands::*;
}
