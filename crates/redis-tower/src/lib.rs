//! A Tower-native Redis client for Rust with typed commands, composable
//! middleware, and complete Redis Stack coverage.
//!
//! Every connection is a `tower::Service`. Commands are typed request/response
//! pairs with compile-time safety. Resilience, caching, and observability are
//! composed via standard Tower layers.
//!
//! # Quick Start
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::{MultiplexedClient, commands::*};
//!
//! // MultiplexedClient is the recommended default: one auto-pipelined
//! // connection, cheap to clone and share across tasks.
//! let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
//! client.execute(Set::new("key", "value")).await?;
//! let val: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
//! # let _ = val;
//! # Ok(())
//! # }
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
//! ```no_run
//! # fn example() {
//! use redis_tower::commands::*;
//!
//! let cmd = Set::new("key", "value").ex(60).nx();
//! let cmd = ZAdd::new("leaderboard").member(100.0, "alice").member(200.0, "bob");
//! # let _ = cmd;
//! # }
//! ```
//!
//! Use [`RedisValueExt::parse_into`] for ergonomic type conversion from
//! command responses:
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::{RedisClient, RedisValueExt, commands::*};
//!
//! let client = RedisClient::connect("127.0.0.1:6379").await?;
//! client.execute(Set::new("counter", "42")).await?;
//! let raw = client.execute(Get::new("counter")).await?;
//! let count: i64 = raw.parse_into()?;
//! # let _ = count;
//! # Ok(())
//! # }
//! ```
//!
//! # Middleware
//!
//! Middleware composes at the *frame* altitude. [`FrameService`] is the
//! `tower::Service<Frame>` at the bottom of the stack, so any Tower layer that
//! speaks `Frame` stacks on top of it. redis-tower also ships built-in layers:
//!
//! - [`TracingLayer`] -- emits tracing spans for each command.
//! - [`MetricsLayer`] -- records command latency and counts via a pluggable
//!   [`MetricsRecorder`].
//! - [`CacheService`] -- client-side frame caching with invalidation.
//! - [`ReconnectService`] -- automatic reconnection with configurable backoff.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use tower::ServiceBuilder;
//! use tower::timeout::TimeoutLayer;
//! use tower::buffer::BufferLayer;
//! use redis_tower::{Frame, FrameService, TracingLayer};
//!
//! let conn = FrameService::connect("127.0.0.1:6379").await?;
//! let svc = ServiceBuilder::new()
//!     .layer(BufferLayer::<Frame>::new(64))
//!     .layer(TimeoutLayer::new(std::time::Duration::from_secs(5)))
//!     .layer(TracingLayer::new())
//!     .service(conn);
//! # let _ = svc;
//! # Ok(())
//! # }
//! ```
//!
//! Layers like `BufferLayer` and `TimeoutLayer` box their error type, so a
//! stack that includes them no longer meets [`CommandAdapter`]'s
//! `Error = RedisError` bound. Keep such layers outside the adapter, or stick
//! to layers that preserve [`RedisError`] when you want typed commands on top
//! (see the next section).
//!
//! # Resilience
//!
//! Circuit breaking ships in this crate as [`CircuitBreakerLayer`], and
//! automatic reconnection with exponential backoff is built in via
//! [`ResilientRedisClient`]. Both are ordinary Tower layers, so they stack
//! under a `ServiceBuilder` at the frame altitude, with [`CommandAdapter`]
//! wrapping the result to accept typed commands:
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//!
//! use tower::ServiceBuilder;
//! use redis_tower::{
//!     CircuitBreakerConfig, CircuitBreakerLayer, CommandAdapter, FrameService, TracingLayer,
//! };
//!
//! let cb = CircuitBreakerLayer::new(CircuitBreakerConfig {
//!     failure_threshold: 5,
//!     recovery_probe_interval: Duration::from_secs(30),
//! });
//!
//! let svc = CommandAdapter::new(
//!     ServiceBuilder::new()
//!         .layer(cb)
//!         .layer(TracingLayer::new())
//!         .service(FrameService::connect("127.0.0.1:6379").await?),
//! );
//! # let _ = svc;
//! # Ok(())
//! # }
//! ```
//!
//! For fault-tolerance primitives this crate does not ship -- rate limiting and
//! bulkhead isolation -- the
//! [tower-resilience](https://crates.io/crates/tower-resilience) crate family
//! provides layers that compose into the same builder.
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
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::{AutoPipelineService, AutoPipelineConfig, CommandAdapter, RedisConnection};
//! use redis_tower::commands::*;
//! use tower::Service;
//!
//! let conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let mut svc = CommandAdapter::new(
//!     AutoPipelineService::new(conn, AutoPipelineConfig::default()),
//! );
//! let val: Option<bytes::Bytes> = svc.call(Get::new("key")).await?;
//! # let _ = val;
//! # Ok(())
//! # }
//! ```
//!
//! # Pipeline and Transactions
//!
//! [`Pipeline`] batches multiple commands into a single roundtrip.
//! [`Transaction`] wraps commands in MULTI/EXEC with optional WATCH support
//! for optimistic locking. The [`transaction()`] helper drives the standard
//! WATCH/read/EXEC retry loop for you, re-running a closure until EXEC commits
//! or a retry cap is hit.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::{transaction, Pipeline, Transaction, RedisConnection};
//! use redis_tower::commands::{Get, Incr, Set};
//!
//! let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
//!
//! let pipelined = Pipeline::new()
//!     .push(Set::new("a", "1"))
//!     .push(Get::new("a"))
//!     .execute(&mut conn)
//!     .await?;
//!
//! let watched = Transaction::new()
//!     .watch(["key"])
//!     .push(Incr::new("key"))
//!     .execute(&mut conn)
//!     .await?;
//!
//! // Optimistic-locking retry loop: read inside the WATCH window, then EXEC,
//! // retrying automatically if another client touches `counter`.
//! let incremented = transaction(&mut conn, ["counter"], async |c| {
//!     let current: i64 = match c.execute(Get::new("counter")).await? {
//!         Some(bytes) => String::from_utf8_lossy(&bytes).parse().unwrap_or(0),
//!         None => 0,
//!     };
//!     Ok(Transaction::new().push(Set::new("counter", (current + 1).to_string())))
//! })
//! .await?;
//! # let _ = (pipelined, watched, incremented);
//! # Ok(())
//! # }
//! ```
//!
//! # Pub/Sub
//!
//! [`PubSubConnection`] provides a dedicated subscribe/publish interface.
//! Messages are delivered as a [`futures::Stream`] of [`PubSubMessage`]
//! values with channel and payload fields.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # use futures::StreamExt;
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
//! # Ok(())
//! # }
//! ```
//!
//! # Streams
//!
//! [`StreamConsumer`] wraps XREADGROUP into a Rust [`futures::Stream`] with
//! automatic acknowledgement and consumer group management. Configure batch
//! size, block timeout, and auto-ack behavior via [`ConsumerConfig`].
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::consumer::{StreamConsumer, ConsumerConfig};
//! use redis_tower::RedisConnection;
//! use tokio_stream::StreamExt;
//!
//! let conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let consumer = StreamConsumer::new("my-group", "worker-1", ["my-stream"])
//!     .config(ConsumerConfig { batch_size: 20, auto_ack: true, ..Default::default() });
//!
//! let mut stream = std::pin::pin!(consumer.into_stream(conn));
//! while let Some(msg) = stream.next().await {
//!     let msg = msg?;
//!     println!("{}: {} fields", msg.id, msg.fields.len());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Scripting
//!
//! [`Script`] pre-computes the SHA1 digest at construction time and provides
//! an [`execute`](Script::execute) method that tries EVALSHA first, falling
//! back to EVAL on NOSCRIPT. This avoids sending the full script text on
//! every call.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let mut conn = redis_tower::RedisConnection::connect("127.0.0.1:6379").await?;
//! use redis_tower::Script;
//!
//! let script = Script::new("return redis.call('GET', KEYS[1])");
//! let result = script.execute(&mut conn, &["mykey"], &[]).await?;
//! # let _ = result;
//! # Ok(())
//! # }
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
//! ```no_run
//! # #[allow(deprecated)]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let mut conn = redis_tower::RedisConnection::connect("127.0.0.1:6379").await?;
//! use redis_tower::Json;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct User { name: String, age: u32 }
//!
//! let mut json = Json::new(&mut conn);
//! json.set("user:1", "$", &User { name: "Alice".into(), age: 30 }).await?;
//! let user: User = json.get("user:1", "$").await?;
//! # let _ = user;
//! # Ok(())
//! # }
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
pub mod retry;
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
pub use pubsub::{
    KeyspaceEvent, KeyspaceEventStream, MessageKind, NotificationKind, PubSubConnection,
    PubSubMessage,
};
pub use reconnect::ResilientConnection;
pub use reconnect_layer::ReconnectService;
pub use resilient::ResilientRedisClient;
pub use retry::{RetryClient, RetryLayer, RetryPolicy, RetryService};
pub use scan_stream::ScanStream;
pub use script::Script;
pub use tracing_layer::{TracingLayer, TracingService};
pub use transaction::{
    DEFAULT_TRANSACTION_RETRIES, Transaction, TransactionExecutor, TransactionResult, transaction,
    transaction_with_retries,
};

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
