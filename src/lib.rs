//! A Tower-based Redis client with strong typing and composable middleware.
//!
//! `redis-tower` provides a type-safe, composable Redis client built on the
//! [Tower](https://github.com/tower-rs/tower) ecosystem. It combines strong typing,
//! zero-cost abstractions, and middleware composition for building resilient Redis applications.
//!
//! # Features
//!
//! - **Type Safety**: 200+ strongly-typed commands with compile-time validation
//! - **Tower Integration**: Native `Service` trait for composable middleware
//! - **Zero-Cost Abstractions**: Optional features for cluster, sentinel, and modules
//! - **Resilience**: Ready for circuit breakers, retries, timeouts via Tower
//! - **Performance**: High-performance RESP parser with zero-copy parsing
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use redis_tower::commands::{Get, Set, Incr};
//! use tower::ServiceExt;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect to Redis
//!     let mut client = redis_tower::RedisClient::connect("localhost:6379").await?;
//!
//!     // Strongly typed commands
//!     client.call(Set::new("counter", "0")).await?;
//!
//!     let count: i64 = client.call(Incr::new("counter")).await?;
//!     println!("Counter: {}", count);
//!
//!     let value: Option<String> = client.call(Get::new("counter")).await?;
//!     println!("Value: {:?}", value);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Command Categories
//!
//! The client supports 200+ Redis commands organized by category:
//!
//! - **Strings**: [`Get`](commands::Get), [`Set`](commands::Set), [`Incr`](commands::Incr), [`Lcs`](commands::Lcs)
//! - **Hashes**: [`HGet`](commands::HGet), [`HSet`](commands::HSet), [`HIncrBy`](commands::HIncrBy)
//! - **Lists**: [`LPush`](commands::LPush), [`RPop`](commands::RPop), [`LRange`](commands::LRange)
//! - **Sets**: [`SAdd`](commands::Sadd), [`SInter`](commands::Sinter), [`SUnion`](commands::Sunion)
//! - **Sorted Sets**: [`Zadd`](commands::Zadd), [`ZRange`](commands::Zrange), [`ZMPop`](commands::ZMPop)
//! - **Streams**: [`XAdd`](commands::XAdd), [`XReadGroup`](commands::XReadGroup), [`XAck`](commands::XAck)
//! - **Geo**: [`GeoAdd`](commands::GeoAdd), [`GeoSearch`](commands::GeoSearch), [`GeoSearchStore`](commands::GeoSearchStore)
//!
//! See the [`commands`] module for the complete list.
//!
//! # Builder Patterns
//!
//! Complex commands use builder patterns for optional parameters:
//!
//! ```rust,no_run
//! use redis_tower::commands::{Set, ZRangeByScore};
//! # async fn example(client: &mut redis_tower::RedisClient) -> Result<(), Box<dyn std::error::Error>> {
//!
//! // Set with expiration and options
//! let cmd = Set::new("key", "value")
//!     .ex(3600)  // Expire in 1 hour
//!     .nx()      // Only set if not exists
//!     .get();    // Return old value
//!
//! // Sorted set range with scores
//! let members = client.call(
//!     ZRangeByScore::new("leaderboard", 0.0, 100.0)
//!         .withscores()
//!         .limit(0, 10)
//! ).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Tower Middleware
//!
//! Compose resilience layers around your Redis client:
//!
//! ```rust,no_run
//! use tower::ServiceBuilder;
//! use tower::timeout::TimeoutLayer;
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = ServiceBuilder::new()
//!     .layer(TimeoutLayer::new(Duration::from_secs(5)))
//!     // Add circuit breaker, retry, rate limiting, etc.
//!     .service(redis_tower::RedisClient::connect("localhost:6379").await?);
//! # Ok(())
//! # }
//! ```
//!
//! # Optional Features
//!
//! Enable features in your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! redis-tower = { version = "0.1", features = ["cluster", "bloom"] }
//! ```
//!
//! Available features:
//!
//! - `cluster` - Redis Cluster support with slot routing
//! - `sentinel` - Redis Sentinel support for high availability
//! - `deprecated` - Deprecated commands with migration guides
//! - `bloom` - Bloom filter commands (Redis Stack)
//! - `json` - RedisJSON commands (planned)
//! - `search` - RediSearch commands (planned)
//!
//! # Cluster Support
//!
//! ```rust,no_run
//! # #[cfg(feature = "cluster")]
//! use redis_tower::cluster::ClusterClient;
//!
//! # #[cfg(feature = "cluster")]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = ClusterClient::new(vec![
//!     "redis://127.0.0.1:7000",
//!     "redis://127.0.0.1:7001",
//!     "redis://127.0.0.1:7002",
//! ]).await?;
//!
//! // Automatic slot-based routing
//! use redis_tower::commands::{Set, Get};
//! client.call(Set::new("key", "value")).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Type Safety
//!
//! Commands know their response types at compile time:
//!
//! ```rust,no_run
//! use redis_tower::commands::{Get, Incr, ZRangeByScore};
//! use bytes::Bytes;
//!
//! # async fn example(client: &mut redis_tower::RedisClient) -> Result<(), Box<dyn std::error::Error>> {
//! // Compiler enforces correct types
//! let value: Option<Bytes> = client.call(Get::new("key")).await?;
//! let count: i64 = client.call(Incr::new("counter")).await?;
//! let members: Vec<(String, f64)> = client.call(
//!     ZRangeByScore::new("zset", 0.0, 100.0).withscores()
//! ).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Error Handling
//!
//! All commands return `Result<T, RedisError>`:
//!
//! ```rust,no_run
//! use redis_tower::{RedisError, commands::Get};
//!
//! # async fn example(client: &mut redis_tower::RedisClient) {
//! match client.call(Get::new("key")).await {
//!     Ok(Some(value)) => println!("Got: {:?}", value),
//!     Ok(None) => println!("Key not found"),
//!     Err(RedisError::Connection(e)) => eprintln!("Connection error: {}", e),
//!     Err(RedisError::Redis(e)) => eprintln!("Redis error: {}", e),
//!     Err(e) => eprintln!("Other error: {}", e),
//! }
//! # }
//! ```

// TODO: Re-enable after fixing all missing docs
// #![warn(missing_docs)]
#![warn(clippy::all)]
#![deny(unsafe_code)]
#![allow(dead_code)]
#![allow(unused_variables)]

pub mod client;
pub mod codec;
pub mod commands;
pub mod config;
pub mod connection_pool;
pub mod health;
pub mod hooks;
pub mod metrics;
pub mod monitor;
pub mod parser;
pub mod pipeline;
pub mod pool;
pub mod pubsub;
pub mod read_preference;
pub mod tls;
pub mod tracing;
pub mod transaction;
pub mod types;
pub mod url;

// Deployment topology support (feature-gated)
#[cfg(feature = "cluster")]
pub mod cluster;

#[cfg(feature = "sentinel")]
pub mod sentinel;

// Redis Stack modules (feature-gated)
#[cfg(feature = "modules")]
pub mod modules;

// Re-exports for convenience
pub use client::{RedisClient, ResilientRedisClient};
pub use commands::Command;
pub use pipeline::{Pipeline, PipelineResults};
pub use pubsub::{PubSubConnection, PubSubMessage};
pub use transaction::{Discard, Exec, Multi, Transaction, Unwatch, Watch};
pub use types::{RedisError, RedisValue};
