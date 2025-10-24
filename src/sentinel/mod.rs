//! Redis Sentinel support for high availability
//!
//! This module provides automatic master discovery and failover using Redis Sentinel.
//! It integrates with Tower's middleware ecosystem to provide composable resilience patterns.
//!
//! # Features
//!
//! - **Automatic Master Discovery**: Query Sentinel nodes to find the current master
//! - **Automatic Failover**: Tower's `Reconnect` middleware handles master changes transparently
//! - **Read-from-Replica**: Optional load-balanced reads across replica nodes
//! - **Type Safety**: Strongly-typed commands and responses
//! - **Composable Middleware**: Stack timeout, circuit breaker, retry on top
//!
//! # Example
//!
//! ```no_run
//! use redis_tower::sentinel::{SentinelConfig, SentinelClient};
//! use redis_tower::commands::{Set, Get};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Configure Sentinel
//! let config = SentinelConfig::builder()
//!     .sentinel_node("sentinel1", 26379)
//!     .sentinel_node("sentinel2", 26379)
//!     .sentinel_node("sentinel3", 26379)
//!     .master_name("mymaster")
//!     .read_from_replicas(true)
//!     .build()?;
//!
//! // Create client
//! let client = SentinelClient::new(config);
//!
//! // Get master connection with automatic failover
//! let mut master = client.master();
//!
//! // Execute commands
//! master.call(Set::new("key", "value")).await?;
//! let value: Option<bytes::Bytes> = master.call(Get::new("key")).await?;
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod commands;
pub mod config;
pub mod discovery;
pub mod make_service;

pub use client::SentinelClient;
pub use commands::{ReplicaInfo, RoleInfo, SentinelInfo};
pub use config::{SentinelConfig, SentinelConfigBuilder, SentinelConfigError};
