//! High-level, ergonomic client interfaces for Redis Stack modules.
//!
//! This crate is the dedicated home for typed, ergonomic clients over the
//! Redis Stack modules (RedisJSON, RediSearch, RedisTimeSeries, the
//! probabilistic data structures, and Vector Sets). Callers never see raw
//! [`Frame`](redis_tower::Frame) values or command structs; they work with
//! typed Rust values, with serialization and response parsing handled inside
//! each client.
//!
//! The lower-level command builders live in `redis-tower-commands`, and the
//! `redis-tower` crate exposes the connections and middleware these clients
//! wrap. This crate sits on top, trading a little flexibility for a much
//! friendlier surface.
//!
//! # Feature flags
//!
//! Each module client is gated behind a feature flag. The `full` feature
//! (enabled by default) turns them all on.
//!
//! | Feature         | Enables                                                        |
//! |-----------------|----------------------------------------------------------------|
//! | `json`          | [`json::JsonClient`] — RedisJSON                               |
//! | `search`        | [`search::SearchClient`] — RediSearch                          |
//! | `timeseries`    | [`timeseries::TimeSeriesClient`] — RedisTimeSeries             |
//! | `probabilistic` | Bloom, Cuckoo, Count-Min Sketch, TopK, T-Digest clients        |
//! | `vector`        | [`vector::VectorSetClient`] — Vector Sets                      |
//! | `full`          | All of the above (default)                                     |
//!
//! # Quick start
//!
//! ```ignore
//! use redis_tower::RedisClient;
//! use redis_tower_modules::json::JsonClient;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct User {
//!     name: String,
//!     age: u32,
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("redis://127.0.0.1:6379").await?;
//! let mut json = JsonClient::new(client);
//!
//! let user = User { name: "Ada".into(), age: 36 };
//! json.set("user:1", "$", &user).await?;
//!
//! let fetched: User = json.get("user:1", "$").await?;
//! assert_eq!(fetched.name, "Ada");
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "json")]
#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
pub mod json;

#[cfg(feature = "search")]
#[cfg_attr(docsrs, doc(cfg(feature = "search")))]
pub mod search;

#[cfg(feature = "timeseries")]
#[cfg_attr(docsrs, doc(cfg(feature = "timeseries")))]
pub mod timeseries;

#[cfg(feature = "probabilistic")]
#[cfg_attr(docsrs, doc(cfg(feature = "probabilistic")))]
pub mod probabilistic;

#[cfg(feature = "vector")]
#[cfg_attr(docsrs, doc(cfg(feature = "vector")))]
pub mod vector;
