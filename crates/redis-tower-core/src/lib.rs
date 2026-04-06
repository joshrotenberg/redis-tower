//! Core connection and Tower `Service` implementation for redis-tower.
//!
//! This crate provides the foundational building blocks for the redis-tower
//! client. Most users should depend on the `redis-tower` facade crate instead
//! of using this directly.
//!
//! # Command Trait
//!
//! The [`Command`] trait defines the interface every typed Redis command must
//! implement. Each command specifies how to serialize itself into a RESP
//! [`Frame`] via `to_frame()` and how to parse the server response into its
//! associated `Response` type via `parse_response()`. A `name()` method
//! provides the command name for observability and tracing.
//!
//! # Connection
//!
//! [`RedisConnection`] implements `tower::Service<Cmd>` for any `Cmd: Command`,
//! making it composable with Tower middleware such as timeouts, rate limiting,
//! and buffering. It requires `&mut self` for `call()`, following the proper
//! Tower contract for connection-oriented services.
//!
//! # Transport
//!
//! [`RedisStream`] abstracts the underlying transport, supporting TCP and Unix
//! sockets out of the box. When the `tls-native-tls` or `tls-rustls` feature
//! is enabled, TLS-wrapped connections are also available via the `tls`
//! module.
//!
//! # URL Parsing
//!
//! [`parse_redis_url`] and [`RedisUrl`] parse `redis://`, `rediss://` (TLS),
//! and `redis+unix://` connection strings into structured configuration,
//! including host, port, database selection, and authentication credentials.
//!
//! # Value Conversion
//!
//! The [`value`] module provides the [`RedisConvert`], [`RedisValueExt`], and
//! [`FromRedisBytes`] traits for ergonomic conversion between RESP frames and
//! Rust types.

mod command;
mod connection;
mod error;
mod frame_service;
mod stream;
#[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
pub mod tls;
mod url;
pub mod value;

pub use command::Command;
pub use connection::RedisConnection;
pub use error::RedisError;
pub use frame_service::FrameService;
pub use stream::RedisStream;
pub use url::{RedisUrl, parse_redis_url};

pub use value::{FromRedisBytes, RedisConvert, RedisValueExt};

pub use redis_tower_protocol::{Frame, RespCodec};
