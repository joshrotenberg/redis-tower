//! Core connection and Tower `Service` implementation for redis-tower.
//!
//! This crate provides:
//! - [`Command`] trait for defining typed Redis commands
//! - [`RedisConnection`] implementing `Service<Cmd>` for any `Cmd: Command`
//! - [`RedisStream`] abstracting TCP, Unix, and TLS transports
//! - Configuration and URL parsing
//!
//! Most users should depend on the `redis-tower` facade crate instead of using
//! this directly.

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
