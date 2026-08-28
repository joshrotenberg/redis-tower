//! RESP protocol types and codec for redis-tower, backed by [`resp_rs`].
//!
//! This crate re-exports `resp_rs::resp3::Frame` and provides a Tokio codec
//! adapter for use with `tokio_util::codec::Framed`.
//!
//! # Quick start
//!
//! ```
//! use redis_tower_protocol::{Frame, frame_to_bytes};
//! use redis_tower_protocol::helpers::{array, bulk};
//!
//! let command = array(vec![bulk("SET"), bulk("key"), bulk("value")]);
//! let encoded = frame_to_bytes(&command);
//! assert!(!encoded.is_empty());
//! assert!(matches!(command, Frame::Array(Some(_))));
//! ```
//!
//! # Frame Types
//!
//! The [`Frame`] enum (re-exported from `resp_rs`) covers all RESP3 wire types
//! including simple strings, errors, integers, bulk strings, arrays, maps,
//! sets, doubles, booleans, and null values.
//!
//! # Codec
//!
//! [`RespCodec`] implements both `tokio_util::codec::Encoder<Frame>` and
//! `tokio_util::codec::Decoder<Item = Frame>`, so it can be used directly with
//! `tokio_util::codec::Framed` for non-blocking read/write on any `AsyncRead +
//! AsyncWrite` transport.
//!
//! # Decode Limits
//!
//! Decoding is bounded by [`RespLimits`]: a maximum single-frame size and a
//! maximum nesting depth, both applied before a frame is materialized. They
//! exist so a malicious or compromised server cannot drive unbounded
//! allocation or overflow the stack with a deeply nested reply. The defaults
//! ([`DEFAULT_MAX_FRAME_SIZE`], [`DEFAULT_MAX_DEPTH`]) sit above anything a
//! Redis server sends in normal operation. Normal redis-tower clients tighten
//! them through `redis_tower_core::ConnectionConfig`; callers constructing the
//! codec directly use [`RespCodec::with_limits`].
//!
//! # Helpers
//!
//! The [`helpers`] module provides convenience constructors for building
//! command frames: [`helpers::bulk`] for bulk strings, [`helpers::array`] for
//! arrays, and [`helpers::null_bulk`] for null bulk strings. It also offers
//! [`helpers::display`] for `redis-cli`-style rendering of a frame and, behind
//! the `serde` feature, [`helpers::frame_to_json`] for converting a frame into
//! a `serde_json::Value`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod codec;
mod error;
pub mod helpers;

pub use codec::{DEFAULT_MAX_DEPTH, DEFAULT_MAX_FRAME_SIZE, RespCodec, RespLimits};
pub use error::ProtocolError;

// Re-export the frame type and serializer directly from resp-rs.
pub use resp_rs::ParseError;
pub use resp_rs::resp3::{Frame, frame_to_bytes};
