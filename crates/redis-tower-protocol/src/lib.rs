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
//! [`RespLimits`] bounds each frame's wire size and aggregate nesting before
//! the recursive parser materializes it. Complete and fragmented input obey
//! the same limits; incomplete aggregate headers do not reserve storage for
//! their declared children. Wire size is not an exact heap budget: decoded
//! aggregate elements have representation overhead. A receive buffer may hold
//! many individually bounded replies. Clients configure these limits through
//! `redis_tower_core::ConnectionConfig`; direct codec users call
//! [`RespCodec::with_limits`].
//!
//! Attributes and streamed RESP3 encodings are rejected rather than emitted
//! as independent replies. Representing these variants in [`Frame`] does not
//! mean the codec supports their metadata or sequence semantics. Errors and
//! incomplete input leave the receive buffer unchanged.
//! The repository's [protocol validation guide](https://github.com/joshrotenberg/redis-tower/blob/main/docs/PROTOCOL-TESTING.md)
//! is the disposition table for supported, rejected, generated, and fuzzed
//! wire forms.
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
mod preflight;

pub use codec::{DEFAULT_MAX_DEPTH, DEFAULT_MAX_FRAME_SIZE, RespCodec, RespLimits};
pub use error::ProtocolError;

// Re-export the frame type and serializer directly from resp-rs.
pub use resp_rs::ParseError;
pub use resp_rs::resp3::{Frame, frame_to_bytes};
