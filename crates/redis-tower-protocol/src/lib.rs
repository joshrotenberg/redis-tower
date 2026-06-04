//! RESP protocol types and codec for redis-tower, backed by [`resp_rs`].
//!
//! This crate re-exports `resp_rs::resp3::Frame` and provides a Tokio codec
//! adapter for use with `tokio_util::codec::Framed`.
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
//! # Helpers
//!
//! The [`helpers`] module provides convenience constructors for building
//! command frames: [`helpers::bulk`] for bulk strings, [`helpers::array`] for
//! arrays, and [`helpers::null_bulk`] for null bulk strings.

#![deny(missing_docs)]

mod codec;
mod error;
pub mod helpers;

pub use codec::RespCodec;
pub use error::ProtocolError;

// Re-export the frame type and serializer directly from resp-rs.
pub use resp_rs::ParseError;
pub use resp_rs::resp3::{Frame, frame_to_bytes};
