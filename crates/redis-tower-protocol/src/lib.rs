//! RESP protocol types and codec for redis-tower, backed by [`resp_rs`].
//!
//! This crate re-exports `resp_rs::resp3::Frame` and provides a Tokio codec
//! adapter for use with `tokio_util::codec::Framed`.

mod codec;
mod error;
pub mod helpers;

pub use codec::RespCodec;
pub use error::ProtocolError;

// Re-export the frame type and serializer directly from resp-rs.
pub use resp_rs::ParseError;
pub use resp_rs::resp3::{Frame, frame_to_bytes};
