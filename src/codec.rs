//! RESP protocol codec for Tokio

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

/// RESP protocol codec
pub struct RespCodec {
    // TODO: Integrate resp-parser-rs
}

impl RespCodec {
    /// Create a new RESP codec
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for RespCodec {
    fn default() -> Self {
        Self::new()
    }
}

/// RESP frame type (placeholder)
#[derive(Debug, Clone)]
pub enum Frame {
    /// Simple string
    SimpleString(String),
    /// Error
    Error(String),
    /// Integer
    Integer(i64),
    /// Bulk string
    BulkString(Vec<u8>),
    /// Array
    Array(Vec<Frame>),
    /// Null
    Null,
}

impl Decoder for RespCodec {
    type Item = Frame;
    type Error = std::io::Error;

    fn decode(&mut self, _src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // TODO: Implement RESP decoding using resp-parser-rs
        todo!("Implement RESP decoding")
    }
}

impl Encoder<Frame> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, _item: Frame, _dst: &mut BytesMut) -> Result<(), Self::Error> {
        // TODO: Implement RESP encoding
        todo!("Implement RESP encoding")
    }
}
