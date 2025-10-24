//! RESP protocol codec for Tokio

use bytes::{Buf, BufMut, Bytes, BytesMut};
use resp_parser::resp3::{Frame as Resp3Frame, parse_frame};
use tokio_util::codec::{Decoder, Encoder};

/// RESP protocol codec
///
/// This codec wraps the resp-parser library to provide Tokio-compatible
/// encoding and decoding of RESP frames.
pub struct RespCodec {
    // Future: could add protocol version selection (RESP2 vs RESP3)
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

/// RESP frame type
///
/// Wraps resp-parser's Frame type with additional utility methods.
/// Supports both RESP2 and RESP3 protocol features.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// Simple string
    SimpleString(Bytes),
    /// Error
    Error(Bytes),
    /// Integer
    Integer(i64),
    /// Bulk string (None represents null)
    BulkString(Option<Bytes>),
    /// Array
    Array(Vec<Frame>),
    /// Null (for RESP2 compatibility)
    Null,

    // RESP3-specific types
    /// Map - key/value pairs (RESP3)
    Map(Vec<(Frame, Frame)>),
    /// Set - unique elements (RESP3)
    Set(Vec<Frame>),
    /// Double - floating point number (RESP3)
    Double(f64),
    /// Boolean (RESP3)
    Boolean(bool),
    /// Push - server-initiated message (RESP3 pub/sub)
    Push(Vec<Frame>),
}

impl Frame {
    /// Convert from resp-parser's Frame type
    fn from_resp3(frame: Resp3Frame) -> Self {
        match frame {
            // RESP2/RESP3 common types
            Resp3Frame::SimpleString(s) => Frame::SimpleString(s),
            Resp3Frame::Error(e) => Frame::Error(e),
            Resp3Frame::Integer(i) => Frame::Integer(i),
            Resp3Frame::BulkString(None) => Frame::Null,
            Resp3Frame::BulkString(Some(b)) => Frame::BulkString(Some(b)),
            Resp3Frame::Null => Frame::Null,

            // Arrays
            Resp3Frame::Array(None) => Frame::Null,
            Resp3Frame::Array(Some(arr)) => {
                Frame::Array(arr.into_iter().map(Frame::from_resp3).collect())
            }

            // RESP3-specific types
            Resp3Frame::Map(pairs) => {
                let converted: Vec<(Frame, Frame)> = pairs
                    .into_iter()
                    .map(|(k, v)| (Frame::from_resp3(k), Frame::from_resp3(v)))
                    .collect();
                Frame::Map(converted)
            }
            Resp3Frame::Set(items) => {
                Frame::Set(items.into_iter().map(Frame::from_resp3).collect())
            }
            Resp3Frame::Double(d) => Frame::Double(d),
            Resp3Frame::Boolean(b) => Frame::Boolean(b),
            Resp3Frame::Push(frames) => {
                Frame::Push(frames.into_iter().map(Frame::from_resp3).collect())
            }

            // Streaming types - convert to accumulated forms
            Resp3Frame::StreamedArray(arr) => {
                Frame::Array(arr.into_iter().map(Frame::from_resp3).collect())
            }
            Resp3Frame::StreamedSet(items) => {
                Frame::Set(items.into_iter().map(Frame::from_resp3).collect())
            }
            Resp3Frame::StreamedMap(pairs) => {
                let converted: Vec<(Frame, Frame)> = pairs
                    .into_iter()
                    .map(|(k, v)| (Frame::from_resp3(k), Frame::from_resp3(v)))
                    .collect();
                Frame::Map(converted)
            }

            // Other RESP3 types we don't fully support yet
            // BigNumber, VerbatimString, etc. - convert to bulk string for now
            _ => Frame::SimpleString(Bytes::from("OK")),
        }
    }

    /// Encode frame to bytes for transmission
    fn encode_to(&self, dst: &mut BytesMut) {
        match self {
            Frame::SimpleString(s) => {
                dst.put_u8(b'+');
                dst.put_slice(s);
                dst.put_slice(b"\r\n");
            }
            Frame::Error(e) => {
                dst.put_u8(b'-');
                dst.put_slice(e);
                dst.put_slice(b"\r\n");
            }
            Frame::Integer(i) => {
                dst.put_u8(b':');
                dst.put_slice(i.to_string().as_bytes());
                dst.put_slice(b"\r\n");
            }
            Frame::BulkString(None) | Frame::Null => {
                dst.put_slice(b"$-1\r\n");
            }
            Frame::BulkString(Some(bytes)) => {
                dst.put_u8(b'$');
                dst.put_slice(bytes.len().to_string().as_bytes());
                dst.put_slice(b"\r\n");
                dst.put_slice(bytes);
                dst.put_slice(b"\r\n");
            }
            Frame::Array(arr) => {
                dst.put_u8(b'*');
                dst.put_slice(arr.len().to_string().as_bytes());
                dst.put_slice(b"\r\n");
                for frame in arr {
                    frame.encode_to(dst);
                }
            }

            // RESP3-specific types
            Frame::Map(pairs) => {
                dst.put_u8(b'%');
                dst.put_slice(pairs.len().to_string().as_bytes());
                dst.put_slice(b"\r\n");
                for (key, value) in pairs {
                    key.encode_to(dst);
                    value.encode_to(dst);
                }
            }
            Frame::Set(items) => {
                dst.put_u8(b'~');
                dst.put_slice(items.len().to_string().as_bytes());
                dst.put_slice(b"\r\n");
                for item in items {
                    item.encode_to(dst);
                }
            }
            Frame::Double(d) => {
                dst.put_u8(b',');
                dst.put_slice(d.to_string().as_bytes());
                dst.put_slice(b"\r\n");
            }
            Frame::Boolean(b) => {
                dst.put_u8(b'#');
                dst.put_u8(if *b { b't' } else { b'f' });
                dst.put_slice(b"\r\n");
            }
            Frame::Push(frames) => {
                dst.put_u8(b'>');
                dst.put_slice(frames.len().to_string().as_bytes());
                dst.put_slice(b"\r\n");
                for frame in frames {
                    frame.encode_to(dst);
                }
            }
        }
    }
}

impl Decoder for RespCodec {
    type Item = Frame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        // Convert BytesMut to Bytes for zero-copy parsing
        let bytes = src.clone().freeze();

        // Use resp-parser to parse the frame
        match parse_frame(bytes) {
            Ok((frame, remaining)) => {
                // Calculate how many bytes were consumed
                let consumed = src.len() - remaining.len();

                // Advance the buffer past the consumed bytes
                src.advance(consumed);

                // Convert resp3 Frame to our Frame type
                Ok(Some(Frame::from_resp3(frame)))
            }
            Err(resp_parser::resp3::ParseError::Incomplete) => {
                // Not enough data yet, wait for more
                Ok(None)
            }
            Err(e) => {
                // Parse error
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("RESP parse error: {:?}", e),
                ))
            }
        }
    }
}

impl Encoder<Frame> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode_to(dst);
        Ok(())
    }
}
