//! RESP protocol codec for Tokio

use bytes::{BufMut, Bytes, BytesMut};
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
            Resp3Frame::StreamedAttribute(pairs) => {
                let converted: Vec<(Frame, Frame)> = pairs
                    .into_iter()
                    .map(|(k, v)| (Frame::from_resp3(k), Frame::from_resp3(v)))
                    .collect();
                Frame::Map(converted)
            }
            Resp3Frame::StreamedPush(frames) => {
                Frame::Push(frames.into_iter().map(Frame::from_resp3).collect())
            }
            Resp3Frame::StreamedString(chunks) => {
                // Concatenate all chunks into a single bulk string
                let total_len: usize = chunks.iter().map(|c| c.len()).sum();
                let mut combined = Vec::with_capacity(total_len);
                for chunk in chunks {
                    combined.extend_from_slice(&chunk);
                }
                Frame::BulkString(Some(Bytes::from(combined)))
            }

            // RESP3 types that need explicit handling
            Resp3Frame::BigNumber(num) => {
                // Convert big number to bulk string representation
                Frame::BulkString(Some(num))
            }
            Resp3Frame::VerbatimString(format, content) => {
                // Combine format and content with colon separator
                let mut combined = Vec::with_capacity(format.len() + 1 + content.len());
                combined.extend_from_slice(&format);
                combined.push(b':');
                combined.extend_from_slice(&content);
                Frame::BulkString(Some(Bytes::from(combined)))
            }
            Resp3Frame::SpecialFloat(val) => {
                // inf, -inf, nan - convert to string representation
                Frame::BulkString(Some(val))
            }
            Resp3Frame::BlobError(err) => {
                // Blob errors are like errors but with binary data
                Frame::Error(err)
            }
            Resp3Frame::Attribute(pairs) => {
                // Attributes are metadata, convert to map
                let converted: Vec<(Frame, Frame)> = pairs
                    .into_iter()
                    .map(|(k, v)| (Frame::from_resp3(k), Frame::from_resp3(v)))
                    .collect();
                Frame::Map(converted)
            }

            // Streaming headers and chunks should not appear in final parsed frames
            // These are intermediate states that parse_frame should handle
            Resp3Frame::StreamedStringHeader
            | Resp3Frame::StreamedBlobErrorHeader
            | Resp3Frame::StreamedVerbatimStringHeader
            | Resp3Frame::StreamedArrayHeader
            | Resp3Frame::StreamedSetHeader
            | Resp3Frame::StreamedMapHeader
            | Resp3Frame::StreamedAttributeHeader
            | Resp3Frame::StreamedPushHeader
            | Resp3Frame::StreamedStringChunk(_)
            | Resp3Frame::StreamTerminator => {
                // These should never appear as final frames from parse_frame
                // If they do, it indicates a protocol violation or parser bug
                Frame::Error(Bytes::from(
                    "ERR Unexpected streaming frame in non-streaming context",
                ))
            }
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

        // Use split() to get a zero-copy Bytes view without cloning the entire buffer
        // This is more efficient than clone().freeze() as it transfers ownership
        let bytes = src.split().freeze();

        // Use resp-parser to parse the frame
        // We need to clone here since parse_frame takes ownership and we might need
        // to restore the buffer on Incomplete/Error
        match parse_frame(bytes.clone()) {
            Ok((frame, remaining)) => {
                // Put back only the unconsumed bytes
                // This avoids keeping the consumed bytes in memory
                src.unsplit(remaining.into());

                // Convert resp3 Frame to our Frame type
                Ok(Some(Frame::from_resp3(frame)))
            }
            Err(resp_parser::resp3::ParseError::Incomplete) => {
                // Not enough data yet, restore the buffer and wait for more
                src.unsplit(bytes.into());
                Ok(None)
            }
            Err(e) => {
                // Parse error - restore buffer for potential recovery
                src.unsplit(bytes.into());
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

#[cfg(test)]
mod tests {
    use super::*;
    use resp_parser::resp3::Frame as Resp3Frame;

    #[test]
    fn test_big_number_conversion() {
        let big_num = Bytes::from("123456789012345678901234567890");
        let resp3_frame = Resp3Frame::BigNumber(big_num.clone());
        let frame = Frame::from_resp3(resp3_frame);

        match frame {
            Frame::BulkString(Some(data)) => {
                assert_eq!(data, big_num);
            }
            _ => panic!("Expected BulkString, got {:?}", frame),
        }
    }

    #[test]
    fn test_verbatim_string_conversion() {
        let format = Bytes::from("txt");
        let content = Bytes::from("Hello, World!");
        let resp3_frame = Resp3Frame::VerbatimString(format.clone(), content.clone());
        let frame = Frame::from_resp3(resp3_frame);

        match frame {
            Frame::BulkString(Some(data)) => {
                // Should be "txt:Hello, World!"
                assert_eq!(data[0..3], format[..]);
                assert_eq!(data[3], b':');
                assert_eq!(data[4..], content[..]);
            }
            _ => panic!("Expected BulkString, got {:?}", frame),
        }
    }

    #[test]
    fn test_special_float_inf() {
        let resp3_frame = Resp3Frame::SpecialFloat(Bytes::from("inf"));
        let frame = Frame::from_resp3(resp3_frame);

        match frame {
            Frame::BulkString(Some(data)) => {
                assert_eq!(data, Bytes::from("inf"));
            }
            _ => panic!("Expected BulkString, got {:?}", frame),
        }
    }

    #[test]
    fn test_special_float_neg_inf() {
        let resp3_frame = Resp3Frame::SpecialFloat(Bytes::from("-inf"));
        let frame = Frame::from_resp3(resp3_frame);

        match frame {
            Frame::BulkString(Some(data)) => {
                assert_eq!(data, Bytes::from("-inf"));
            }
            _ => panic!("Expected BulkString, got {:?}", frame),
        }
    }

    #[test]
    fn test_special_float_nan() {
        let resp3_frame = Resp3Frame::SpecialFloat(Bytes::from("nan"));
        let frame = Frame::from_resp3(resp3_frame);

        match frame {
            Frame::BulkString(Some(data)) => {
                assert_eq!(data, Bytes::from("nan"));
            }
            _ => panic!("Expected BulkString, got {:?}", frame),
        }
    }

    #[test]
    fn test_blob_error_conversion() {
        let error_msg =
            Bytes::from("WRONGTYPE Operation against a key holding the wrong kind of value");
        let resp3_frame = Resp3Frame::BlobError(error_msg.clone());
        let frame = Frame::from_resp3(resp3_frame);

        match frame {
            Frame::Error(data) => {
                assert_eq!(data, error_msg);
            }
            _ => panic!("Expected Error, got {:?}", frame),
        }
    }

    #[test]
    fn test_attribute_conversion() {
        let resp3_frame = Resp3Frame::Attribute(vec![
            (
                Resp3Frame::SimpleString(Bytes::from("key1")),
                Resp3Frame::Integer(42),
            ),
            (
                Resp3Frame::SimpleString(Bytes::from("key2")),
                Resp3Frame::SimpleString(Bytes::from("value")),
            ),
        ]);
        let frame = Frame::from_resp3(resp3_frame);

        match frame {
            Frame::Map(pairs) => {
                assert_eq!(pairs.len(), 2);
                // Verify first pair
                match (&pairs[0].0, &pairs[0].1) {
                    (Frame::SimpleString(k), Frame::Integer(v)) => {
                        assert_eq!(k, &Bytes::from("key1"));
                        assert_eq!(*v, 42);
                    }
                    _ => panic!("Unexpected first pair types"),
                }
            }
            _ => panic!("Expected Map, got {:?}", frame),
        }
    }

    #[test]
    fn test_streamed_string_conversion() {
        let chunks = vec![
            Bytes::from("Hello, "),
            Bytes::from("World"),
            Bytes::from("!"),
        ];
        let resp3_frame = Resp3Frame::StreamedString(chunks);
        let frame = Frame::from_resp3(resp3_frame);

        match frame {
            Frame::BulkString(Some(data)) => {
                assert_eq!(data, Bytes::from("Hello, World!"));
            }
            _ => panic!("Expected BulkString, got {:?}", frame),
        }
    }

    #[test]
    fn test_streamed_attribute_conversion() {
        let resp3_frame = Resp3Frame::StreamedAttribute(vec![(
            Resp3Frame::SimpleString(Bytes::from("ttl")),
            Resp3Frame::Integer(3600),
        )]);
        let frame = Frame::from_resp3(resp3_frame);

        match frame {
            Frame::Map(pairs) => {
                assert_eq!(pairs.len(), 1);
            }
            _ => panic!("Expected Map, got {:?}", frame),
        }
    }

    #[test]
    fn test_streamed_push_conversion() {
        let resp3_frame = Resp3Frame::StreamedPush(vec![
            Resp3Frame::SimpleString(Bytes::from("pubsub")),
            Resp3Frame::SimpleString(Bytes::from("message")),
            Resp3Frame::SimpleString(Bytes::from("channel1")),
            Resp3Frame::BulkString(Some(Bytes::from("hello"))),
        ]);
        let frame = Frame::from_resp3(resp3_frame);

        match frame {
            Frame::Push(frames) => {
                assert_eq!(frames.len(), 4);
            }
            _ => panic!("Expected Push, got {:?}", frame),
        }
    }

    #[test]
    fn test_streaming_header_returns_error() {
        let resp3_frame = Resp3Frame::StreamedStringHeader;
        let frame = Frame::from_resp3(resp3_frame);

        match frame {
            Frame::Error(msg) => {
                assert!(String::from_utf8_lossy(&msg).contains("Unexpected streaming frame"));
            }
            _ => panic!("Expected Error for streaming header, got {:?}", frame),
        }
    }

    #[test]
    fn test_stream_terminator_returns_error() {
        let resp3_frame = Resp3Frame::StreamTerminator;
        let frame = Frame::from_resp3(resp3_frame);

        match frame {
            Frame::Error(msg) => {
                assert!(String::from_utf8_lossy(&msg).contains("Unexpected streaming frame"));
            }
            _ => panic!("Expected Error for stream terminator, got {:?}", frame),
        }
    }

    #[test]
    fn test_streaming_chunk_returns_error() {
        let resp3_frame = Resp3Frame::StreamedStringChunk(Bytes::from("chunk"));
        let frame = Frame::from_resp3(resp3_frame);

        match frame {
            Frame::Error(msg) => {
                assert!(String::from_utf8_lossy(&msg).contains("Unexpected streaming frame"));
            }
            _ => panic!("Expected Error for streaming chunk, got {:?}", frame),
        }
    }

    #[test]
    fn test_no_fake_ok_responses() {
        // This test ensures we don't have any fake "OK" responses
        // All RESP3 types should be explicitly handled

        // Test all special types to ensure none return SimpleString("OK")
        let test_frames = vec![
            Resp3Frame::BigNumber(Bytes::from("123")),
            Resp3Frame::VerbatimString(Bytes::from("txt"), Bytes::from("content")),
            Resp3Frame::SpecialFloat(Bytes::from("inf")),
            Resp3Frame::BlobError(Bytes::from("error")),
            Resp3Frame::Attribute(vec![]),
        ];

        for resp3_frame in test_frames {
            let frame = Frame::from_resp3(resp3_frame);
            match frame {
                Frame::SimpleString(ref s) if s.as_ref() == b"OK" => {
                    panic!("Found fake OK response!");
                }
                _ => {} // Expected - any other response is fine
            }
        }
    }
}
