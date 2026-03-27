use bytes::{Buf, Bytes, BytesMut};
use resp_rs::resp3;
use tokio_util::codec::{Decoder, Encoder};

use crate::Frame;
use crate::error::ProtocolError;

/// Tokio codec for RESP3 frame encoding/decoding, backed by resp-rs.
#[derive(Debug, Default)]
pub struct RespCodec;

impl Decoder for RespCodec {
    type Item = Frame;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, ProtocolError> {
        if src.is_empty() {
            return Ok(None);
        }

        let input = Bytes::copy_from_slice(src);
        match resp3::parse_frame(input) {
            Ok((frame, remaining)) => {
                let consumed = src.len() - remaining.len();
                src.advance(consumed);
                Ok(Some(frame))
            }
            Err(resp_rs::ParseError::Incomplete) => Ok(None),
            Err(e) => Err(ProtocolError::Parse(e)),
        }
    }
}

impl Encoder<Frame> for RespCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), ProtocolError> {
        let serialized = resp3::frame_to_bytes(&item);
        dst.extend_from_slice(&serialized);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_simple_string() {
        let mut buf = BytesMut::from("+OK\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::SimpleString(Bytes::from("OK")));
    }

    #[test]
    fn decode_error() {
        let mut buf = BytesMut::from("-ERR unknown\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Error(Bytes::from("ERR unknown")));
    }

    #[test]
    fn decode_integer() {
        let mut buf = BytesMut::from(":42\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Integer(42));
    }

    #[test]
    fn decode_bulk_string() {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::BulkString(Some(Bytes::from("hello"))));
    }

    #[test]
    fn decode_null_bulk_string() {
        let mut buf = BytesMut::from("$-1\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::BulkString(None));
    }

    #[test]
    fn decode_array() {
        let mut buf = BytesMut::from("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from("GET"))),
                Frame::BulkString(Some(Bytes::from("key"))),
            ]))
        );
    }

    #[test]
    fn decode_incomplete() {
        let mut buf = BytesMut::from("$5\r\nhel");
        let mut codec = RespCodec;
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn encode_frame() {
        let mut buf = BytesMut::new();
        let mut codec = RespCodec;
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
            Frame::BulkString(Some(Bytes::from("value"))),
        ]));
        codec.encode(frame, &mut buf).unwrap();
        assert_eq!(&buf[..], b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
    }

    #[test]
    fn roundtrip() {
        let original = Frame::Array(Some(vec![
            Frame::SimpleString(Bytes::from("OK")),
            Frame::Integer(42),
            Frame::BulkString(Some(Bytes::from("hello"))),
            Frame::BulkString(None),
        ]));
        let serialized = resp3::frame_to_bytes(&original);
        let mut buf = BytesMut::from(&serialized[..]);
        let mut codec = RespCodec;
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(original, decoded);
    }
}
