use bytes::{Buf, BytesMut};
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

        // Use clone().freeze() for a zero-copy Bytes view instead of copy_from_slice.
        // BytesMut::clone() is copy-on-write; freeze() converts to immutable Bytes
        // without allocating a new buffer. This avoids copying the entire receive
        // buffer on every decode call (particularly expensive under pipelining where
        // decode is called once per response frame from the same buffer).
        let input = src.clone().freeze();
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
    use bytes::Bytes;

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

    // -- RESP3 types --

    #[test]
    fn decode_double() {
        let mut buf = BytesMut::from(",2.72\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Double(2.72));
    }

    #[test]
    fn decode_boolean_true() {
        let mut buf = BytesMut::from("#t\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Boolean(true));
    }

    #[test]
    fn decode_boolean_false() {
        let mut buf = BytesMut::from("#f\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Boolean(false));
    }

    #[test]
    fn decode_null() {
        let mut buf = BytesMut::from("_\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Null);
    }

    #[test]
    fn decode_map() {
        let mut buf = BytesMut::from("%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Map(vec![
                (Frame::SimpleString(Bytes::from("key1")), Frame::Integer(1)),
                (Frame::SimpleString(Bytes::from("key2")), Frame::Integer(2)),
            ])
        );
    }

    #[test]
    fn decode_set() {
        let mut buf = BytesMut::from("~2\r\n+a\r\n+b\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Set(vec![
                Frame::SimpleString(Bytes::from("a")),
                Frame::SimpleString(Bytes::from("b")),
            ])
        );
    }

    #[test]
    fn decode_push() {
        let mut buf = BytesMut::from(">2\r\n+invalidate\r\n*1\r\n+key\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Push(vec![
                Frame::SimpleString(Bytes::from("invalidate")),
                Frame::Array(Some(vec![Frame::SimpleString(Bytes::from("key"))])),
            ])
        );
    }

    #[test]
    fn decode_blob_error() {
        let mut buf = BytesMut::from("!12\r\nSYNTAX error\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::BlobError(Bytes::from("SYNTAX error")));
    }

    #[test]
    fn decode_big_number() {
        let mut buf = BytesMut::from("(12345678901234567890\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::BigNumber(Bytes::from("12345678901234567890")));
    }

    #[test]
    fn decode_verbatim_string() {
        let mut buf = BytesMut::from("=15\r\ntxt:hello world\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::VerbatimString(Bytes::from("txt"), Bytes::from("hello world"))
        );
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

    // -- Edge-case tests --

    #[test]
    fn decode_large_bulk_string() {
        let payload = vec![b'x'; 1024 * 1024];
        let header = format!("${}\r\n", payload.len());
        let mut wire = BytesMut::from(header.as_bytes());
        wire.extend_from_slice(&payload);
        wire.extend_from_slice(b"\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut wire).unwrap().unwrap();
        assert_eq!(frame, Frame::BulkString(Some(Bytes::from(payload))));
    }

    #[test]
    fn decode_fragmented() {
        let wire = b"$5\r\nhello\r\n";
        let mut codec = RespCodec;
        let mut buf = BytesMut::new();
        let mut result = None;
        for &byte in wire.iter() {
            buf.extend_from_slice(&[byte]);
            if let Some(frame) = codec.decode(&mut buf).unwrap() {
                result = Some(frame);
                break;
            }
        }
        assert_eq!(
            result.unwrap(),
            Frame::BulkString(Some(Bytes::from("hello")))
        );
    }

    #[test]
    fn decode_zero_length_bulk_string() {
        let mut buf = BytesMut::from("$0\r\n\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::BulkString(Some(Bytes::new())));
    }

    #[test]
    fn decode_null_array() {
        let mut buf = BytesMut::from("*-1\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Array(None));
    }

    #[test]
    fn decode_empty_array() {
        let mut buf = BytesMut::from("*0\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Array(Some(vec![])));
    }

    #[test]
    fn decode_nested_array() {
        let mut buf = BytesMut::from("*2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n");
        let mut codec = RespCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Array(Some(vec![
                Frame::Array(Some(vec![
                    Frame::BulkString(Some(Bytes::from("foo"))),
                    Frame::BulkString(Some(Bytes::from("bar"))),
                ])),
                Frame::BulkString(Some(Bytes::from("baz"))),
            ]))
        );
    }

    #[test]
    fn decode_incomplete_no_crlf() {
        // Missing trailing \r\n — should return Ok(None), not an error.
        let mut buf = BytesMut::from("$5\r\nhello");
        let mut codec = RespCodec;
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn encode_null_bulk_string() {
        let mut buf = BytesMut::new();
        let mut codec = RespCodec;
        codec.encode(Frame::BulkString(None), &mut buf).unwrap();
        assert_eq!(&buf[..], b"$-1\r\n");
    }

    #[test]
    fn encode_empty_array() {
        let mut buf = BytesMut::new();
        let mut codec = RespCodec;
        codec.encode(Frame::Array(Some(vec![])), &mut buf).unwrap();
        assert_eq!(&buf[..], b"*0\r\n");
    }

    #[test]
    fn encode_nested_array() {
        let mut buf = BytesMut::new();
        let mut codec = RespCodec;
        codec
            .encode(
                Frame::Array(Some(vec![
                    Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from("a")))])),
                    Frame::Integer(1),
                ])),
                &mut buf,
            )
            .unwrap();
        assert_eq!(&buf[..], b"*2\r\n*1\r\n$1\r\na\r\n:1\r\n");
    }

    // -- Property-based tests --

    #[cfg(test)]
    mod prop_tests {
        use super::*;
        use proptest::prelude::*;

        fn arb_leaf_frame() -> impl Strategy<Value = Frame> {
            prop_oneof![
                "[a-zA-Z0-9 ]{0,20}".prop_map(|s| Frame::SimpleString(Bytes::from(s))),
                any::<i64>().prop_map(Frame::Integer),
                prop::collection::vec(any::<u8>(), 0..=64)
                    .prop_map(|v| Frame::BulkString(Some(Bytes::from(v)))),
                Just(Frame::BulkString(None)),
                any::<bool>().prop_map(Frame::Boolean),
                Just(Frame::Null),
            ]
        }

        fn arb_frame() -> impl Strategy<Value = Frame> {
            arb_leaf_frame().prop_recursive(3, 16, 4, |inner| {
                prop_oneof![
                    inner.clone(),
                    prop::collection::vec(inner.clone(), 0..=4).prop_map(|v| Frame::Array(Some(v))),
                ]
            })
        }

        proptest! {
            #[test]
            fn codec_roundtrip(frame in arb_frame()) {
                let mut codec = RespCodec;
                let mut buf = BytesMut::new();
                codec.encode(frame.clone(), &mut buf).unwrap();
                let decoded = codec.decode(&mut buf).unwrap().unwrap();
                prop_assert_eq!(frame, decoded);
            }
        }
    }
}
