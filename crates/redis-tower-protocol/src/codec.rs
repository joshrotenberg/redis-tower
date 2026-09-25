use bytes::{Buf, Bytes, BytesMut};
use resp_rs::resp3;
use tokio_util::codec::{Decoder, Encoder};

use crate::Frame;
use crate::error::ProtocolError;

/// Default maximum wire size, in bytes, of a single decoded frame.
///
/// The 512 MiB limit includes headers, payloads, and aggregate children. It is
/// not a bulk-payload limit: a reply containing a 512 MiB value, or a larger
/// aggregate, requires a higher limit. Lower it for untrusted servers.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 512 * 1024 * 1024;

/// Default maximum nesting depth of a decoded frame.
///
/// Redis replies nest a handful of levels at most (`CLUSTER SLOTS` and
/// `XINFO STREAM` are the deepest in common use), so 128 leaves a wide margin
/// over real traffic while still bounding a hostile server's reply.
pub const DEFAULT_MAX_DEPTH: usize = 128;

/// Resource limits [`RespCodec`] applies while decoding.
///
/// Frames are structurally scanned before the allocating parser runs. The
/// size limit bounds wire bytes, not exact heap usage: decoded aggregate
/// elements have their own representation overhead. Incomplete frames never
/// reserve storage for their declared element counts. Set either field to
/// [`usize::MAX`] to disable that codec limit; dependency parsing limits remain.
///
/// ```
/// use redis_tower_protocol::{RespCodec, RespLimits};
///
/// let codec = RespCodec::with_limits(RespLimits {
///     max_frame_size: 8 * 1024 * 1024,
///     max_depth: 16,
/// });
/// assert_eq!(codec.limits().max_depth, 16);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RespLimits {
    /// Largest single frame's wire encoding, including headers and terminators.
    ///
    /// Enforced for complete and incomplete input before materialization. A
    /// declared length or element count that cannot fit is rejected early.
    /// This is not a total receive-buffer or exact decoded-heap limit: a burst
    /// of individually small pipelined replies is accepted.
    pub max_frame_size: usize,
    /// Deepest nesting of arrays, sets, pushes, and maps, outermost first.
    ///
    /// Empty aggregates count as a level; RESP2 null arrays are nil leaves.
    /// Attributes and streamed encodings are unsupported independently of depth.
    pub max_depth: usize,
}

impl Default for RespLimits {
    fn default() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_depth: DEFAULT_MAX_DEPTH,
        }
    }
}

/// Tokio codec for RESP3 frame encoding/decoding, backed by resp-rs.
///
/// Decoding enforces the [`RespLimits`] the codec was built with; encoding is
/// unaffected, since outbound frames are ones this client built itself.
/// Attributes and streamed RESP3 encodings fail closed with an unsupported-
/// operation error until their metadata/sequence can be attached to one reply.
/// Errors and incomplete input leave the receive buffer unchanged.
#[derive(Debug, Default, Clone, Copy)]
pub struct RespCodec {
    limits: RespLimits,
}

impl RespCodec {
    /// A codec with the default limits.
    pub fn new() -> Self {
        Self::default()
    }

    /// A codec with explicit decode limits.
    pub fn with_limits(limits: RespLimits) -> Self {
        Self { limits }
    }

    /// The limits this codec enforces while decoding.
    pub fn limits(&self) -> RespLimits {
        self.limits
    }
}

impl Decoder for RespCodec {
    type Item = Frame;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, ProtocolError> {
        let Some(frame_len) = crate::preflight::frame_len(src, self.limits)? else {
            return Ok(None);
        };

        // Materialize only a complete, bounded first frame. BytesMut::clone()
        // copies, so cloning the entire unread pipeline here would repeatedly
        // copy its shrinking remainder. Copying just this frame also preserves
        // the original buffer if the parser rejects scalar semantics.
        let input = Bytes::copy_from_slice(&src[..frame_len]);
        let (frame, remaining) = resp3::parse_frame(input)?;
        if !remaining.is_empty() {
            return Err(resp_rs::ParseError::InvalidFormat.into());
        }
        src.advance(frame_len);
        Ok(Some(frame))
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
    fn attributed_replies_fail_closed_before_consuming_pipeline_bytes() {
        for wire in [
            b"|1\r\n+ttl\r\n:1\r\n+first\r\n+second\r\n".as_slice(),
            b"*1\r\n|1\r\n+ttl\r\n:1\r\n+first\r\n+second\r\n".as_slice(),
        ] {
            let mut codec = RespCodec::new();
            let mut bytes = BytesMut::from(wire);
            assert!(matches!(
                codec.decode(&mut bytes),
                Err(ProtocolError::Io(error)) if error.kind() == std::io::ErrorKind::Unsupported
            ));
            assert_eq!(
                bytes.as_ref(),
                wire,
                "no reply may be mistaken for the next request's result"
            );
        }
    }

    #[test]
    fn decode_simple_string() {
        let mut buf = BytesMut::from("+OK\r\n");
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::SimpleString(Bytes::from("OK")));
    }

    #[test]
    fn decode_error() {
        let mut buf = BytesMut::from("-ERR unknown\r\n");
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Error(Bytes::from("ERR unknown")));
    }

    #[test]
    fn decode_integer() {
        let mut buf = BytesMut::from(":42\r\n");
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Integer(42));
    }

    #[test]
    fn decode_bulk_string() {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::BulkString(Some(Bytes::from("hello"))));
    }

    #[test]
    fn decode_null_bulk_string() {
        let mut buf = BytesMut::from("$-1\r\n");
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::BulkString(None));
    }

    #[test]
    fn decode_array() {
        let mut buf = BytesMut::from("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        let mut codec = RespCodec::new();
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
        let mut codec = RespCodec::new();
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn encode_frame() {
        let mut buf = BytesMut::new();
        let mut codec = RespCodec::new();
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
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Double(2.72));
    }

    #[test]
    fn decode_boolean_true() {
        let mut buf = BytesMut::from("#t\r\n");
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Boolean(true));
    }

    #[test]
    fn decode_boolean_false() {
        let mut buf = BytesMut::from("#f\r\n");
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Boolean(false));
    }

    #[test]
    fn decode_null() {
        let mut buf = BytesMut::from("_\r\n");
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Null);
    }

    #[test]
    fn decode_map() {
        let mut buf = BytesMut::from("%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n");
        let mut codec = RespCodec::new();
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
        let mut codec = RespCodec::new();
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
        let mut codec = RespCodec::new();
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
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::BlobError(Bytes::from("SYNTAX error")));
    }

    #[test]
    fn decode_big_number() {
        let mut buf = BytesMut::from("(12345678901234567890\r\n");
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::BigNumber(Bytes::from("12345678901234567890")));
    }

    #[test]
    fn decode_verbatim_string() {
        let mut buf = BytesMut::from("=15\r\ntxt:hello world\r\n");
        let mut codec = RespCodec::new();
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
        let mut codec = RespCodec::new();
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
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut wire).unwrap().unwrap();
        assert_eq!(frame, Frame::BulkString(Some(Bytes::from(payload))));
    }

    #[test]
    fn decode_fragmented() {
        let wire = b"$5\r\nhello\r\n";
        let mut codec = RespCodec::new();
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
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::BulkString(Some(Bytes::new())));
    }

    #[test]
    fn decode_null_array() {
        let mut buf = BytesMut::from("*-1\r\n");
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Array(None));
    }

    #[test]
    fn decode_empty_array() {
        let mut buf = BytesMut::from("*0\r\n");
        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Array(Some(vec![])));
    }

    #[test]
    fn decode_nested_array() {
        let mut buf = BytesMut::from("*2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n");
        let mut codec = RespCodec::new();
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
        let mut codec = RespCodec::new();
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn encode_null_bulk_string() {
        let mut buf = BytesMut::new();
        let mut codec = RespCodec::new();
        codec.encode(Frame::BulkString(None), &mut buf).unwrap();
        assert_eq!(&buf[..], b"$-1\r\n");
    }

    #[test]
    fn encode_empty_array() {
        let mut buf = BytesMut::new();
        let mut codec = RespCodec::new();
        codec.encode(Frame::Array(Some(vec![])), &mut buf).unwrap();
        assert_eq!(&buf[..], b"*0\r\n");
    }

    #[test]
    fn encode_nested_array() {
        let mut buf = BytesMut::new();
        let mut codec = RespCodec::new();
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
                "[a-zA-Z0-9 ._:/-]{0,24}".prop_map(|s| Frame::SimpleString(Bytes::from(s))),
                "[A-Z][A-Z0-9 ]{0,23}".prop_map(|s| Frame::Error(Bytes::from(s))),
                any::<i64>().prop_map(Frame::Integer),
                prop::collection::vec(any::<u8>(), 0..=64)
                    .prop_map(|v| Frame::BulkString(Some(Bytes::from(v)))),
                Just(Frame::BulkString(None)),
                prop::collection::vec(any::<u8>(), 0..=64)
                    .prop_map(|v| Frame::BlobError(Bytes::from(v))),
                any::<f64>()
                    .prop_filter("finite doubles have a stable Frame variant", |value| {
                        value.is_finite()
                    })
                    .prop_map(Frame::Double),
                prop_oneof![
                    Just(Frame::SpecialFloat(Bytes::from_static(b"inf"))),
                    Just(Frame::SpecialFloat(Bytes::from_static(b"-inf"))),
                    Just(Frame::SpecialFloat(Bytes::from_static(b"nan"))),
                ],
                any::<bool>().prop_map(Frame::Boolean),
                "-?[0-9]{1,48}".prop_map(|value| Frame::BigNumber(Bytes::from(value))),
                prop::collection::vec(any::<u8>(), 0..=64).prop_map(|content| {
                    Frame::VerbatimString(Bytes::from_static(b"txt"), Bytes::from(content))
                }),
                Just(Frame::Null),
                Just(Frame::Array(None)),
            ]
        }

        fn arb_frame() -> impl Strategy<Value = Frame> {
            arb_leaf_frame().prop_recursive(4, 32, 6, |inner| {
                prop_oneof![
                    prop::collection::vec(inner.clone(), 0..=5)
                        .prop_map(|items| Frame::Array(Some(items))),
                    prop::collection::vec(inner.clone(), 0..=5).prop_map(Frame::Set),
                    prop::collection::vec(inner.clone(), 0..=5).prop_map(Frame::Push),
                    prop::collection::vec((inner.clone(), inner), 0..=4).prop_map(Frame::Map),
                ]
            })
        }

        fn encode_frames(frames: &[Frame]) -> Vec<u8> {
            frames
                .iter()
                .flat_map(|frame| resp3::frame_to_bytes(frame).to_vec())
                .collect()
        }

        fn decode_whole(wire: &[u8]) -> (Vec<Frame>, Vec<u8>) {
            let mut codec = RespCodec::new();
            let mut buffer = BytesMut::from(wire);
            let mut frames = Vec::new();
            while let Some(frame) = codec.decode(&mut buffer).unwrap() {
                frames.push(frame);
            }
            (frames, buffer.to_vec())
        }

        fn decode_fragmented(wire: &[u8], plan: &[usize]) -> (Vec<Frame>, Vec<u8>) {
            let mut codec = RespCodec::new();
            let mut buffer = BytesMut::new();
            let mut frames = Vec::new();
            let mut offset = 0;
            let mut plan_index = 0;
            while offset < wire.len() {
                let chunk_size = plan[plan_index % plan.len()];
                plan_index += 1;
                let end = offset.saturating_add(chunk_size).min(wire.len());
                buffer.extend_from_slice(&wire[offset..end]);
                offset = end;
                while let Some(frame) = codec.decode(&mut buffer).unwrap() {
                    frames.push(frame);
                }
            }
            (frames, buffer.to_vec())
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(192))]

            #[test]
            fn codec_roundtrip(frame in arb_frame()) {
                let mut codec = RespCodec::new();
                let mut buf = BytesMut::new();
                codec.encode(frame.clone(), &mut buf).unwrap();
                let decoded = codec.decode(&mut buf).unwrap().unwrap();
                prop_assert_eq!(frame, decoded);
            }

            #[test]
            fn pipelines_are_partition_invariant(
                frames in prop::collection::vec(arb_frame(), 0..=16),
                plan in prop::collection::vec(1usize..=64, 1..=16),
            ) {
                let wire = encode_frames(&frames);
                let whole = decode_whole(&wire);
                let fragmented = decode_fragmented(&wire, &plan);
                prop_assert_eq!(&fragmented, &whole);
                prop_assert_eq!(whole.0, frames);
                prop_assert!(whole.1.is_empty());
            }

            #[test]
            fn truncated_pipelines_have_partition_invariant_prefixes(
                frames in prop::collection::vec(arb_frame(), 1..=12),
                plan in prop::collection::vec(1usize..=32, 1..=12),
                cutoff_seed in any::<usize>(),
            ) {
                let wire = encode_frames(&frames);
                let cutoff = cutoff_seed % (wire.len() + 1);
                let prefix = &wire[..cutoff];
                prop_assert_eq!(decode_fragmented(prefix, &plan), decode_whole(prefix));
            }
        }
    }
}

#[cfg(test)]
mod limit_tests {
    use super::*;
    use bytes::Bytes;

    /// `*1\r\n` repeated `depth` times around a single integer: the cheapest
    /// wire encoding of a deeply nested reply, and the shape a hostile server
    /// would use to drive `parse_frame`'s recursion.
    fn nested(depth: usize) -> BytesMut {
        let mut buf = BytesMut::new();
        for _ in 0..depth {
            buf.extend_from_slice(b"*1\r\n");
        }
        buf.extend_from_slice(b":1\r\n");
        buf
    }

    #[test]
    fn default_limits_are_the_documented_constants() {
        let limits = RespCodec::new().limits();
        assert_eq!(limits.max_frame_size, DEFAULT_MAX_FRAME_SIZE);
        assert_eq!(limits.max_depth, DEFAULT_MAX_DEPTH);
    }

    #[test]
    fn nesting_beyond_the_default_cap_is_rejected() {
        let mut codec = RespCodec::new();
        // Without the pre-scan this input recurses 100_000 frames deep inside
        // resp-rs and aborts the process with a stack overflow.
        let mut buf = nested(100_000);
        let err = codec.decode(&mut buf).unwrap_err();
        assert!(
            matches!(err, ProtocolError::NestingTooDeep { max } if max == DEFAULT_MAX_DEPTH),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn nesting_within_the_cap_still_decodes() {
        let mut codec = RespCodec::new();
        // The complete frame is scanned before recursive parsing.
        let mut buf = nested(DEFAULT_MAX_DEPTH);
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert!(matches!(frame, Frame::Array(Some(_))));
        assert!(buf.is_empty());
    }

    #[test]
    fn a_custom_depth_cap_is_honored_in_both_directions() {
        let limits = RespLimits {
            max_depth: 3,
            ..RespLimits::default()
        };

        let mut codec = RespCodec::with_limits(limits);
        let mut ok = nested(3);
        assert!(codec.decode(&mut ok).unwrap().is_some());

        let mut too_deep = nested(4);
        let err = codec.decode(&mut too_deep).unwrap_err();
        assert!(matches!(err, ProtocolError::NestingTooDeep { max: 3 }));
    }

    #[test]
    fn map_pairs_count_as_one_level_not_two() {
        // %1\r\n +k\r\n +v\r\n is depth 1: the pair is two children of one map,
        // not a level each. A scanner that counted pairs as nesting would
        // reject this.
        let limits = RespLimits {
            max_depth: 1,
            ..RespLimits::default()
        };
        let mut codec = RespCodec::with_limits(limits);
        let mut buf = BytesMut::from(&b"%1\r\n+k\r\n+v\r\n"[..]);
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Map(vec![(
                Frame::SimpleString(Bytes::from("k")),
                Frame::SimpleString(Bytes::from("v"))
            )])
        );
    }

    #[test]
    fn aggregate_markers_inside_a_bulk_payload_are_not_nesting() {
        // The scanner skips blob payloads whole. If it did not, this value
        // would read as 5_000 levels of array and be rejected, breaking a
        // client that stores RESP-looking bytes in Redis.
        let payload = b"*1\r\n".repeat(5_000);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(format!("${}\r\n", payload.len()).as_bytes());
        buf.extend_from_slice(&payload);
        buf.extend_from_slice(b"\r\n");

        let mut codec = RespCodec::new();
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::BulkString(Some(Bytes::from(payload))));
    }

    #[test]
    fn an_oversized_declared_frame_is_rejected_before_payload_arrives() {
        let limits = RespLimits {
            max_frame_size: 64,
            ..RespLimits::default()
        };
        let mut codec = RespCodec::with_limits(limits);

        // The declaration already establishes an impossible-to-fit extent.
        let mut buf = BytesMut::from(&b"$1000000\r\n"[..]);
        let err = codec.decode(&mut buf).unwrap_err();
        assert!(
            matches!(err, ProtocolError::FrameTooLarge { size, max } if size == 1_000_012 && max == 64),
            "unexpected error: {err:?}"
        );
        assert_eq!(buf.as_ref(), b"$1000000\r\n");
    }

    #[test]
    fn the_size_cap_applies_per_frame_not_to_pipelined_bytes() {
        // Three complete replies buffered together exceed a 6-byte cap in
        // total, but each is parsed and drained on its own, so none is
        // rejected. The cap bounds each frame, not the total receive buffer.
        let limits = RespLimits {
            max_frame_size: 6,
            ..RespLimits::default()
        };
        let mut codec = RespCodec::with_limits(limits);
        let mut buf = BytesMut::from(&b"+OK\r\n+OK\r\n+OK\r\n"[..]);

        for _ in 0..3 {
            let frame = codec.decode(&mut buf).unwrap().unwrap();
            assert_eq!(frame, Frame::SimpleString(Bytes::from("OK")));
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn limits_can_be_disabled() {
        let limits = RespLimits {
            max_frame_size: usize::MAX,
            max_depth: usize::MAX,
        };
        let mut codec = RespCodec::with_limits(limits);
        let mut buf = nested(64);
        assert!(codec.decode(&mut buf).unwrap().is_some());
    }

    #[test]
    fn every_wire_type_survives_a_tight_depth_cap() {
        // The scanner has to recognize each RESP3 type byte to skip it
        // correctly; a tag it mishandles would either miscount nesting or
        // desynchronize. Each of these is depth 1 under a cap of 1.
        let limits = RespLimits {
            max_depth: 1,
            ..RespLimits::default()
        };
        let mut codec = RespCodec::with_limits(limits);

        let cases: [&[u8]; 8] = [
            b"*8\r\n+s\r\n-e\r\n:1\r\n#t\r\n(12345678901234567890\r\n,1.5\r\n_\r\n$3\r\nabc\r\n",
            b"*2\r\n=9\r\ntxt:hello\r\n!5\r\nerror\r\n",
            b"~2\r\n+a\r\n+b\r\n",
            b">2\r\n+invalidate\r\n$1\r\nk\r\n",
            b"%2\r\n+k1\r\n:1\r\n+k2\r\n:2\r\n",
            b"*1\r\n$-1\r\n",
            b"*-1\r\n",
            b"*0\r\n",
        ];

        for case in cases {
            let mut buf = BytesMut::from(case);
            let decoded = codec.decode(&mut buf);
            assert!(
                decoded.is_ok(),
                "{:?} was rejected: {:?}",
                String::from_utf8_lossy(case),
                decoded.unwrap_err()
            );
            assert!(
                decoded.unwrap().is_some(),
                "{:?} decoded to nothing",
                String::from_utf8_lossy(case)
            );
        }
    }

    #[test]
    fn a_malformed_frame_is_still_the_parsers_verdict() {
        // Scalar semantics remain the parser's verdict after framing checks.
        let mut codec = RespCodec::with_limits(RespLimits {
            max_depth: 1,
            ..RespLimits::default()
        });
        let mut buf = BytesMut::from(&b"#maybe\r\n"[..]);
        assert!(matches!(
            codec.decode(&mut buf).unwrap_err(),
            ProtocolError::Parse(_)
        ));
    }
}
