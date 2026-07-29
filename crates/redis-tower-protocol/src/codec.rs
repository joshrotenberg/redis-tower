use bytes::{Buf, BytesMut};
use resp_rs::resp3;
use tokio_util::codec::{Decoder, Encoder};

use crate::Frame;
use crate::error::ProtocolError;

/// Default maximum size, in bytes, of a single frame the decoder will buffer.
///
/// Set to 512 MiB, which is Redis's own `proto-max-bulk-len` ceiling, so the
/// default cannot reject a reply a Redis server could legitimately send.
/// Lower it when talking to a server that is not fully trusted.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 512 * 1024 * 1024;

/// Default maximum nesting depth of a decoded frame.
///
/// Redis replies nest a handful of levels at most (`CLUSTER SLOTS` and
/// `XINFO STREAM` are the deepest in common use), so 128 leaves a wide margin
/// over real traffic while still bounding a hostile server's reply.
pub const DEFAULT_MAX_DEPTH: usize = 128;

/// Minimum wire bytes one nesting level can occupy (`*1\r\n`).
const MIN_BYTES_PER_LEVEL: usize = 4;

/// Resource limits [`RespCodec`] applies while decoding.
///
/// Both limits exist to bound what a malicious or compromised server can make
/// the client allocate. Set either field to [`usize::MAX`] to disable it.
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
    /// Largest single frame, in bytes, the decoder will buffer before failing.
    ///
    /// This bounds the incomplete frame currently being assembled, not the
    /// total bytes in the read buffer: a pipelined burst of many small frames
    /// is never rejected, because each is parsed and drained as it completes.
    pub max_frame_size: usize,
    /// Deepest nesting the decoder will accept, counting aggregate frames
    /// (arrays, sets, pushes, maps, attributes) from the outermost inward.
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
        if src.is_empty() {
            return Ok(None);
        }

        // Depth is checked before the buffer reaches the parser, not after:
        // resp-rs recurses once per nesting level and tracks no depth of its
        // own, so a hostile `*1\r\n` chain would overflow the stack inside
        // `parse_frame` before any post-hoc inspection could run.
        //
        // A frame nested `d` levels deep needs at least `4 * d` bytes, so a
        // buffer too short to reach the cap skips the scan entirely and the
        // common case of a small reply pays nothing.
        if src.len() >= self.limits.max_depth.saturating_mul(MIN_BYTES_PER_LEVEL)
            && exceeds_depth(src, self.limits.max_depth)
        {
            return Err(ProtocolError::NestingTooDeep {
                max: self.limits.max_depth,
            });
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
            Err(resp_rs::ParseError::Incomplete) => {
                // Everything buffered here belongs to one unfinished frame:
                // any frame ahead of it already parsed and drained. Refusing
                // to keep buffering is what bounds the allocation a server can
                // drive by declaring a length it never sends.
                if src.len() > self.limits.max_frame_size {
                    return Err(ProtocolError::FrameTooLarge {
                        size: src.len(),
                        max: self.limits.max_frame_size,
                    });
                }
                Ok(None)
            }
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

/// One step of the depth scan: the index just past the element, and whether it
/// opened a new nesting level.
enum Step {
    /// A complete element that owns no children.
    Leaf(usize),
    /// An aggregate that opened a level owing this many children.
    Open(usize, usize),
}

/// Returns `true` if the first frame in `buf` nests deeper than `max_depth`.
///
/// This is a structural pre-pass, not a parser. It walks element headers,
/// skips blob payloads whole, and tracks how many children each open aggregate
/// still owes. It allocates no frame and never recurses, so it is safe to run
/// on input that would overflow the stack inside `parse_frame`.
///
/// It is deliberately permissive. Anything it cannot interpret -- a truncated
/// buffer, an unknown type byte, a malformed length -- ends the scan with
/// `false`, leaving `resp_rs::resp3::parse_frame` the single authority on what
/// is a valid frame. The one judgement made here is "deeper than `max_depth`".
fn exceeds_depth(buf: &[u8], max_depth: usize) -> bool {
    // Children still owed by each currently-open aggregate, innermost last.
    let mut open: Vec<usize> = Vec::new();
    let mut pos = 0usize;

    loop {
        let Some(&tag) = buf.get(pos) else {
            return false;
        };

        let step = match tag {
            // Line-delimited leaves: simple string, error, integer, boolean,
            // big number, double, null, stream terminator.
            b'+' | b'-' | b':' | b'#' | b'(' | b',' | b'_' | b'.' => {
                match crlf_after(buf, pos + 1) {
                    Some(after) => Step::Leaf(after),
                    None => return false,
                }
            }
            // Length-prefixed blobs: bulk string, blob error, verbatim string,
            // streamed chunk. Skipping the payload whole is what keeps a `*`
            // byte inside a value from being counted as nesting.
            b'$' | b'!' | b'=' | b';' => match blob_end(buf, pos) {
                Some(after) => Step::Leaf(after),
                None => return false,
            },
            // Aggregates of single elements.
            b'*' | b'~' | b'>' => match header_count(buf, pos) {
                Some((Some(n), after)) if n > 0 => Step::Open(n, after),
                // An empty, null, or streamed header owns no children.
                Some((_, after)) => Step::Leaf(after),
                None => return false,
            },
            // Aggregates of key/value pairs.
            b'%' | b'|' => match header_count(buf, pos) {
                Some((Some(n), after)) if n > 0 => match n.checked_mul(2) {
                    Some(children) => Step::Open(children, after),
                    None => return false,
                },
                Some((_, after)) => Step::Leaf(after),
                None => return false,
            },
            _ => return false,
        };

        match step {
            Step::Open(children, after) => {
                open.push(children);
                if open.len() > max_depth {
                    return true;
                }
                pos = after;
            }
            Step::Leaf(after) => {
                pos = after;
                // Settle the finished element against its parents, closing
                // every aggregate whose last child it was. A closed aggregate
                // is itself an element of its own parent, so this walks out.
                loop {
                    match open.last_mut() {
                        // Nothing left open: the outermost frame is complete.
                        None => return false,
                        Some(remaining) => {
                            *remaining -= 1;
                            if *remaining > 0 {
                                break;
                            }
                            open.pop();
                        }
                    }
                }
            }
        }
    }
}

/// Index just past the CRLF ending the line that starts at `from`.
fn crlf_after(buf: &[u8], from: usize) -> Option<usize> {
    line_end(buf, from).map(|end| end + 2)
}

/// Index of the `\r` ending the line that starts at `from`.
fn line_end(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Index just past a length-prefixed blob, payload and trailing CRLF included.
///
/// The returned index may sit past the end of `buf` when the payload has not
/// arrived yet; the caller's `buf.get` then ends the scan.
fn blob_end(buf: &[u8], pos: usize) -> Option<usize> {
    let end = line_end(buf, pos + 1)?;
    let header = &buf[pos + 1..end];
    // `?` opens a streamed string and `-1` is a null bulk string; neither
    // carries a payload of its own.
    if header == b"?" || header == b"-1" {
        return Some(end + 2);
    }
    let len = parse_len(header)?;
    end.checked_add(2)?.checked_add(len)?.checked_add(2)
}

/// The element count on an aggregate header, plus the index just past that
/// header line. A `None` count means the header owns no children: `?` opens a
/// streamed aggregate and `-1` is the null array.
fn header_count(buf: &[u8], pos: usize) -> Option<(Option<usize>, usize)> {
    let end = line_end(buf, pos + 1)?;
    let header = &buf[pos + 1..end];
    if header == b"?" || header == b"-1" {
        return Some((None, end + 2));
    }
    Some((Some(parse_len(header)?), end + 2))
}

/// Parse a non-negative decimal length. Returns `None` on anything else, which
/// defers the input to the real parser.
fn parse_len(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() {
        return None;
    }
    let mut n: usize = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((b - b'0') as usize)?;
    }
    Some(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

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
                let mut codec = RespCodec::new();
                let mut buf = BytesMut::new();
                codec.encode(frame.clone(), &mut buf).unwrap();
                let decoded = codec.decode(&mut buf).unwrap().unwrap();
                prop_assert_eq!(frame, decoded);
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
        // Deep enough to run the scan (past the 4 * max_depth fast path) but
        // inside the limit.
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
    fn an_incomplete_frame_past_the_size_cap_is_rejected() {
        let limits = RespLimits {
            max_frame_size: 64,
            ..RespLimits::default()
        };
        let mut codec = RespCodec::with_limits(limits);

        // A server declaring a large payload and then trickling it: under the
        // cap the decoder keeps waiting, over it the connection fails instead
        // of buffering whatever the server chooses to declare.
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"$1000000\r\n");
        buf.extend_from_slice(&[b'x'; 40]);
        assert!(codec.decode(&mut buf).unwrap().is_none());

        buf.extend_from_slice(&[b'x'; 40]);
        let err = codec.decode(&mut buf).unwrap_err();
        assert!(
            matches!(err, ProtocolError::FrameTooLarge { size, max } if size == 90 && max == 64),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn the_size_cap_applies_per_frame_not_to_pipelined_bytes() {
        // Three complete replies buffered together exceed a 6-byte cap in
        // total, but each is parsed and drained on its own, so none is
        // rejected. Only an unfinished frame is measured against the cap.
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
        // The scan defers on anything it cannot interpret, so garbage keeps
        // producing the parse error it always did rather than a limit error.
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
