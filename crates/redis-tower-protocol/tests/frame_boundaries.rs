//! Public-codec regressions for untrusted wire boundaries and resource limits.

use bytes::{Bytes, BytesMut};
use redis_tower_protocol::{Frame, ProtocolError, RespCodec, RespLimits};
use tokio_util::codec::Decoder;

fn codec(max_frame_size: usize, max_depth: usize) -> RespCodec {
    RespCodec::with_limits(RespLimits {
        max_frame_size,
        max_depth,
    })
}

fn bulk_wire(payload: &[u8]) -> Vec<u8> {
    let mut wire = format!("${}\r\n", payload.len()).into_bytes();
    wire.extend_from_slice(payload);
    wire.extend_from_slice(b"\r\n");
    wire
}

fn assert_unsupported(error: ProtocolError) {
    assert!(
        matches!(error, ProtocolError::Io(ref error) if error.kind() == std::io::ErrorKind::Unsupported),
        "expected an unsupported protocol error, got {error:?}"
    );
}

#[test]
fn complete_oversized_frames_are_rejected_without_consuming_bytes() {
    let cases: &[&[u8]] = &[
        b"$16\r\nabcdefghijklmnop\r\n",
        b"+abcdefghijklmnop\r\n",
        b"*3\r\n:1\r\n:2\r\n:3\r\n",
        b"%1\r\n+key\r\n+value\r\n",
    ];
    for &wire in cases {
        let mut input = BytesMut::from(wire);
        let error = codec(8, 16).decode(&mut input).unwrap_err();
        assert!(
            matches!(error, ProtocolError::FrameTooLarge { size, max: 8 } if size > 8),
            "wire {wire:?}: {error:?}"
        );
        assert_eq!(&input[..], wire);
    }
}

#[test]
fn fragmented_oversized_frames_never_escape_the_same_limit() {
    let cases: &[&[u8]] = &[
        b"$16\r\nabcdefghijklmnop\r\n",
        b"+abcdefghijklmnop\r\n",
        b"*3\r\n:1\r\n:2\r\n:3\r\n",
    ];
    for &wire in cases {
        for split in 0..=wire.len() {
            let mut decoder = codec(8, 16);
            let mut input = BytesMut::from(&wire[..split]);
            let first = decoder.decode(&mut input);
            assert_eq!(&input[..], &wire[..split]);
            let error = match first {
                Err(error) => error,
                Ok(None) => {
                    input.extend_from_slice(&wire[split..]);
                    let error = decoder.decode(&mut input).unwrap_err();
                    assert_eq!(&input[..], wire);
                    error
                }
                Ok(Some(frame)) => panic!("split {split} accepted oversized frame {frame:?}"),
            };
            assert!(
                matches!(error, ProtocolError::FrameTooLarge { size, max: 8 } if size > 8),
                "wire {wire:?}, split {split}: {error:?}"
            );
        }
    }
}

#[test]
fn exact_wire_size_limit_accepts_every_fragmentation_boundary() {
    let wire = bulk_wire(b"a\0\xff\r\nz");
    let expected = Frame::BulkString(Some(Bytes::from_static(b"a\0\xff\r\nz")));
    for split in 0..=wire.len() {
        let mut decoder = codec(wire.len(), 0);
        let mut input = BytesMut::from(&wire[..split]);
        if split == wire.len() {
            assert_eq!(decoder.decode(&mut input).unwrap(), Some(expected.clone()));
        } else {
            assert!(decoder.decode(&mut input).unwrap().is_none());
            assert_eq!(&input[..], &wire[..split]);
            input.extend_from_slice(&wire[split..]);
            assert_eq!(decoder.decode(&mut input).unwrap(), Some(expected.clone()));
        }
        assert!(input.is_empty());
    }
}

#[test]
fn aggregate_headers_reject_impossible_cardinality_before_receiving_children() {
    // These inputs are deliberately small. Previously, even this seven-byte
    // array header reached resp-rs and allocated space for 4096 Frame values.
    // Every declared child needs at least three wire bytes, already over cap.
    for wire in [b"*4096\r\n", b"~4096\r\n", b">4096\r\n", b"%4096\r\n"] {
        let mut input = BytesMut::from(&wire[..]);
        let error = codec(64, 16).decode(&mut input).unwrap_err();
        assert!(
            matches!(error, ProtocolError::FrameTooLarge { size, max: 64 } if size > 64),
            "header {wire:?}: {error:?}"
        );
        assert_eq!(&input[..], wire);
    }
}

#[test]
fn oversized_blob_declarations_fail_before_the_body_arrives() {
    for wire in [b"$4096\r\n", b"!4096\r\n", b"=4096\r\n"] {
        let mut input = BytesMut::from(&wire[..]);
        let error = codec(64, 16).decode(&mut input).unwrap_err();
        assert!(matches!(
            error,
            ProtocolError::FrameTooLarge { size, max: 64 } if size > 64
        ));
        assert_eq!(&input[..], wire);
    }
}

#[test]
fn pipeline_total_size_and_later_frames_do_not_change_the_first_frame_verdict() {
    let tails: &[&[u8]] = &[
        b"+OK\r\n+OK\r\n+OK\r\n",
        b"$4096\r\n",
        b"*1\r\n*1\r\n:1\r\n",
        b"*?\r\n:1\r\n.\r\n",
        b"|?\r\n",
        b"?not-a-frame\r\n",
    ];
    for &tail in tails {
        let mut input = BytesMut::from(&b"+OK\r\n"[..]);
        input.extend_from_slice(tail);
        let mut decoder = codec(5, 1);
        assert_eq!(
            decoder.decode(&mut input).unwrap(),
            Some(Frame::SimpleString(Bytes::from_static(b"OK"))),
            "later frame influenced the first reply: {tail:?}"
        );
        assert_eq!(&input[..], tail);
    }

    let mut decoder = codec(5, 0);
    let mut input = BytesMut::from(&b"+OK\r\n+OK\r\n+OK\r\n"[..]);
    for _ in 0..3 {
        assert_eq!(
            decoder.decode(&mut input).unwrap(),
            Some(Frame::SimpleString(Bytes::from_static(b"OK")))
        );
    }
    assert!(input.is_empty());
}

#[test]
fn all_streaming_tokens_fail_closed_at_top_level_and_inside_aggregates() {
    let tokens: &[&[u8]] = &[
        b"$?\r\n",
        b"!?\r\n",
        b"=?\r\n",
        b"*?\r\n",
        b"~?\r\n",
        b"%?\r\n",
        b">?\r\n",
        b"|?\r\n",
        b";3\r\nabc\r\n",
        b";0\r\n",
        b".\r\n",
    ];
    let wrappers: &[(&[u8], &[u8])] = &[
        (b"", b""),
        (b"*1\r\n", b""),
        (b"~1\r\n", b""),
        (b">1\r\n", b""),
        (b"%1\r\n+key\r\n", b""),
        (b"%1\r\n", b"+value\r\n"),
        (b"*1\r\n%1\r\n+key\r\n", b""),
    ];
    for &token in tokens {
        for &(prefix, suffix) in wrappers {
            let mut wire = prefix.to_vec();
            wire.extend_from_slice(token);
            wire.extend_from_slice(suffix);
            wire.extend_from_slice(b"+LATER\r\n");
            let mut input = BytesMut::from(&wire[..]);
            assert_unsupported(codec(1024, 16).decode(&mut input).unwrap_err());
            assert_eq!(&input[..], wire);
        }
    }
}

#[test]
fn fragmented_streaming_tokens_never_produce_partial_successful_replies() {
    let tokens: &[&[u8]] = &[
        b"$?\r\n",
        b"!?\r\n",
        b"=?\r\n",
        b"*?\r\n",
        b"~?\r\n",
        b"%?\r\n",
        b">?\r\n",
        b"|?\r\n",
        b";3\r\nabc\r\n",
        b";0\r\n",
        b".\r\n",
    ];
    for &token in tokens {
        let mut wire = b"%1\r\n+key\r\n".to_vec();
        wire.extend_from_slice(token);
        wire.extend_from_slice(b"+LATER\r\n");
        for split in 0..=wire.len() {
            let mut decoder = codec(1024, 16);
            let mut input = BytesMut::from(&wire[..split]);
            let first = decoder.decode(&mut input);
            assert_eq!(&input[..], &wire[..split]);
            let error = match first {
                Err(error) => error,
                Ok(None) => {
                    input.extend_from_slice(&wire[split..]);
                    let error = decoder.decode(&mut input).unwrap_err();
                    assert_eq!(&input[..], wire);
                    error
                }
                Ok(Some(frame)) => panic!("split {split} leaked streaming frame {frame:?}"),
            };
            assert_unsupported(error);
        }
    }
}

#[test]
fn fixed_attributes_remain_unsupported_even_when_nested() {
    let cases: &[&[u8]] = &[
        b"|0\r\n+OK\r\n",
        b"|1\r\n+key\r\n+value\r\n+OK\r\n",
        b"*1\r\n|0\r\n+OK\r\n",
        b"%1\r\n+key\r\n|0\r\n+OK\r\n",
    ];
    for &wire in cases {
        let mut input = BytesMut::from(wire);
        assert_unsupported(codec(1024, 16).decode(&mut input).unwrap_err());
        assert_eq!(&input[..], wire);
    }
}

#[test]
fn protocol_looking_binary_payloads_are_not_scanned_as_frames() {
    let payload = b"\xff\0*?\r\n|?\r\n;3\r\n.\r\n$4096\r\n*1\r\n%1\r\n";
    let mut wire = b"*1\r\n".to_vec();
    wire.extend_from_slice(&bulk_wire(payload));
    let expected = Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from_static(
        payload,
    )))]));
    for split in 0..wire.len() {
        let mut decoder = codec(wire.len(), 1);
        let mut input = BytesMut::from(&wire[..split]);
        assert!(decoder.decode(&mut input).unwrap().is_none());
        assert_eq!(&input[..], &wire[..split]);
        input.extend_from_slice(&wire[split..]);
        assert_eq!(decoder.decode(&mut input).unwrap(), Some(expected.clone()));
        assert!(input.is_empty());
    }
}

#[test]
fn empty_aggregates_count_depth_but_null_arrays_are_leaves() {
    for wire in [b"*0\r\n", b"~0\r\n", b"%0\r\n", b">0\r\n"] {
        let mut input = BytesMut::from(&wire[..]);
        assert!(matches!(
            codec(64, 0).decode(&mut input),
            Err(ProtocolError::NestingTooDeep { max: 0 })
        ));
        assert_eq!(&input[..], wire);
        assert!(codec(64, 1).decode(&mut input).unwrap().is_some());
        assert!(input.is_empty());
    }
    let mut null_array = BytesMut::from(&b"*-1\r\n"[..]);
    assert_eq!(
        codec(64, 0).decode(&mut null_array).unwrap(),
        Some(Frame::Array(None))
    );

    let mut nested_null = BytesMut::from(&b"*1\r\n*-1\r\n"[..]);
    assert!(codec(64, 1).decode(&mut nested_null).unwrap().is_some());
    let mut nested_empty = BytesMut::from(&b"%1\r\n+key\r\n*0\r\n"[..]);
    assert!(matches!(
        codec(64, 1).decode(&mut nested_empty),
        Err(ProtocolError::NestingTooDeep { max: 1 })
    ));
    assert!(codec(64, 2).decode(&mut nested_empty).unwrap().is_some());
}

#[test]
fn malformed_frames_preserve_buffer_and_parse_error_class() {
    let cases: &[&[u8]] = &[
        b"#maybe\r\n",
        b":not-an-integer\r\n",
        b"$-2\r\n",
        b"*not-a-count\r\n",
        b"%not-a-count\r\n",
        b"$3\r\nabcXX",
        b"?invalid\r\n",
    ];
    for &wire in cases {
        let mut input = BytesMut::from(wire);
        let error = codec(1024, 16).decode(&mut input).unwrap_err();
        assert!(matches!(error, ProtocolError::Parse(_)), "{error:?}");
        assert_eq!(&input[..], wire);
    }
}

#[test]
fn nested_maps_consume_the_same_depth_budget_as_other_aggregates() {
    let wire = b"%1\r\n+outer\r\n%1\r\n+inner\r\n+value\r\n";
    let mut input = BytesMut::from(&wire[..]);
    assert!(matches!(
        codec(wire.len(), 1).decode(&mut input),
        Err(ProtocolError::NestingTooDeep { max: 1 })
    ));
    assert_eq!(&input[..], wire);

    let mut input = BytesMut::from(&wire[..]);
    assert!(codec(wire.len(), 2).decode(&mut input).unwrap().is_some());
    assert!(input.is_empty());
}

#[test]
fn independent_mixed_wire_fixtures_are_partition_invariant() {
    let fixtures: &[&[u8]] = &[
        b"+OK\r\n-ERR nope\r\n:42\r\n$-1\r\n",
        b"%2\r\n+map\r\n~2\r\n+a\r\n+b\r\n+push\r\n>2\r\n+invalidate\r\n$1\r\nk\r\n",
        b"*6\r\n!4\r\noops\r\n,inf\r\n,-inf\r\n,nan\r\n(12345678901234567890\r\n=7\r\ntxt:abc\r\n",
        b"$25\r\n*?\r\n|1\r\n+looks\r\n+framed\r\n\r\n+LATER\r\n",
    ];

    for &wire in fixtures {
        let expected = decode_pipeline(wire, &[wire.len().max(1)]);
        for split in 0..=wire.len() {
            let first = split.max(1);
            let second = wire.len().saturating_sub(split).max(1);
            assert_eq!(
                decode_pipeline(wire, &[first, second]),
                expected,
                "partition {split} changed fixture {wire:?}"
            );
        }
    }
}

fn decode_pipeline(wire: &[u8], plan: &[usize]) -> (Vec<Frame>, Vec<u8>) {
    let mut decoder = codec(4096, 16);
    let mut input = BytesMut::new();
    let mut frames = Vec::new();
    let mut offset = 0;
    let mut plan_index = 0;
    while offset < wire.len() {
        let end = offset
            .saturating_add(plan[plan_index % plan.len()])
            .min(wire.len());
        plan_index += 1;
        input.extend_from_slice(&wire[offset..end]);
        offset = end;
        while let Some(frame) = decoder.decode(&mut input).unwrap() {
            frames.push(frame);
        }
    }
    (frames, input.to_vec())
}
