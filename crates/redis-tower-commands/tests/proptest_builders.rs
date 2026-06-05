//! Property-based tests for command frame builders (issue #347).
//!
//! These tests feed randomly generated keys, values, fields, and scores into the
//! typed command builders (`Set`, `ZAdd`, `HSet`, ...) and assert that the frame
//! produced by [`Command::to_frame`] is *well-formed*:
//!
//! 1. It is a top-level `Frame::Array(Some(_))` whose elements are all
//!    non-null bulk strings (the RESP shape Redis expects for a command).
//! 2. The first element is the upper-cased command name.
//! 3. Every caller-supplied key/value/field byte sequence appears verbatim as a
//!    bulk-string argument in the frame.
//! 4. The frame survives an encode -> decode roundtrip through [`RespCodec`]
//!    without error and compares equal.
//!
//! The edge cases called out in the issue -- keys with spaces, `\r\n`, NUL bytes,
//! and very long keys -- are covered both by the random UTF-8 generator (which
//! freely emits control characters) and by dedicated targeted tests.

use bytes::BytesMut;
use proptest::prelude::*;
use redis_tower_commands::{HSet, Set, ZAdd};
use redis_tower_core::{Command, Frame, RespCodec};
use tokio_util::codec::{Decoder, Encoder};

// -- Frame inspection helpers --

/// Returns the bulk-string argument payloads of a command frame, asserting along
/// the way that the frame is a well-formed RESP command: a non-null array whose
/// every element is a non-null bulk string.
fn command_args(frame: &Frame) -> Vec<Vec<u8>> {
    let elements = match frame {
        Frame::Array(Some(elements)) => elements,
        other => panic!("command frame must be a non-null array, got {other:?}"),
    };
    assert!(
        !elements.is_empty(),
        "command frame must have at least the command name"
    );
    elements
        .iter()
        .map(|el| match el {
            Frame::BulkString(Some(bytes)) => bytes.to_vec(),
            other => panic!("every command argument must be a non-null bulk string, got {other:?}"),
        })
        .collect()
}

/// Asserts the frame encodes and decodes back to an equal frame via `RespCodec`.
fn assert_codec_roundtrip(frame: &Frame) {
    let mut codec = RespCodec;
    let mut buf = BytesMut::new();
    codec
        .encode(frame.clone(), &mut buf)
        .expect("encoding a builder-produced frame must not fail");
    let decoded = codec
        .decode(&mut buf)
        .expect("decoding a builder-produced frame must not fail")
        .expect("a complete frame must decode to Some");
    assert_eq!(
        *frame, decoded,
        "frame must survive an encode -> decode roundtrip unchanged"
    );
    assert!(
        buf.is_empty(),
        "decoding should consume the entire buffer, {} bytes left",
        buf.len()
    );
}

// -- Strategies --

/// Arbitrary UTF-8 string, including control characters (spaces, `\r`, `\n`,
/// NUL, etc.). `.{0,64}` in proptest's regex strategy matches any Unicode
/// scalar value, so this naturally exercises the tricky bytes the issue calls
/// out without special-casing them.
fn arb_redis_string() -> impl Strategy<Value = String> {
    ".{0,64}"
}

proptest! {
    // -- SET --

    /// SET key value [flags]: the frame is well-formed, names the command, and
    /// carries the exact key and value bytes in argument positions 1 and 2.
    #[test]
    fn set_frame_is_well_formed(key in arb_redis_string(), value in arb_redis_string()) {
        let frame = Set::new(key.clone(), value.clone()).to_frame();
        let args = command_args(&frame);

        prop_assert_eq!(&args[0], b"SET");
        prop_assert_eq!(&args[1], key.as_bytes());
        prop_assert_eq!(&args[2], value.as_bytes());
        assert_codec_roundtrip(&frame);
    }

    /// SET with optional flags still round-trips and still carries the key and
    /// value verbatim regardless of which flags are present.
    #[test]
    fn set_with_flags_is_well_formed(
        key in arb_redis_string(),
        value in arb_redis_string(),
        ex in proptest::option::of(any::<u64>()),
        nx in any::<bool>(),
        get in any::<bool>(),
    ) {
        let mut cmd = Set::new(key.clone(), value.clone());
        if let Some(secs) = ex {
            cmd = cmd.ex(secs);
        }
        if nx {
            cmd = cmd.nx();
        }
        if get {
            cmd = cmd.get();
        }
        let frame = cmd.to_frame();
        let args = command_args(&frame);

        prop_assert_eq!(&args[0], b"SET");
        prop_assert_eq!(&args[1], key.as_bytes());
        prop_assert_eq!(&args[2], value.as_bytes());
        // The key/value bytes must not be corrupted by flag handling.
        prop_assert!(args.iter().any(|a| a.as_slice() == key.as_bytes()));
        prop_assert!(args.iter().any(|a| a.as_slice() == value.as_bytes()));
        assert_codec_roundtrip(&frame);
    }

    // -- HSET --

    /// HSET key field value [field value ...]: well-formed, command-named, and
    /// every generated field and value byte sequence is present in the frame.
    #[test]
    fn hset_frame_is_well_formed(
        key in arb_redis_string(),
        pairs in proptest::collection::vec((arb_redis_string(), arb_redis_string()), 1..8),
    ) {
        let ((first_field, first_value), rest) = pairs.split_first().unwrap();
        let mut cmd = HSet::new(key.clone(), first_field.clone(), first_value.clone());
        for (field, value) in rest {
            cmd = cmd.field(field.clone(), value.clone());
        }
        let frame = cmd.to_frame();
        let args = command_args(&frame);

        prop_assert_eq!(&args[0], b"HSET");
        prop_assert_eq!(&args[1], key.as_bytes());
        // HSET key f1 v1 f2 v2 ... -> 2 + 2 * pairs args.
        prop_assert_eq!(args.len(), 2 + 2 * pairs.len());
        for (i, (field, value)) in pairs.iter().enumerate() {
            prop_assert_eq!(&args[2 + 2 * i], field.as_bytes());
            prop_assert_eq!(&args[2 + 2 * i + 1], value.as_bytes());
        }
        assert_codec_roundtrip(&frame);
    }

    // -- ZADD --

    /// ZADD key score member [score member ...]: well-formed, command-named, and
    /// every member byte sequence is present. Scores are arbitrary finite f64.
    #[test]
    fn zadd_frame_is_well_formed(
        key in arb_redis_string(),
        members in proptest::collection::vec(
            (any::<f64>().prop_filter("finite", |f| f.is_finite()), arb_redis_string()),
            1..8,
        ),
    ) {
        let mut cmd = ZAdd::new(key.clone());
        for (score, member) in &members {
            cmd = cmd.member(*score, member.clone());
        }
        let frame = cmd.to_frame();
        let args = command_args(&frame);

        prop_assert_eq!(&args[0], b"ZADD");
        prop_assert_eq!(&args[1], key.as_bytes());
        // ZADD key s1 m1 s2 m2 ... -> 2 + 2 * members args (no flags set).
        prop_assert_eq!(args.len(), 2 + 2 * members.len());
        for (i, (score, member)) in members.iter().enumerate() {
            let score_str = score.to_string();
            prop_assert_eq!(&args[2 + 2 * i], score_str.as_bytes());
            prop_assert_eq!(&args[2 + 2 * i + 1], member.as_bytes());
        }
        assert_codec_roundtrip(&frame);
    }
}

// -- Targeted edge-case tests (issue #347: spaces, \r\n, NUL, very long keys) --

#[test]
fn set_handles_crlf_nul_and_spaces() {
    for key in [
        "key with spaces",
        "key\r\nwith\r\ncrlf",
        "key\0with\0nul",
        "\r\n\0 mixed \0\r\n",
    ] {
        let frame = Set::new(key, "value").to_frame();
        let args = command_args(&frame);
        assert_eq!(&args[1], key.as_bytes());
        assert_codec_roundtrip(&frame);
    }
}

#[test]
fn set_handles_very_long_key() {
    // A key larger than typical inline limits; the builder and codec must treat
    // it as opaque bulk-string bytes with a correct length prefix.
    let key = "k".repeat(1024 * 1024);
    let frame = Set::new(key.clone(), "v").to_frame();
    let args = command_args(&frame);
    assert_eq!(args[1].len(), key.len());
    assert_eq!(&args[1], key.as_bytes());
    assert_codec_roundtrip(&frame);
}

#[test]
fn hset_and_zadd_handle_crlf_and_nul() {
    let weird = "f\r\n\0ield";
    let hset = HSet::new("h", weird, "v\0\r\n").to_frame();
    let hargs = command_args(&hset);
    assert_eq!(&hargs[2], weird.as_bytes());
    assert_codec_roundtrip(&hset);

    let zadd = ZAdd::new("z").member(1.5, weird).to_frame();
    let zargs = command_args(&zadd);
    assert_eq!(&zargs[3], weird.as_bytes());
    assert_codec_roundtrip(&zadd);
}
