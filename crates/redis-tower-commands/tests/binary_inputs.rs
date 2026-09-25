//! Byte-exact coverage for typed command arguments.

use bytes::Bytes;
use redis_tower_commands::{CommandArg, Get, HSet, LPush, SAdd, Set, XAdd, ZAdd};
use redis_tower_core::{Command, Frame};

fn wire_args(command: &impl Command) -> Vec<Bytes> {
    match command.to_frame() {
        Frame::Array(Some(items)) => items
            .into_iter()
            .map(|item| match item {
                Frame::BulkString(Some(value)) => value,
                other => panic!("expected a bulk-string argument, got {other:?}"),
            })
            .collect(),
        other => panic!("expected a command array, got {other:?}"),
    }
}

fn bytes(items: &[&[u8]]) -> Vec<Bytes> {
    items
        .iter()
        .map(|item| Bytes::copy_from_slice(item))
        .collect()
}

#[test]
fn strings_preserve_invalid_utf8_and_protocol_looking_payloads() {
    let key = b"{\xff\x80}:key\0\r\n".as_slice();
    let value = b"$5\r\nhello\r\n\xff".as_slice();

    assert_eq!(wire_args(&Get::new(key)), bytes(&[b"GET", key]));
    assert_eq!(
        wire_args(&Set::new(key, value)),
        bytes(&[b"SET", key, value])
    );
}

#[test]
fn hashes_preserve_empty_fields_and_binary_values() {
    let key = b"hash:\xff".as_slice();
    let field = b"".as_slice();
    let value = b"\xf0\x28\x8c\x28\0".as_slice();

    assert_eq!(
        wire_args(&HSet::new(key, field, value)),
        bytes(&[b"HSET", key, field, value])
    );
}

#[test]
fn lists_sets_and_sorted_sets_keep_argument_boundaries() {
    let key = b"collection:{\xff}".as_slice();
    let first = b"a\r\nb".as_slice();
    let second = b"\xff\0".as_slice();

    assert_eq!(
        wire_args(&LPush::elements(key, [first, second])),
        bytes(&[b"LPUSH", key, first, second])
    );
    assert_eq!(
        wire_args(&SAdd::members(key, [first, second])),
        bytes(&[b"SADD", key, first, second])
    );
    assert_eq!(
        wire_args(&ZAdd::new(key).member(1.5, second)),
        bytes(&[b"ZADD", key, b"1.5", second])
    );
}

#[test]
fn streams_preserve_binary_names_fields_and_values() {
    let key = b"stream:{\xff}".as_slice();
    let field = b"field\0\xff".as_slice();
    let value = b"\r\n*2\r\n".as_slice();

    assert_eq!(
        wire_args(&XAdd::new(key).id("1-0").field(field, value)),
        bytes(&[b"XADD", key, b"1-0", field, value])
    );
}

#[test]
fn large_owned_values_and_clones_share_the_same_exact_bytes() {
    let payload = vec![0xa5; 256 * 1024];
    let argument = CommandArg::from(payload.clone());
    let cloned = argument.clone();

    assert_eq!(argument.as_bytes(), payload);
    assert_eq!(cloned.as_bytes(), payload);
    assert_eq!(
        wire_args(&Set::new("large", argument)),
        vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"large"),
            Bytes::from(payload),
        ]
    );
}

#[test]
fn string_ergonomics_and_existing_generic_wrappers_still_compile() {
    fn old_style_wrapper(key: impl Into<String>) -> Get {
        Get::new(key.into())
    }

    let owned = String::from("owned");
    let shared = Bytes::from_static(b"shared");
    let boxed: Box<str> = "boxed".into();
    let mut mutable = String::from("mutable");

    assert_eq!(wire_args(&Get::new("literal"))[1], b"literal"[..]);
    assert_eq!(wire_args(&Get::new(&owned))[1], b"owned"[..]);
    assert_eq!(wire_args(&Get::new(owned))[1], b"owned"[..]);
    assert_eq!(wire_args(&Get::new(&shared))[1], b"shared"[..]);
    assert_eq!(wire_args(&Get::new(shared))[1], b"shared"[..]);
    assert_eq!(wire_args(&Get::new(boxed))[1], b"boxed"[..]);
    assert_eq!(wire_args(&Get::new('λ'))[1], "λ".as_bytes()[..]);
    assert_eq!(
        wire_args(&Get::new(mutable.as_mut_str()))[1],
        b"mutable"[..]
    );
    assert_eq!(wire_args(&old_style_wrapper("generic"))[1], b"generic"[..]);
}
