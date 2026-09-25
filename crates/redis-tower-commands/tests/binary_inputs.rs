//! Byte-exact coverage for typed command arguments.

use bytes::Bytes;
use redis_tower_commands::*;
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

#[test]
fn lifecycle_blocking_scan_and_routing_arguments_are_byte_exact() {
    let key = b"key:{\xff}\0\r\n".as_slice();
    let other = b"dest:\x80".as_slice();
    let pattern = b"*\xff\r\n".as_slice();
    let empty = b"".as_slice();

    assert_eq!(
        wire_args(&Del::keys([key, empty])),
        bytes(&[b"DEL", key, empty])
    );
    assert_eq!(
        wire_args(&Rename::new(key, other)),
        bytes(&[b"RENAME", key, other])
    );
    assert_eq!(wire_args(&Keys::new(pattern)), bytes(&[b"KEYS", pattern]));
    assert_eq!(
        wire_args(&BLPop::keys([key, other], 0.5)),
        bytes(&[b"BLPOP", key, other, b"0.5"])
    );
    assert_eq!(
        wire_args(&HScan::new(key).match_pattern(pattern)),
        bytes(&[b"HSCAN", key, b"0", b"MATCH", pattern])
    );
    assert_eq!(
        wire_args(&Watch::keys([key, other])),
        bytes(&[b"WATCH", key, other])
    );
    assert_eq!(
        wire_args(&ClusterKeySlot::new(key)),
        bytes(&[b"CLUSTER", b"KEYSLOT", key])
    );
}

#[test]
fn scripts_pubsub_and_server_payloads_are_byte_exact() {
    let script = b"return ARGV[1]\n--\xff".as_slice();
    let key = b"script-key\0\xff".as_slice();
    let payload = b"$4\r\ndata\r\n\xff".as_slice();
    let channel = b"channel\xff\r\n".as_slice();

    assert_eq!(
        wire_args(&Eval::new(script).key(key).arg(payload)),
        bytes(&[b"EVAL", script, b"1", key, payload])
    );
    assert_eq!(
        wire_args(&FCall::new("function").key(key).arg(payload)),
        bytes(&[b"FCALL", b"function", b"1", key, payload])
    );
    assert_eq!(
        wire_args(&ScriptLoad::new(script)),
        bytes(&[b"SCRIPT", b"LOAD", script])
    );
    assert_eq!(
        wire_args(&FunctionLoad::new(payload)),
        bytes(&[b"FUNCTION", b"LOAD", payload])
    );
    assert_eq!(
        wire_args(&Publish::new(channel, payload)),
        bytes(&[b"PUBLISH", channel, payload])
    );
    assert_eq!(
        wire_args(&Ping::with_message(payload)),
        bytes(&[b"PING", payload])
    );
    assert_eq!(wire_args(&Echo::new(payload)), bytes(&[b"ECHO", payload]));
    assert_eq!(
        wire_args(&Auth::password(payload)),
        bytes(&[b"AUTH", payload])
    );
    assert_eq!(
        wire_args(&AclDryRun::new("default", "GET").arg(key)),
        bytes(&[b"ACL", b"DRYRUN", b"default", b"GET", key])
    );
    assert_eq!(
        wire_args(&CommandGetKeys::new("GET").arg(key)),
        bytes(&[b"COMMAND", b"GETKEYS", b"GET", key])
    );
}

#[test]
fn geo_hll_bitmap_and_diagnostics_are_byte_exact() {
    let key = b"structure:{\xff}".as_slice();
    let member = b"member\0\xff\r\n".as_slice();
    let dest = b"dest:\x80".as_slice();

    assert_eq!(
        wire_args(&GeoAdd::new(key).member(-73.9, 40.7, member)),
        bytes(&[b"GEOADD", key, b"-73.9", b"40.7", member])
    );
    assert_eq!(
        wire_args(&PfAdd::new(key, member)),
        bytes(&[b"PFADD", key, member])
    );
    assert_eq!(
        wire_args(&BitOp::new(BitOperation::Xor, dest, [key, member])),
        bytes(&[b"BITOP", b"XOR", dest, key, member])
    );
    assert_eq!(
        wire_args(&MemoryUsage::new(key)),
        bytes(&[b"MEMORY", b"USAGE", key])
    );
}

#[test]
fn module_keys_items_labels_and_elements_are_byte_exact() {
    let key = b"module:{\xff}\0".as_slice();
    let item = b"item\xff\r\n".as_slice();
    let suggestion = b"suggestion\0\xff\r\n".as_slice();
    let payload = b"payload\0\x80".as_slice();
    let label = b"label\xff".as_slice();

    assert_eq!(
        wire_args(&BfAdd::new(key, item)),
        bytes(&[b"BF.ADD", key, item])
    );
    assert_eq!(
        wire_args(&CmsQuery::new(key, [item, payload])),
        bytes(&[b"CMS.QUERY", key, item, payload])
    );
    assert_eq!(
        wire_args(&TdigestInfo::new(key)),
        bytes(&[b"TDIGEST.INFO", key])
    );
    assert_eq!(
        wire_args(&JsonGet::new(key).path("$")),
        bytes(&[b"JSON.GET", key, b"$"])
    );
    assert_eq!(
        wire_args(&FtSugAdd::new(key, suggestion, 1.5).payload(payload)),
        bytes(&[b"FT.SUGADD", key, suggestion, b"1.5", b"PAYLOAD", payload])
    );
    assert_eq!(
        wire_args(&TsCreate::new(key).label(label, payload)),
        bytes(&[b"TS.CREATE", key, b"LABELS", label, payload])
    );
    assert_eq!(
        wire_args(&VAdd::new(key, vec![1.0, 2.0], item)),
        bytes(&[b"VADD", key, b"VALUES", b"2", b"1", b"2", item])
    );
}
