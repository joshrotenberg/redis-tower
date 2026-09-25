# Binary data and typed command arguments

Redis keys and most data arguments are byte strings. They are not required to
be UTF-8. `redis-tower-commands` uses [`CommandArg`](https://docs.rs/redis-tower-commands/latest/redis_tower_commands/struct.CommandArg.html)
for binary-safe typed inputs wherever Redis treats an argument as opaque data:
keys, stored values, fields, members, patterns, script arguments, channels, and
module payloads.

```rust,ignore
use bytes::Bytes;
use redis_tower::commands::{Get, HSet, Set, XAdd};

let string_key = b"user:\xff".as_slice();
let hash_key = b"profile:\xff".as_slice();
let stream_key = b"events:\xff".as_slice();

client
    .execute(Set::new(string_key, vec![0x00, 0xfe, 0xff]))
    .await?;
let value: Option<Bytes> = client.execute(Get::new(string_key)).await?;

client
    .execute(HSet::new(
        hash_key,
        b"embedding".as_slice(),
        vector_bytes,
    ))
    .await?;
client
    .execute(XAdd::new(stream_key).field(b"payload", payload))
    .await?;
```

## Ownership and compatibility

`CommandArg` owns shared `Bytes` storage so command values remain `Send +
'static` and can be cloned for Tower middleware. Conversions behave as follows:

| Input | Storage behavior |
|---|---|
| `Bytes` | shares the existing allocation |
| `Vec<u8>` | takes ownership without copying the payload |
| `String` | takes ownership without copying the UTF-8 payload |
| `&[u8]`, byte arrays, `&str`, `&String` | copies once into owned storage |
| `CommandArg` | moves the value; cloning shares storage |

Ordinary string calls such as `Get::new("key")` and `Set::new(String::from("k"),
"v")` remain direct. Generic wrappers that want to accept both text and bytes
should use `impl Into<CommandArg>`. A wrapper intentionally restricted to text
can retain `impl Into<String>` and pass `value.into()` to a builder. As with the
old `impl Into<String>` API, avoid an unnecessary `.into()` on a literal at the
call site because an `impl Trait` parameter gives the conversion no unique
target; pass the literal directly or use `CommandArg::from(...)`.

The standard inputs accepted through `Into<String>` remain accepted directly,
including `Box<str>`, `char`, and `&mut str`. A downstream newtype that only
implemented `Into<String>` is the direct source-compatibility exception: either
convert it to `String` before calling the builder or implement
`From<YourType> for CommandArg`. This intentional break must not ship in a
`0.1.x` patch release. It requires `redis-tower-commands` 0.2.0 and
`redis-tower` 0.2.0, with workspace and downstream dependency requirements
updated when that release is prepared.

Stream field names and stream names in
`XREAD`/`XREADGROUP` results are now `Bytes`, as are `StreamMessage.stream` and
its field names. The previous `String` representation used lossy UTF-8
conversion and could not round-trip an input that the builder now accepts.
Consumer and group names in stream metadata are also `Bytes`. Stream IDs remain
`String` because Redis defines their grammar as decimal numbers separated by
`-`.

## Semantic inventory

The table is an argument-semantic inventory, not a search for Rust `String`
fields. “Text” means Redis assigns grammar or an encoding to the value; “opaque”
means Redis compares, stores, hashes, or routes the exact bytes.

| Family | Opaque inputs | Deliberately structured/text inputs | Typed status |
|---|---|---|---|
| Strings | keys and stored values, comparison values | integer/float increments, expiry numbers, digest syntax | `CommandArg` throughout |
| Hashes | keys, field names, field values | increments and expiry numbers | `CommandArg` throughout |
| Lists and non-blocking multi-list commands | keys, elements, pivots | indexes, counts, LEFT/RIGHT and BEFORE/AFTER tokens | `CommandArg` throughout |
| Sets | keys and members | counts and limits | `CommandArg` throughout |
| Sorted sets | keys and members; lex bounds contain a binary member after their Redis prefix | scores, rank indexes, aggregation and direction tokens | all builder byte strings use `CommandArg`; Redis validates bound grammar |
| Streams | stream keys, field names/values, group and consumer names | entry IDs/cursors, trim thresholds and option tokens | builder byte strings use `CommandArg`; stream keys and field names decode as `Bytes` |
| Keys and key lifecycle | keys, rename/copy destinations, patterns, serialized RESTORE payload | TTLs, database numbers, SORT grammar | opaque positions use `CommandArg` throughout |
| Blocking commands | list/zset keys | timeouts and direction tokens | keys use `CommandArg` throughout |
| Scan | collection keys and glob patterns | cursors, count, TYPE token | keys/patterns use `CommandArg`; results are `Bytes` |
| Scripting and functions | script bodies, KEYS and ARGV, function payloads | SHA-1 hex digests, function/library names and subcommand grammar | opaque positions use `CommandArg`; cached `Script` helpers also provide binary argument methods |
| Pub/Sub commands and sessions | channels, patterns, payloads | PUBSUB subcommands | typed commands and dedicated sessions are binary-safe |
| Geo and HyperLogLog | keys, members/elements | coordinates, units | opaque positions use `CommandArg` throughout |
| Bitmap, transaction, diagnostics, and server helpers | keys, WATCH keys, tracking prefixes and data-bearing arguments | numeric offsets plus administrative grammar | opaque positions use `CommandArg`; `PING`/`ECHO` echo responses are exact `Bytes` |
| ACL and cluster administration | ACL passwords and simulated command arguments; keys supplied to `CLUSTER KEYSLOT` | ACL usernames/rules/categories, node IDs, addresses, command grammar and numeric slots | deliberate split: opaque data uses `CommandArg`; operational identifiers remain text-first |
| Redis 8.8 arrays | keys, values, predicates | indexes and option grammar | already binary-safe |
| Bloom/Cuckoo, sketches, t-digest | keys and item payloads | capacities, probabilities and numeric observations | opaque positions use `CommandArg` throughout |
| JSON | keys | JSONPath and serialized JSON syntax | keys use `CommandArg`; structured JSON interfaces stay text/serde-oriented |
| Search | document keys, suggestion strings/payloads, tag values and vector blobs | index/schema/query syntax | opaque inputs and outputs preserve bytes; names and query DSL stay text-oriented; Redis Search treats NUL inside a suggestion string as a terminator |
| Time series | keys and label names/values | timestamps, reducers and retention/configuration | opaque inputs and returned keys/labels preserve bytes |
| Vector sets | keys and element names; vector bytes | JSON attributes and filter/query expression grammar | keys/elements use `CommandArg`; returned elements are `Bytes` |

`RawCommand::arg` remains the universal escape hatch for commands or extension
syntax outside the typed surface. It accepts `AsRef<[u8]>`, can retain a typed
response with `.query::<T>()`, and does not perform lossy conversion.

## Response decoding

Responses corresponding to opaque values preserve exact bytes. This includes
keys, members, stream names/fields, Search documents/suggestions/payloads/tag
values, TimeSeries keys/labels, Vector Set elements, TopK items, and echoed
`PING`/`ECHO` messages.

Redis-defined textual metadata remains `String`: scan cursors, stream IDs,
SHA-1 digests, JSON text, query plans, server diagnostic reports, ACL usernames
and rules, Cluster node descriptions and IDs, addresses, and command/config
names. Some of these existing metadata parsers use UTF-8 replacement for a
malformed server reply; that behavior cannot affect an opaque value because
those positions are classified separately above.

ACL usernames/rules/categories and Cluster node IDs/addresses intentionally
remain text-first. Redis exposes them as operational configuration grammar, not
application key/value payloads. In contrast, ACL passwords and `ACL DRYRUN`
arguments, and the key accepted by `CLUSTER KEYSLOT`, preserve exact bytes.

The client serializes Search suggestion strings as exact bulk-string bytes, but
the Redis Search autocomplete implementation treats an embedded NUL as a
string terminator. Suggestion keys and payloads remain binary-safe, and other
suggestion bytes round-trip subject to the server's autocomplete semantics.

## Cluster routing

Typed commands serialize a `CommandArg` directly into RESP bulk strings. The
Cluster key extractor reads those same frame bytes, so slot hashing and hash-tag
extraction happen before any decoding and use the exact key bytes. Braces inside
invalid UTF-8 keys therefore have the same routing meaning as braces in an ASCII
key.
