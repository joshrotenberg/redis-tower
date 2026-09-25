# Differential testing against redis-rs

The differential corpus uses redis-rs as an independent implementation oracle,
not as the Redis specification. Both clients run commands against the same
server version but use separate key namespaces for every mutation. When the
clients disagree, Redis documentation and server behavior decide the expected
contract.

The test-only dependency is pinned to `redis = 1.7.0`; the per-PR job records
the resolved oracle package with `cargo tree` alongside the Redis server
version. It is not a production dependency.
Standalone command and conversion failures report the case, command or step,
adapter side, selected RESP protocol, live Redis version, both client versions,
and deterministic seed (`0x52454449535f4d43`). Connection failures use
`server=unavailable` because no server version can be queried safely.
A synthetic user/password regression drives both adapters through connection
failure and verifies that neither the target nor raw error text reaches the
captured panic message.

## Initial MCP-derived case ledger

| Behavior | Input and isolated setup | Expected semantics | Protocol/server requirement | redis-tower adapter | redis-rs adapter |
|---|---|---|---|---|---|
| Scalar, null, empty, and binary | `GET` missing; binary-safe `SET`/`GET`/`ECHO`; `MGET` over present, missing, and empty values in per-client namespaces | Null stays distinct from empty bytes; arbitrary bytes round-trip without UTF-8 loss | RESP2 and RESP3; Redis 7.4.3 and 8.0.6 per PR | `RedisConnection` + `RawCommand` | async `MultiplexedConnection` + `Cmd` |
| Remaining typed binary families | typed `RENAME`, `EVAL`, `ECHO`, `PUBLISH`, `GEOADD`, `PFADD`, and `BITOP` with invalid UTF-8, NUL, CR/LF, and protocol-looking bytes | Typed serialization and server-visible results agree without decoding opaque arguments | RESP2 and RESP3 | typed command builders | `Cmd` with byte-slice arguments |
| Numeric boundaries and decoding | Store `u64::MAX`, `-1`, and invalid UTF-8 as bulk values | Both decode the maximum as `u64`; both reject negative-to-`u64` and invalid UTF-8-to-`String` conversions | RESP2 and RESP3 | `TypedRawCommand<u64/String>` | `FromRedisValue<u64/String>` |
| Typed builder options | Exercise `Set::nx().get()` against an existing key and `Set::xx().get()` against a missing key; compare the returned old value and a follow-up `GET` | Builder options reach the wire: NX preserves the existing value and XX does not create a missing key | RESP2 and RESP3 | typed `Set` builder | `Cmd` with explicit `NX`/`XX`/`GET` arguments |
| Hashes and unordered sets | Invalid-UTF-8 keys, binary hash value plus multiple fields; set members inserted in a different lexical order | HGETALL compares as sorted pairs; SMEMBERS compares as a sorted collection | RESP2 flat pairs / arrays and RESP3 maps / sets | typed `HSet` / `SAdd`, raw response normalized after decode | `Cmd` request and raw response |
| Lists and sorted sets | Invalid-UTF-8 keys, ordered list including binary bytes; `ZRANGE WITHSCORES` | List order is preserved; zset member/score pairs survive RESP2/RESP3 shape differences | RESP2 and RESP3 | typed `RPush` / `ZAdd`, raw response | `Cmd` request and raw response |
| Streams and nested replies | Invalid-UTF-8 stream key, deterministic typed `XADD 1-0`, then `XRANGE` | Entry IDs, field order, binary values, and nested structure agree | Redis 5+; RESP2 and RESP3 | typed `XAdd`, raw response | `Cmd` request and raw response |
| Server and conversion errors | `GET` against a list; `INCR` at `i64::MAX`; typed range/UTF-8 failures | Server error codes agree (`WRONGTYPE`, `ERR`); client-side conversion failures remain failures without pretending their library-specific categories are identical | RESP2 and RESP3 | `RedisError` classification | `redis::RedisError` classification |
| Raw and administrative shapes | Lua returns null, empty aggregate, status, integer, and binary values; `COMMAND INFO GET` returns administrative nesting | Ordered nested values agree without blanket flattening or lossy conversion | RESP2 and RESP3 | `RawCommand` | `Cmd` |
| Pipeline outcome and alignment | `SET`, failing `HSET`, then `GET`, followed by `PING` | Per-command failure is retained, later reply stays paired with `GET`, and the next command remains aligned | RESP2 and RESP3 | `Pipeline` | `Pipeline::ignore_errors` |
| Transaction commit and abort | Atomic `SET`/`INCR`/`GET`; separate clients mutate a watched key before `EXEC` | Commit results agree; WATCH conflict returns an aborted transaction and does not apply its body | RESP2 and RESP3 | `Transaction` / exclusive connection | atomic `Pipeline` / exclusive connection |
| Blocking command ownership | Seed independent lists, then `BLMOVE` binary data with a finite timeout | Results agree while each blocking operation owns a dedicated, unshared session | Redis 6.2+; RESP2 and RESP3 | dedicated `RedisConnection` | unshared async connection |
| Public topology entry points | Binary `SET`/`GET` through live Cluster and Sentinel fixtures, with an independent redis-rs Cluster connection or direct connection to the Sentinel-discovered master | Routing/discovery does not change the binary result; mutations remain in independent namespaces | Per-PR Redis 7.4.3 and 8.0.6 Cluster/Sentinel fixture legs | `ClusterConnection` / `SentinelConnection` | async Cluster connection / direct async master connection |
| RedisJSON module replies | Independent JSON documents; compare `JSON.GET` and unordered `JSON.OBJKEYS` | JSON bytes agree and object-key order is the only normalization | Path-filtered per-PR and nightly module-enabled Redis 8; RESP2 and RESP3 | `RawCommand` | `Cmd` |
| Queued cancellation | Cancel single and multi requests before the configured batch window closes | Cancelled queue entries never reach the socket | Deterministic in-memory/TCP fixture; no Redis process | `AutoPipelineService` | Not applicable: redis-tower lifecycle contract |
| Lost non-idempotent reply | Fake server records an INCR-like request, applies it once, then closes before replying; replacement socket receives a probe | Caller gets an error, execution remains unknown, and reconnect never silently replays the write | Deterministic TCP fixture; no Redis process | factory-backed `AutoPipelineService` | Not applicable: redis-tower lifecycle contract |

The executable standalone cases live in
[`differential_redis_rs.rs`](../crates/redis-tower/tests/differential_redis_rs.rs).
The RedisJSON case lives in
[`differential_module_replies.rs`](../crates/redis-tower-modules/tests/differential_module_replies.rs)
and is selected by the per-PR module gate and module-enabled nightly job.

## Normalization policy

Normalization is deliberately narrow:

- Sets are sorted because Redis does not promise member order.
- RESP2 flat pair arrays, RESP3 pair arrays, and RESP3 maps become sorted pairs
  only for commands whose documented result is a map or pair collection.
- Status strings and bulk strings preserve their bytes; `OK` is not converted
  through UTF-8 to compare successfully.
- Null and empty values, ordered arrays, integer widths, binary bytes, and
  server error codes are never collapsed together.
- Client-side conversion error taxonomies may differ; the shared contract is
  rejection, while server-originated error codes must agree.

The negative controls prove that the comparator rejects lossy binary
conversion, reversed sorted-set rank order, and a removed `NX` option from the
typed `Set` path. A replay-count control and the live lost-reply fixture fail
if a non-idempotent request executes twice.

## Cancellation and unknown execution

There are three materially different states:

1. A request cancelled while still queued is unsent and has no server effect.
2. A request whose complete reply was received has a known result.
3. A request written before its connection loses the reply has unknown
   execution. Reconnection cannot determine whether Redis applied it.

redis-tower removes cancelled queue entries before flushing and quarantines a
connection whose reply alignment is no longer knowable. It does not replay the
lost non-idempotent request on the replacement socket. Applications that retry
an unknown-execution write must supply their own idempotency mechanism.

## Reproduction

```bash
cargo test -p redis-tower --test differential_redis_rs \
  --all-features -- --test-threads=1

# Requires REDIS_URL pointing at a RedisJSON-capable server.
cargo test -p redis-tower-modules --test differential_module_replies \
  --all-features -- --ignored --test-threads=1
```
