# Command cookbook

This cookbook connects redis-tower's typed command categories to the client or
session that should execute them. Command structs remain available from
`redis_tower::commands::*`; the same structs are also grouped into browsable
category modules such as `commands::strings`, `commands::streams`, and
`commands::search`.

Use the repository examples when you want a complete runnable program. The
snippets here focus on the contract that is easy to miss when choosing a
command, response type, or connection shape.

The Rust snippets require an external Redis server/topology, so mdBook marks
them `ignore` instead of trying to execute them. Their matching functions live
in [`examples/command_cookbook.rs`](../examples/command_cookbook.rs), which CI
compiles with the advertised workspace crates and features.

## Pick ownership before commands

| Work | Recommended owner | Important constraint |
|---|---|---|
| Concurrent independent requests | `MultiplexedClient` | One shared auto-pipelined socket; do not run blocking or stateful multi-call sequences on it |
| One exclusive sequence | `RedisConnection` | Mutable, single-owner socket; required for WATCH/read/build retries |
| Blocking requests spread across sockets | `ConnectionPool` | Dispatches each command to an independent member, but exposes no checkout/lease API |
| Cluster or Sentinel traffic | Their multiplexed clients | Routing/failover is topology-aware; Cluster multi-key atomic work must stay in one slot |
| Subscription stream | `PubSubConnection` / `BinaryPubSubConnection` | Consumes a dedicated connection and owns subscription state |
| MONITOR stream | `MonitorStream` | Permanently changes its dedicated socket until it is dropped |

See [Production tuning](PRODUCTION-TUNING.md) for queueing, deadlines,
reconnection, and shutdown. See [Cloud and rotating credentials](CLOUD-AUTH.md)
and [Client-side caching](CLIENT-SIDE-CACHING.md) for those specialized
lifecycles.

## Typed commands and response shapes

Command types declare their response at compile time. Nil, empty, integer, and
server-error replies are not interchangeable:

```rust,ignore
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use bytes::Bytes;
use redis_tower::{MultiplexedClient, commands::{Get, Incr, Set}};

let client = MultiplexedClient::connect("127.0.0.1:6379").await?;

let missing_or_value: Option<Bytes> = client.execute(Get::new("profile:1")).await?;
let next: i64 = client.execute(Incr::new("visits")).await?;

// SET always returns Option<Bytes>: without GET, OK is normalized to None;
// with GET, Some(bytes) is the previous value and None means there was none.
let previous: Option<Bytes> = client
    .execute(Set::new("profile:1", "ready").get())
    .await?;
# let _ = (missing_or_value, next, previous);
# Ok(())
# }
```

`Get` distinguishes a missing key (`None`) from an empty value
(`Some(Bytes::new())`). Collection commands similarly preserve their documented
ordering and optional entries. Redis command errors remain `RedisError::Redis`;
they are not converted into an empty success value.

`Set` needs extra care because its options alter the wire reply. The current
typed response maps both ordinary `OK` and a null conditional reply to `None`,
so `Set::new(...).nx()` alone cannot tell an applied write from an unmet
condition. `SET GET` exposes the previous value, but `None` still means there
was no previous value. If the application must distinguish every combination,
use an atomic script with an explicit result shape until the typed API offers a
richer outcome.

That response-design work is tracked in
[#729](https://github.com/joshrotenberg/redis-tower/issues/729); this guide
states the current contract rather than implying the condition is observable.

The [strings](https://docs.rs/redis-tower-commands/latest/redis_tower_commands/strings/),
[hashes](https://docs.rs/redis-tower-commands/latest/redis_tower_commands/hashes/),
[lists](https://docs.rs/redis-tower-commands/latest/redis_tower_commands/lists/),
[sets](https://docs.rs/redis-tower-commands/latest/redis_tower_commands/sets/),
and [sorted-set](https://docs.rs/redis-tower-commands/latest/redis_tower_commands/sorted_sets/)
pages summarize their main result shapes.

## Binary keys and values

Redis keys, fields, members, and stored values are byte strings. Typed builders
accept byte slices, `Vec<u8>`, and `Bytes` through `CommandArg`; replies that
represent Redis byte strings remain `Bytes`.

```rust,ignore
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use bytes::Bytes;
use redis_tower::{MultiplexedClient, commands::{Get, HSet, Set}};

let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
let key = b"user:\xff".as_slice();

client.execute(Set::new(key, b"\x00\xfe\xff".as_slice())).await?;
client.execute(HSet::new(key, b"field".as_slice(), b"value\xff".as_slice())).await?;
let value: Option<Bytes> = client.execute(Get::new(key)).await?;
# let _ = value;
# Ok(())
# }
```

Grammar remains typed or textual: cursors, stream IDs, JSON paths, Search
queries, addresses, and SHA-1 digests are not arbitrary payload bytes. See
[Binary data and typed arguments](BINARY-DATA.md) for ownership and the exact
per-family inventory.

## Cursor iteration

`SCAN` is a cursor protocol, not a one-shot snapshot. Process every returned
page, including pages with zero results, and stop only at cursor `0`. A changing
keyspace can produce duplicates.

```rust,ignore
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use redis_tower::{MultiplexedClient, commands::Scan};

let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
let mut cursor = "0".to_string();
loop {
    let page = client
        .execute(Scan::new().cursor(cursor).match_pattern("user:*").count(100))
        .await?;
    let finished = page.is_finished();
    let next_cursor = page.cursor.clone();
    for key in page.results {
        println!("{key:?}");
    }
    if finished {
        break;
    }
    cursor = next_cursor;
}
# Ok(())
# }
```

On Redis Cluster, a keyless `SCAN` routed normally visits only one node. Use
`redis_tower_cluster::ScanClusterStream` / `ClusterScan` for a cluster-wide
walk and account for resharding semantics documented on that type.

## Pipelines, transactions, and Cluster slots

A `Pipeline` batches network I/O but is not atomic. A `Transaction` submits a
known MULTI/EXEC body atomically. Both keep typed results by index:

```rust,ignore
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use bytes::Bytes;
use redis_tower::{Pipeline, RedisConnection, Transaction, TransactionResult};
use redis_tower::commands::{Get, Incr, Set};

let mut connection = RedisConnection::connect("127.0.0.1:6379").await?;

let pipeline = Pipeline::new()
    .push(Set::new("{account:1}:name", "Ada"))
    .push(Get::new("{account:1}:name"))
    .execute(&mut connection)
    .await?;
let name: &Option<Bytes> = pipeline.get(1)?;

let transaction = Transaction::new()
    .watch(["{account:1}:counter"])
    .push(Incr::new("{account:1}:counter"))
    .execute(&mut connection)
    .await?;
match transaction {
    TransactionResult::Committed(mut replies) => {
        let counter: i64 = replies.take(0)?;
        println!("{counter}");
    }
    TransactionResult::Aborted => { /* rebuild if appropriate */ }
}
# let _ = name;
# Ok(())
# }
```

A direct `Transaction::watch` is safe on supported shared clients because the
already-built WATCH/MULTI/EXEC exchange is submitted as one operation. A
read/compute/write retry loop spans separate calls and must hold a dedicated
`RedisConnection` for the complete WATCH/read/build/EXEC window. A
`ConnectionPool` does not expose a connection lease and cannot provide that
ownership.

Cluster atomic transactions and multi-key commands require every key to hash
to the same slot. Hash tags such as `{account:1}` make that relationship
explicit. A split helper can distribute independent reads, but splitting is
not atomic and is not a substitute for co-located keys.

Run the complete examples with:

```bash
cargo run -p redis-tower-examples --example pipeline
cargo run -p redis-tower-examples --example transaction
```

## Streams, consumer groups, and blocking reads

The application owns acknowledgement. Create a group once, read on an isolated
connection, finish processing, then acknowledge the exact entry ID:

```rust,ignore
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use redis_tower::RedisConnection;
use redis_tower::commands::{XAck, XGroupCreate, XReadGroup};

let mut connection = RedisConnection::connect("127.0.0.1:6379").await?;
connection
    .execute(XGroupCreate::new("events", "workers", "0").mkstream())
    .await?;

let streams = connection
    .execute(XReadGroup::new("workers", "worker-1", "events").count(10).block(1_000))
    .await?;
for (_stream, entries) in streams {
    for entry in entries {
        // Persist the side effect before acknowledging this ID.
        process(&entry).await?;
        connection.execute(XAck::new("events", "workers", entry.id)).await?;
    }
}
# async fn process(_entry: &redis_tower::commands::StreamEntry) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
# Ok(())
# }
```

`XREAD`/`XREADGROUP BLOCK` and list blocking commands hold their socket until a
reply or Redis timeout. Do not put them on a `MultiplexedClient`, where they
would stop the shared worker. Prefer a finite Redis blocking timeout so the
task can observe shutdown; dropping a dedicated connection closes the socket.
A caller timeout after bytes reach Redis cannot prove that no entry was
delivered, so processing and acknowledgement must be idempotent where a lost
reply matters.

See [`examples/streams.rs`](../examples/streams.rs) for a runnable basic stream
flow and [Production tuning](PRODUCTION-TUNING.md#graceful-shutdown) for shutdown
ordering.

## Raw, custom, and module replies

`RawCommand` is the binary-safe escape hatch. It returns `Frame` by default;
`.query::<T>()` applies a `FromFrame` decoder without changing the request:

```rust,ignore
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use redis_tower::{MultiplexedClient, commands::RawCommand};

let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
let count: i64 = client
    .execute(RawCommand::new("SCARD").arg("members").query())
    .await?;
let members: Vec<bytes::Bytes> = client
    .execute(RawCommand::new("SMEMBERS").arg("members").query())
    .await?;
# let _ = (count, members);
# Ok(())
# }
```

Choose the decoder from the documented RESP shape: `Option<T>` for nil,
`Vec<T>` for ordered arrays, tuples/maps only when the protocol shape warrants
them, and raw `Frame` when a module varies by protocol version or options. Do
not normalize nil into empty bytes or unordered results into an invented
ordering.

The Cluster router knows the layouts of supported typed commands. An unknown
custom command uses the legacy first-argument key convention for ordinary
single-command routing; that is safe only when the first argument really is
the complete routing key. Keyless node-local administration should use an
explicit dedicated node connection. Unknown multi-key atomic layouts should
not be guessed.

Feature modules have both Cargo and server prerequisites. Applications using
the `redis-tower` facade need its `commands-*` feature names; applications
depending directly on `redis-tower-commands` use the shorter names:

| `redis-tower` feature | `redis-tower-commands` feature | Public category | Server capability |
|---|---|---|---|
| `commands-json` | `json` | `commands::json` | RedisJSON |
| `commands-search` | `search` | `commands::search` | Redis Search |
| `commands-bloom` | `bloom` | `commands::bloom` | RedisBloom Bloom/Cuckoo |
| `commands-sketch` | `sketch` | `commands::sketch` | RedisBloom CMS/Top-K |
| `commands-tdigest` | `tdigest` | `commands::tdigest` | RedisBloom T-Digest |
| `commands-timeseries` | `timeseries` | `commands::timeseries` | RedisTimeSeries |
| `commands-vector-sets` | `vector-sets` | `commands::vector_sets` | Redis Vector Sets |

The facade's default `commands-stack` feature and the command crate's default
`stack` feature each enable all seven command families. Neither installs those
capabilities on the Redis server.

## Pub/Sub and MONITOR

Publishing is an ordinary typed command. Receiving messages changes the socket
into a stateful session, so construct the session from a fresh connection:

```rust,ignore
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use redis_tower::{BinaryPubSubConnection, RedisConnection};
use tokio_stream::StreamExt;

let connection = RedisConnection::connect("127.0.0.1:6379").await?;
let mut subscriber = BinaryPubSubConnection::from_connection(connection)?;
subscriber.subscribe_bytes(&[b"events\xff".as_slice()]).await?;

if let Some(message) = subscriber.next().await {
    let message = message?;
    println!("{:?}: {:?}", message.channel, message.payload);
}
# Ok(())
# }
```

`reconnect_with` installs a replacement connection and replays subscriptions
that were previously confirmed. Messages published while disconnected or
before Redis processes the replayed subscription are not recovered. Messages
that arrive after server-side subscription processing but before the client
reads the confirmation are buffered and delivered. Cluster regular Pub/Sub is
tied to an explicitly selected node; sharded Pub/Sub follows slot ownership
through its separate API.

`MonitorStream::new` likewise consumes a fresh connection. There is no resume
cursor and no automatic recovery of commands observed during a gap. MONITOR is
an expensive debugging facility, not an application event stream.

Run the complete Pub/Sub example with:

```bash
cargo run -p redis-tower-examples --example pubsub
```

## Standalone, Cluster, Sentinel, and Universal entry points

Choose the topology at construction; command types stay the same:

```rust,ignore
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use redis_tower::MultiplexedClient;
use redis_tower_cluster::MultiplexedClusterClient;
use redis_tower_sentinel::MultiplexedSentinelClient;
use redis_tower_client::UniversalClient;

let standalone = MultiplexedClient::connect_url("redis://127.0.0.1:6379").await?;
let cluster = MultiplexedClusterClient::connect("127.0.0.1:7000").await?;
let sentinel = MultiplexedSentinelClient::connect(
    &["127.0.0.1:26379"],
    "mymaster",
).await?;
let universal = UniversalClient::connect_url("redis://127.0.0.1:6379").await?;
# let _ = (standalone, cluster, sentinel, universal);
# Ok(())
# }
```

Use the explicit builders when Sentinel-hop and data-node credentials differ,
when Cluster TLS/address mapping needs customization, or when read routing and
reconnect policy matter. `UniversalClient` is the convenient application-level
enum; topology-specific APIs remain necessary for cluster-wide scan, dedicated
node sessions, sharded Pub/Sub, and advanced failover controls.

The runnable [`cluster`](../examples/cluster.rs) and
[`sentinel`](../examples/sentinel.rs) examples document their local fixtures.
TLS/authentication and rotating credentials remain canonical in
[Cloud and rotating credentials](CLOUD-AUTH.md).

## Administration and diagnostics

`commands::server`, `commands::acl`, `commands::cluster`, and
`commands::diagnostics` contain typed operator commands. Managed services often
restrict `CONFIG`, `DEBUG`, `MODULE`, ACL mutation, failover, and persistence
commands. Detect permission/server errors rather than assuming local Redis
configuration.

Connection-state commands deserve extra caution. Configure authentication,
RESP version, selected database, tracking, and decode limits through the
connection/factory APIs so every reconnect repeats the same setup. Issuing
`AUTH`, `SELECT`, `HELLO`, or tracking commands ad hoc on one shared socket does
not update its factory's future connections.

For continuous operations, prefer the metrics and lifecycle-event APIs over
polling expensive diagnostic output. [Production tuning](PRODUCTION-TUNING.md)
is the canonical guide for observability, health probes, reconnect state, and
graceful shutdown.

## Feature-gated and versioned families

Cargo feature availability, Redis server availability, and command version are
three separate checks. Building with `stack` proves that the Rust builders are
present; it does not prove a deployment has RedisJSON, Search, Bloom, or
TimeSeries. Native Array commands require Redis 8.8+, and Vector Sets require a
supporting Redis 8 server.

Treat `ERR unknown command` or module-specific capability errors as deployment
configuration/version failures. Gate startup with an explicit capability probe
when a feature is required, and keep fallback behavior visible rather than
silently dropping an option or changing a reply shape.
