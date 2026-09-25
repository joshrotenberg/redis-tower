# Migrating from redis-rs

This guide maps [`redis-rs` 1.7](https://docs.rs/redis/1.7.0/redis/) to the
current redis-tower API. Both clients provide typed conveniences, raw commands,
multiplexing, pipelines, transactions, Cluster, Sentinel, TLS, pub/sub, and
RESP3. The migration is primarily about choosing explicit connection and
response contracts, not replacing an "untyped" client with a typed one.

## The migration in one table

| redis-rs 1.7 | redis-tower |
|---|---|
| `MultiplexedConnection` | `MultiplexedClient` |
| `ConnectionManager` | Factory-backed `MultiplexedClient`, or serialized `ResilientRedisClient` |
| `AsyncCommands` / `AsyncTypedCommands` methods | Command values such as `Get` and `Set`, passed to `execute` |
| Caller-selected `FromRedisValue` response | Command-defined response type; `RedisValueExt` converts at the edge |
| `Cmd` | `RawCommand`, returning `Frame`, or `TypedRawCommand<T>` |
| `Pipeline` / atomic pipeline | `Pipeline` / `Transaction` |
| Async pub/sub connection | Dedicated `PubSubConnection` |
| Async Cluster connection | `MultiplexedClusterClient` |
| Sentinel client | `MultiplexedSentinelClient` |
| `RedisError` + `ErrorKind` | `RedisError` + classification helpers |

## Dependencies

The versions below are the released APIs used by this guide. Choose only the
redis-tower topology crates the application needs:

```toml
[dependencies]
# Before
redis = { version = "=1.7.0", features = ["tokio-comp", "cluster-async", "connection-manager"] }

# After
redis-tower = "0.1.3"
redis-tower-cluster = "0.1.3"  # only for Redis Cluster
redis-tower-sentinel = "0.1.3" # only for Sentinel
bytes = "1"
tokio-stream = "0.1"           # only for stream APIs such as pub/sub
```

For TLS, enable `tls-rustls` (recommended) or `tls-native-tls` on the facade and
on each topology crate that opens TLS connections; neither backend is enabled
by the facade's default feature set. Redis Stack command groups can be selected
individually or through the facade's default `commands-stack` feature.

The standalone
[registry compatibility harness](https://github.com/joshrotenberg/redis-tower/tree/main/release-tests/redis-rs-migration)
is compiled in CI against these exact crates.io versions. It is an independent
workspace, so local path crates cannot mask a published-API mismatch. The larger
live oracle is documented in [Differential testing](DIFFERENTIAL-TESTING.md).

## Connect and share

```rust,ignore
// redis-rs
let redis_rs = redis::Client::open("redis://127.0.0.1:6379/0")?;
let mut connection = redis_rs.get_multiplexed_async_connection().await?;

// redis-tower
use redis_tower::MultiplexedClient;
let client = MultiplexedClient::connect_url("redis://127.0.0.1:6379/0").await?;
```

Both handles can be cloned and shared across Tokio tasks. Do not add an
`Arc<Mutex<_>>` around `MultiplexedClient`; its worker already serializes and
auto-pipelines concurrent requests.

Choose a different redis-tower connection when the ownership contract differs:

- `RedisConnection` exclusively owns one socket.
- `RedisClient` is a simple serialized shared connection.
- `ConnectionPool<RedisConnection>` owns multiple independent sockets.
- A factory-backed `MultiplexedClient` reconnects without giving up concurrent
  multiplexing.
- `ResilientRedisClient` is a simpler reconnecting, mutex-serialized client.

## Protocol, URLs, and authentication

The defaults differ. A normal redis-rs 1.7 URL selects RESP2; add
`?protocol=resp3` to request RESP3. redis-tower's default
`ProtocolVersion::Auto` tries `HELLO 3` and falls back to RESP2 when RESP3 is not
available. If wire shape matters during a migration, pin both clients instead
of comparing different protocols:

```rust,ignore
// redis-rs RESP2
let redis_rs = redis::Client::open("redis://127.0.0.1:6379/?protocol=resp2")?;

// redis-tower RESP2. The released URL parser does not consume `protocol=`;
// select the protocol with ConnectionConfig.
use redis_tower::{ConnectionConfig, MultiplexedClient, ProtocolVersion};
let config = ConnectionConfig::new().with_protocol(ProtocolVersion::Resp2);
let tower = MultiplexedClient::connect_url_with_connection_config(
    "redis://127.0.0.1:6379/",
    &config,
).await?;

// Use ProtocolVersion::Resp3 to force RESP3 without fallback.
```

`redis://user:password@host/db` and `rediss://...` carry ACL credentials,
database selection, and TLS. Percent-encode URL-special bytes in usernames and
passwords. redis-tower authenticates and selects the database as part of setup;
a reconnecting URL factory repeats those steps on every new socket.

The released redis-tower URL parser accepts Unix sockets with an optional
database query:

```text
unix:///run/redis.sock?db=1
```

The 0.1.3 Unix URL grammar has no authentication or protocol query keys.
`ConnectionConfig` can select the protocol for an unauthenticated Unix socket;
an authenticated Unix/RESP3 setup needs an explicit custom setup sequence.
Do not copy redis-rs Unix query parameters into this released URL unchanged.

For rotating tokens, replace static URL credentials with redis-tower's
`CredentialProvider`; see [Cloud and rotating credentials](CLOUD-AUTH.md).

## Typed commands and response shapes

redis-rs command traits select a conversion from the type requested by the
caller. redis-tower command values have one public response type:

```rust,ignore
// redis-rs
use redis::AsyncCommands;
let _: () = connection.set("key", "value").await?;
let value: Option<String> = connection.get("key").await?;

// redis-tower
use redis_tower::commands::{Get, Set};
client.execute(Set::new("key", "value")).await?;
let value: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
```

String-shaped Redis data stays binary-safe as `Bytes`. Convert only at an
application boundary:

```rust,ignore
use redis_tower::RedisValueExt;
let value: String = client.execute(Get::new("key")).await?.parse_into()?;
```

Do not assume identical public values just because both clients decoded the
same RESP frame. RESP2 may represent a map as a flat array; RESP3 has map, set,
boolean, and verbatim-string types. redis-rs `Value`, redis-tower `Frame`, and a
typed command response deliberately expose different layers. Compare the
documented command result, normalizing only semantics Redis declares unordered
or protocol-dependent.

### Binary arguments

The published 0.1.3 facade depends on `redis-tower-commands` 0.1.2, where many
common typed builders—including `Get` and `Set`—accept UTF-8 strings. Use the
binary-safe `RawCommand::arg` escape hatch for opaque keys and values:

```rust,ignore
use redis_tower::commands::RawCommand;

let key = b"binary:\xff".as_slice();
let payload = vec![0x00, 0xfe, 0xff];
client
    .execute(RawCommand::new("SET").arg(key).arg(payload))
    .await?;
let value: Option<bytes::Bytes> = client
    .execute(RawCommand::new("GET").arg(key).query())
    .await?;
```

Do not force opaque data through UTF-8. The evolving per-family inventory and
ownership rules are tracked in
[Binary data and typed arguments](BINARY-DATA.md); check the rustdoc for the
exact published command version before replacing a raw path with a typed one.

### Raw commands

```rust,ignore
// redis-rs
let value: redis::Value = redis::cmd("MYCOMMAND")
    .arg("key")
    .query_async(&mut connection)
    .await?;

// redis-tower
use redis_tower::commands::RawCommand;
let frame = client
    .execute(RawCommand::new("MYCOMMAND").arg("key"))
    .await?;
```

`RawCommand` returns a `Frame`; validate its shape at the boundary. A typed
builder is preferable when available because it also carries key-routing,
blocking, idempotency, and response-decoding metadata.

## Pipelines and transactions

Concurrent `MultiplexedClient` calls are automatically batched. Use an explicit
`Pipeline` when one task needs an ordered batch on one connection:

```rust,ignore
use redis_tower::{Pipeline, RedisConnection};
use redis_tower::commands::{Get, Incr};

let mut connection = RedisConnection::connect("127.0.0.1:6379").await?;
let mut replies = Pipeline::new()
    .push(Incr::new("counter"))
    .push(Get::new("counter"))
    .execute(&mut connection)
    .await?;
let count: i64 = replies.take(0)?;
let observed: Option<bytes::Bytes> = replies.take(1)?;
```

A pipeline is not atomic. Replace redis-rs's atomic pipeline with
`Transaction`; its result explicitly distinguishes `Committed` from `Aborted`
after a WATCH conflict:

```rust,ignore
use redis_tower::{Transaction, TransactionResult};

match Transaction::new()
    .watch(["counter"])
    .push(Incr::new("counter"))
    .execute(&mut connection)
    .await?
{
    TransactionResult::Committed(mut replies) => {
        let count: i64 = replies.take(0)?;
        # let _ = count;
    }
    TransactionResult::Aborted => { /* rebuild and retry if appropriate */ }
}
```

For read-compute-write WATCH loops, keep the entire loop on a dedicated
`RedisConnection` or an explicitly exclusive pool connection.

## Dedicated and stateful sessions

Do not move every operation onto the default multiplexed client. These APIs
need an exclusive or purpose-built session:

- Run `BLPOP`, blocking `XREAD`, and other blocking commands on a dedicated
  `RedisConnection` or a pool with enough independent connections. One blocking
  request would otherwise stop the shared pipeline worker.
- Construct `PubSubConnection` from a fresh `RedisConnection`; it owns
  subscription state, and `reconnect_with` can restore confirmed subscriptions
  after the application supplies a replacement connection.
- Construct `MonitorStream` from a fresh `RedisConnection`; entering MONITOR
  permanently changes that socket's mode until it is closed.
- Keep manual connection-state sequences and read-compute-write WATCH loops on
  one exclusive connection.

```rust,ignore
use redis_tower::{PubSubConnection, RedisConnection};
use tokio_stream::StreamExt;

let connection = RedisConnection::connect_url("redis://127.0.0.1:6379").await?;
let mut pubsub = PubSubConnection::from_connection(connection)?;
pubsub.subscribe(&["events"]).await?;
while let Some(message) = pubsub.next().await {
    let message = message?;
    # let _ = message;
}
```

## Cluster and Sentinel

```rust,ignore
use redis_tower_cluster::MultiplexedClusterClient;
let cluster = MultiplexedClusterClient::connect_url(
    "rediss://default:secret@seed.example:6379",
).await?;

use redis_tower_sentinel::MultiplexedSentinelClient;
let sentinel = MultiplexedSentinelClient::connect(
    &["127.0.0.1:26379"],
    "mymaster",
).await?;
```

Cluster handles MOVED/ASK routing and topology refresh. Sentinel separates
Sentinel-hop and Redis-node credentials/TLS in its builder and verifies the
discovered role. Cluster pub/sub makes ownership explicit: fixed-node regular
subscriptions and slot-following sharded subscriptions are different APIs.

## Reconnection, retry, and unknown execution

redis-rs `ConnectionManager` reconnects in the background: the command that
observes the dropped connection errors, while later commands wait for the new
connection. redis-tower separates three policies:

1. a connection factory recreates and reconfigures the socket;
2. reconnect backoff controls when another socket is attempted;
3. retry or offline-queue policy decides whether an application command is
   eligible to be sent again.

A write whose request reached Redis but whose reply was lost has unknown
execution. redis-tower does not blindly replay a non-idempotent command such as
`INCR`. Applications that retry such work need an idempotency key or a
transaction/script that makes duplication safe.

## Tower middleware

redis-tower clients implement `tower::Service`, so timeout, tracing, metrics,
circuit-breaker, concurrency, and application-specific policy can compose at a
stable request boundary. The built-in layers are documented on the
[`redis-tower` crate](https://docs.rs/redis-tower). Connection setup timeout,
per-command deadline, Redis's own blocking timeout, and an end-to-end request
deadline are separate controls; preserve that distinction during migration.

## Lessons from an MCP integration

A downstream MCP integration motivated the public
[differential corpus](DIFFERENTIAL-TESTING.md). The reusable lessons are backed
by that repository-local case ledger and executable tests:

- keep redis-rs as an independent oracle instead of sharing redis-tower's
  serializers or response conversion;
- define an application response boundary instead of leaking either client's
  raw protocol enum through a public schema;
- distinguish top-level server errors from nested per-entry errors;
- exercise RESP2 and RESP3 rather than assuming one public response shape;
- test Cluster routing, blocking sessions, binary/null/error values,
  cancellation, and lost replies as separate contracts.

The corpus pins redis-rs 1.7.0 and records the intentionally narrow
normalizations used for each case.
