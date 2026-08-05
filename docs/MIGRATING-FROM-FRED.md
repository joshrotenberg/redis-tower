# Migrating from Fred

This guide maps common
[`fred`](https://docs.rs/fred/10.1.0/fred/) 10.1 idioms to redis-tower.
Fred and redis-tower are both asynchronous Redis clients with automatic
pipelining, cluster and Sentinel support, TLS, pub/sub, transactions, and
reconnection building blocks. The important difference is API shape: Fred
exposes command interface traits with caller-selected response conversion,
while redis-tower sends typed command values through clients that implement
Tower `Service`.

The Fred snippets are intentionally versioned to 10.1. Fred has changed its
builder and client names across major releases, so first translate older Fred
code to the equivalent 10.1 concept, then apply the mapping here. The
[Fred crate docs](https://docs.rs/fred/10.1.0/fred/) and
[Fred examples](https://docs.rs/crate/fred/10.1.0/source/examples/README.md) are the
source of truth for Fred-specific behavior.

## The migration in one table

| Fred | redis-tower |
|---|---|
| `Builder` + `Client::init()` | A client `connect` / `connect_url` constructor |
| `Client` for concurrent commands | `MultiplexedClient` |
| Interface methods such as `get` and `set` | Command values such as `Get` and `Set`, passed to `execute` |
| Caller-selected `FromValue` response | Command-defined response type; `RedisValueExt` converts at the edge |
| `client.pipeline()` | Automatic batching on `MultiplexedClient`; `Pipeline` for an explicit batch on an exclusive connection |
| `Pool` / `ExclusivePool` | `ConnectionPool<S>`; use `RedisConnection` when one operation needs exclusive ownership |
| `ReconnectPolicy` on a builder | Factory-backed `MultiplexedClient` or `ResilientRedisClient` with `ReconnectConfig` |
| Offline command buffering during reconnect | Opt-in `OfflineQueueConfig` on `ResilientRedisClient`, for idempotent typed commands only |
| `SubscriberClient` | Dedicated `PubSubConnection` stream |
| `client.multi()` | `Transaction` / `transaction_with_retries` |
| `custom(cmd!(...))` | `RawCommand` |
| `Client::quit()` + connection task | `MultiplexedClient::shutdown()`; dropping an exclusive `RedisConnection` closes it |

## Dependencies and imports

A Fred application often starts with a broad prelude and opts into interface
features:

```toml
[dependencies]
fred = { version = "10", features = ["i-all", "enable-rustls"] }
```

The redis-tower facade always includes the core typed commands. Its default
`commands-stack` feature adds every Redis Stack command group; the example
below disables defaults so Stack groups and TLS can be selected explicitly.
Cluster, Sentinel, sync, modules, and a runtime-selected universal client are
separate crates:

```toml
[dependencies]
redis-tower = { version = "0.1", default-features = false, features = ["tls-rustls"] }
redis-tower-cluster = { version = "0.1", features = ["tls-rustls"] }
redis-tower-sentinel = { version = "0.1", features = ["tls-rustls"] }
bytes = "1"
tokio-stream = "0.1"

# Add only when application code must choose a topology at runtime.
redis-tower-client = "0.1"
```

Add redis-tower features such as `commands-json`, `commands-search`, or
`commands-stack` only when the application uses those Redis Stack command
groups. This keeps compile time and the public command surface intentional.

Instead of importing Fred interface traits through `fred::prelude::*`, import
the command builders you use (or the complete command prelude):

```rust,ignore
use redis_tower::{MultiplexedClient, RedisValueExt};
use redis_tower::commands::{Get, Set};
// For broad command-heavy modules: use redis_tower::commands::*;
```

## Connect and share a client

Fred separates construction from initialization:

```rust,ignore
use fred::prelude::*;

let config = Config::from_url("redis://default:secret@127.0.0.1:6379/0")?;
let client = Builder::from_config(config).build()?;
let connection_task = client.init().await?;
```

redis-tower constructors return a connected, usable client:

```rust,ignore
use redis_tower::MultiplexedClient;

let client = MultiplexedClient::connect_url(
    "redis://default:secret@127.0.0.1:6379/0",
).await?;
```

`MultiplexedClient` is the closest default replacement for Fred's cheaply
cloneable `Client`: clones share one background worker and one auto-pipelined
connection, and `execute(&self, ...)` is safe from many Tokio tasks. Do not put
it behind `Arc<Mutex<_>>`.

There are two lifecycle differences to account for:

1. `MultiplexedClient::connect_url` owns its worker internally; there is no
   separate connection task to retain or await.
2. A plain `connect` client does **not** automatically reconnect. Choose a
   factory-backed client before replacing Fred code that relies on its
   reconnect policy. See [Reconnection](#reconnection).

## Execute typed commands

Fred places each Redis command on an interface trait and converts the response
to the type requested by the caller:

```rust,ignore
use fred::prelude::*;

client.set("key", "value", None, None, false).await?;
let value: Option<String> = client.get("key").await?;
```

In redis-tower, the command type determines the response type:

```rust,ignore
use redis_tower::commands::{Get, Set};

client.execute(Set::new("key", "value")).await?;
let value: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
```

Binary-safe string responses use `bytes::Bytes`. Convert only where the
application requires another representation:

```rust,ignore
use redis_tower::RedisValueExt;

let value: String = client.execute(Get::new("key")).await?.parse_into()?;
```

This moves a class of response-shape mistakes from runtime conversion into the
command's implementation. Collection commands similarly return a concrete
shape such as `Vec<Bytes>`, `Vec<(Bytes, Bytes)>`, or a command-specific
response struct.

### Command options

Fred commonly represents command options as additional method arguments.
redis-tower puts them on a builder, which makes call sites self-describing:

```rust,ignore
client.execute(Set::new("lease", "owner-a").nx().ex(30)).await?;
client.execute(Get::new("lease")).await?;
```

Check the command type's rustdoc when translating a long Fred call: the builder
methods serialize in Redis grammar order even when they are chained in another
order.

### Commands without a typed builder

Fred's `custom` interface maps to `RawCommand`:

```rust,ignore
use redis_tower::commands::RawCommand;

let frame = client
    .execute(RawCommand::new("MYCOMMAND").arg("key").arg("value"))
    .await?;
```

`RawCommand` returns a protocol `Frame`; match and validate the expected shape
at the boundary. Prefer a typed builder when one exists because it also carries
cluster key metadata, blocking behavior, idempotency, and response parsing.

## Automatic and explicit pipelining

Fred automatically pipelines concurrent commands and also exposes an explicit
`client.pipeline()` buffer. `MultiplexedClient` automatically batches commands
that arrive concurrently, so ordinary application code needs no explicit
pipeline:

```rust,ignore
let a = client.execute(Get::new("a"));
let b = client.execute(Get::new("b"));
let (a, b) = tokio::try_join!(a, b)?;
```

For a deliberate ordered batch on one exclusive connection, replace Fred's
explicit pipeline with `Pipeline`:

```rust,ignore
use redis_tower::{Pipeline, RedisConnection};
use redis_tower::commands::{Get, Incr};

let mut connection = RedisConnection::connect("127.0.0.1:6379").await?;
let mut results = Pipeline::new()
    .push(Incr::new("counter"))
    .push(Get::new("counter"))
    .execute(&mut connection)
    .await?;

let incremented: i64 = results.take(0)?;
let observed: Option<bytes::Bytes> = results.take(1)?;
```

An explicit `Pipeline` works with `RedisConnection`, `RedisClient`, and other
`PipelineExecutor` implementations. It is distinct from a transaction: Redis
can execute other clients' commands between entries, and an error in one entry
does not roll back the others.

## Transactions

Replace Fred's `client.multi()` / `exec` block with `Transaction`. Do not send
raw `MULTI` and `EXEC` builders as independent calls through a multiplexed
client; other tasks could interleave commands on the shared connection.

```rust,ignore
use redis_tower::{MultiplexedClient, Transaction, TransactionResult};
use redis_tower::commands::{Get, Incr};

let mut client = MultiplexedClient::connect("127.0.0.1:6379").await?;
match Transaction::new()
    .watch(["counter"])
    .push(Incr::new("counter"))
    .push(Get::new("counter"))
    .execute(&mut client)
    .await?
{
    TransactionResult::Committed(mut results) => {
        let value: i64 = results.take(0)?;
        let observed: Option<bytes::Bytes> = results.take(1)?;
        # let _ = (value, observed);
    }
    TransactionResult::Aborted => {
        // A watched key changed; rebuild this already-known transaction.
    }
}
```

The standard `MultiplexedClient` sends the complete WATCH/MULTI/EXEC sequence
as one contiguous worker request. For read-compute-write logic and a bounded
retry loop, call `transaction_with_retries` on a dedicated `RedisConnection`
or an exclusive connection checked out from an appropriately sized pool.

## Pooling and blocking commands

Fred offers shared `Pool`, exclusive `ExclusivePool`, and optional dynamic
pooling. In redis-tower, first ask whether a pool is needed:

- Keep a single `MultiplexedClient` for high-concurrency, non-blocking command
  traffic. One connection can sustain many concurrent callers through
  automatic pipelining.
- Use `ConnectionPool<RedisConnection>` when commands block (`BLPOP`, blocking
  `XREAD`, and similar), reply parsing is expensive enough to cause
  head-of-line blocking, or the application genuinely needs multiple
  independent connections.
- Use a dedicated `RedisConnection` for pub/sub, MONITOR, or an operation that
  must own its session state.

```rust,ignore
use redis_tower::{ConnectionPool, RedisConnection};
use redis_tower::pool::{DispatchStrategy, PoolConfig};

let pool = ConnectionPool::connect_with_config(
    PoolConfig::default()
        .name("blocking")
        .size(8)
        .dispatch(DispatchStrategy::LeastConnections),
    || RedisConnection::connect("127.0.0.1:6379"),
).await?;
```

Unlike Fred's `ExclusivePool::acquire`, normal `ConnectionPool::execute`
handles reservation and release for one command. Keep session-scoped sequences
on a connection type that explicitly guarantees exclusive ownership.

## Reconnection

A Fred builder can attach a `ReconnectPolicy` to its client. The closest
high-throughput redis-tower replacement is a factory-backed
`MultiplexedClient`. The factory recreates the connection and replays URL
authentication and database selection; the reconnect config controls bounded
exponential backoff and jitter.

```rust,ignore
use std::time::Duration;
use redis_tower::{AutoPipelineConfig, MultiplexedClient};
use redis_tower::auto_pipeline::AutoPipelineReconnectConfig;
use redis_tower::reconnect::{ReconnectConfig, UrlConnectionFactory};

let factory = UrlConnectionFactory::new(
    "redis://default:secret@127.0.0.1:6379/0",
);
let reconnect = ReconnectConfig::default()
    .base_delay(Duration::from_millis(50))
    .max_delay(Duration::from_secs(2))
    .connect_timeout(Duration::from_secs(3));

let client = MultiplexedClient::from_factory(
    factory,
    AutoPipelineConfig::default(),
    AutoPipelineReconnectConfig::new(reconnect),
).await?;
```

`ResilientRedisClient` is a simpler alternative with built-in reconnection,
but it serializes all commands through one mutex. It is appropriate for modest
traffic or control-plane work; prefer the factory-backed multiplexed form for
high concurrency.

Reconnection and command replay are separate decisions. A lost connection can
leave the caller unsure whether Redis executed the command. Opt into
`client.retry(RetryPolicy::default())` only when the typed command is marked
idempotent; redis-tower's retry wrapper refuses to replay non-idempotent writes.

### Queueing idempotent work while offline

The default `ResilientRedisClient` starts a reconnect campaign after a
connection error but does not hold later work for Redis to return. If the Fred
application intentionally buffered commands during outages, opt into a bounded
offline queue:

```rust,ignore
use redis_tower::{OfflineQueueConfig, ResilientRedisClient};

let client = ResilientRedisClient::connect_url_with_offline_queue(
    "redis://default:secret@127.0.0.1:6379/0",
    OfflineQueueConfig::new(256).with_max_replay_attempts(3),
).await?;
```

Every clone shares this queue. It admits only typed commands whose
`Command::idempotent()` implementation returns `true`, preserves queue-ticket
replay order, returns `RedisError::QueueFull` instead of exceeding its
capacity, and returns `RedisError::ReconnectFailed` to admitted work when the
reconnect or per-command replay budget is exhausted. Concurrently first-polled
futures can be ticketed in either scheduler order. Non-idempotent commands and
raw `Service<Frame>` calls fail while offline rather than risk duplicating a
side effect. Canceling an in-wire command quarantines its socket before the
next ticket runs. A capacity of zero is a valid fail-fast configuration.

Use `offline_queue_depth()` and `is_reconnecting()` for overload and readiness
signals. A `tower::buffer::Buffer` in front of `ReconnectService` is an option
for frame-level stacks, but a frame has lost the typed command's idempotency
metadata. The buffer therefore cannot make a safe replay decision for you.

## Cluster and Sentinel

Fred selects centralized, clustered, or Sentinel deployment through its
configuration and returns the same general client type. redis-tower uses
topology-specific crates so their configuration and routing contracts remain
explicit:

```rust,ignore
use redis_tower_cluster::MultiplexedClusterClient;

let cluster = MultiplexedClusterClient::builder("127.0.0.1:7000")
    .max_redirects(5)
    .connect()
    .await?;
```

```rust,ignore
use redis_tower_sentinel::MultiplexedSentinelClient;

let sentinel = MultiplexedSentinelClient::builder(
    &["127.0.0.1:26379", "127.0.0.1:26380"],
    "mymaster",
)
.connect_with_reconnect()
.await?;
```

For application code that must select a topology from configuration at
runtime, use `redis_tower_client::UniversalClient`. Its URL schemes are
`redis://`, `redis+cluster://`, and `redis+sentinel://`; all variants expose the
same typed `execute` method.

Read the [feature matrix](FEATURE-MATRIX.md) before assuming exact parity.
In particular, RESP3 negotiation is currently standalone-only, and some Fred
features such as dynamic pools or alternate async runtimes do not have direct
redis-tower equivalents.

## Pub/sub

Fred's optional `SubscriberClient` manages subscription state and offers
broadcast receivers or callbacks. redis-tower makes the dedicated Redis
connection and message stream explicit:

```rust,ignore
use redis_tower::{PubSubConnection, RedisConnection};
use tokio_stream::StreamExt;

let connection = RedisConnection::connect_url("redis://127.0.0.1:6379").await?;
let mut subscriber = PubSubConnection::from_connection(connection)?;
subscriber.subscribe(&["events"]).await?;

while let Some(message) = subscriber.next().await {
    let message = message?;
    println!("{}: {:?}", message.channel, message.payload);
}
```

`PubSubConnection` tracks confirmed channel, pattern, and shard-channel
subscriptions. After a connection failure, call `reconnect_with(&factory)` to
open a new dedicated connection and replay them. This recovery step is
explicit; do not assume that merely polling the stream reconnects it.

## Errors, health, and middleware

Replace Fred `ErrorKind` matching with redis-tower's classification helpers:

| Intent | redis-tower |
|---|---|
| Connection failed | `error.is_connection_error()` |
| Retry may be useful | `error.is_retryable()` |
| Redis Cluster redirect | `error.is_moved()` / `error.is_ask()` |
| Wrong data type | `error.is_wrongtype()` |
| Pool saturated | match `RedisError::PoolAcquisitionTimeout` |
| Pipeline queue shed load | match `RedisError::QueueFull` |
| Reconnect lifecycle | subscribe to `ConnectionEventBus` before constructing a reconnecting client |

Fred callbacks and client configuration cover much of its operational policy.
With redis-tower, compose the frame service with `TracingLayer`, `MetricsLayer`,
`CommandTimeoutLayer`, and the Redis-aware circuit breaker, or wrap typed
execution with `RetryLayer`. The [production tuning](PRODUCTION-TUNING.md) page
shows how those pieces fit together.

Connection lifecycle streams are bounded observations, not flow control. A
slow subscriber receives `ConnectionEventRecvError::Lagged { skipped }`; event
consumption never delays reconnect or failover. Use events to drive metrics,
logs, and diagnostics, not as a durable audit log or a prerequisite for
recovery.

## Graceful shutdown

Fred applications normally call `quit()` and then await or otherwise account
for the connection task. For a redis-tower multiplexed client, stop producers,
drop their clones, and consume the final owner with `shutdown()`:

```rust,ignore
// Stop accepting application work and wait for spawned tasks first.
client.shutdown().await;
```

If another clone is still alive, `shutdown()` returns without stopping the
shared worker. Treat ownership during shutdown as an application-level
invariant. For pools call `pool.close().await` to reject new work and drain
accepted commands, then drop the remaining clones and connections.

## Migration checklist

1. Inventory Fred features and deployment modes; add only the matching
   redis-tower crates and feature flags.
2. Replace the Fred client with `MultiplexedClient`, unless the workload is
   blocking or needs exclusive session state.
3. Convert interface calls to typed command builders and make `Bytes`-to-domain
   conversions explicit.
4. Decide whether concurrent automatic batching is enough before translating
   every explicit Fred pipeline.
5. Rebuild transactions with `Transaction`, not independent raw MULTI/EXEC
   calls.
6. Choose an explicit reconnect path and confirm AUTH, SELECT, TLS, and
   credentials are replayed by its factory; decide whether offline work should
   fail fast or use the bounded idempotent-only queue.
7. Put blocking commands, pub/sub, and MONITOR on dedicated connections.
8. Add bounded timeouts, queue or pool backpressure, metrics, readiness checks,
   and graceful shutdown before switching production traffic.
9. Run the old and new clients against the same Redis version in staging and
   compare response conversions, latency distributions, error rates, and
   reconnect/failover behavior.
