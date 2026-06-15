# Migrating from redis-rs

This guide maps common [`redis-rs`](https://github.com/redis-rs/redis-rs) idioms
to their `redis-tower` equivalents. The two crates share the same mental model
(open a connection, run commands, pipeline, pub/sub, cluster, sentinel), so most
migrations are mechanical.

The headline differences:

- **Typed command builders instead of a `Cmd`/`AsyncCommands` split.** Every
  command is a value (`Get::new("k")`) you pass to `execute`, with a typed
  response. There is no `AsyncCommands` trait to import and no `query_async`.
- **Tower-native.** Clients are `Service`s, so middleware (tracing, metrics,
  circuit breaking, timeouts) composes as Tower layers. redis-rs has no
  equivalent layering point.
- **Bytes, not stringly-typed.** String-shaped responses come back as
  `bytes::Bytes` by default; convert at the edge when you want `String`.

## Cargo.toml

```toml
# redis-rs
redis = { version = "0.27", features = ["tokio-comp", "cluster-async"] }

# redis-tower
redis-tower = "0.1"
# Cluster, sentinel, sync, and modules live in sibling crates:
redis-tower-cluster = "0.1"
redis-tower-sentinel = "0.1"
```

TLS is a feature flag on either backend: `tls-rustls` (recommended; pure-Rust,
aws-lc-rs) or `tls-native-tls`.

## Connecting

```rust
// redis-rs (async, multiplexed)
let client = redis::Client::open("redis://127.0.0.1/")?;
let mut con = client.get_multiplexed_async_connection().await?;

// redis-tower
use redis_tower::MultiplexedClient;
let client = MultiplexedClient::connect_url("redis://127.0.0.1").await?;
// or, from a host:port: MultiplexedClient::connect("127.0.0.1:6379").await?
```

`MultiplexedClient` is `Clone` and shares one auto-pipelined connection across
tasks, like redis-rs's `MultiplexedConnection`. For a single exclusive
connection use `RedisConnection`; for an `Arc<Mutex<_>>` shareable handle use
`RedisClient`.

## Running commands

```rust
// redis-rs
use redis::AsyncCommands;
con.set("key", "value").await?;
let v: Option<String> = con.get("key").await?;

// redis-tower
use redis_tower::commands::{Get, Set};
client.execute(Set::new("key", "value")).await?;
let v: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
```

Each command's response type is its natural Redis shape (`Get` -> `Option<Bytes>`,
`Incr` -> `i64`, ...). To get redis-rs-style "parse into whatever type I asked
for," bring in `RedisValueExt` and call `parse_into` -- this replaces the
`FromRedisValue` type inference redis-rs does on `get`:

```rust
use redis_tower::RedisValueExt;
let v: String = client.execute(Get::new("key")).await?.parse_into()?;
let n: i64    = client.execute(Get::new("counter")).await?.parse_into()?;
```

Command options that are method chains in redis-rs are builder methods here:

```rust
// redis-rs:  con.set_options("k", "v", SetOptions::default().with_expiration(EX(10)))
// redis-tower:
client.execute(Set::new("k", "v").ex(10)).await?;   // SET k v EX 10
client.execute(Set::new("k", "v").nx()).await?;     // SET k v NX -> bool
```

### Arbitrary / not-yet-typed commands

```rust
// redis-rs
redis::cmd("SET").arg("key").arg("value").query_async(&mut con).await?;

// redis-tower
use redis_tower::commands::RawCommand;
let reply = client.execute(RawCommand::new("SET").arg("key").arg("value")).await?;
// `reply` is a raw `Frame`; match on it for the value you expect.
```

## Pipelining

```rust
// redis-rs
let (a, b): (i64, i64) = redis::pipe()
    .cmd("INCR").arg("k")
    .cmd("GET").arg("k")
    .query_async(&mut con).await?;

// redis-tower
use redis_tower::Pipeline;
use redis_tower::commands::{Get, Incr};
let mut results = Pipeline::new()
    .push(Incr::new("k"))
    .push(Get::new("k"))
    .execute(&mut conn)
    .await?;
let a: i64 = results.take(0)?;
let b: Option<bytes::Bytes> = results.take(1)?;
```

Note: with `MultiplexedClient`, concurrent commands from different tasks are
already batched into pipelines automatically -- you only need an explicit
`Pipeline` when one task wants to send a batch atomically on the wire.

## Transactions (MULTI/EXEC, WATCH)

```rust
// redis-rs
let (n,): (i64,) = redis::pipe().atomic()
    .cmd("INCR").arg("k")
    .query_async(&mut con).await?;

// redis-tower
use redis_tower::{Transaction, TransactionResult};
use redis_tower::commands::Incr;
match Transaction::new().push(Incr::new("k")).execute(&mut conn).await? {
    TransactionResult::Committed(results) => { let n: &i64 = results.get(0)?; }
    TransactionResult::Aborted => { /* a WATCHed key changed -- rebuild and retry */ }
}
```

For optimistic locking, `WATCH` keys with `Transaction::new().watch(["k"])...`; the
result is `Aborted` if a watched key changed before `EXEC`.

## Pub/Sub

```rust
// redis-rs
let mut pubsub = client.get_async_pubsub().await?;
pubsub.subscribe("channel").await?;
let mut stream = pubsub.on_message();
while let Some(msg) = stream.next().await { /* ... */ }

// redis-tower
use redis_tower::{PubSubConnection, RedisConnection};
use tokio_stream::StreamExt;
let conn = RedisConnection::connect_url("redis://127.0.0.1").await?;
let mut pubsub = PubSubConnection::from_connection(conn)?;
pubsub.subscribe(&["channel"]).await?;
while let Some(msg) = pubsub.next().await {
    let msg = msg?; // PubSubMessage { channel, payload, .. }
}
```

redis-tower tracks active subscriptions, so after a connection drop you can
`pubsub.reconnect_with(&factory).await?` to restore them -- the subscriptions
survive the reconnect instead of silently going quiet.

## Cluster

```rust
// redis-rs
let client = redis::cluster::ClusterClient::new(vec!["redis://127.0.0.1:6379/"])?;
let mut con = client.get_async_connection().await?;

// redis-tower
use redis_tower_cluster::MultiplexedClusterClient;
let client = MultiplexedClusterClient::connect("127.0.0.1:7000").await?;
// or with auth/TLS from a URL:
let client = MultiplexedClusterClient::connect_url("rediss://default:pw@host:7000").await?;
```

MOVED/ASK redirects, single-slot topology patching, TRYAGAIN/CLUSTERDOWN/LOADING
retries, and failover self-healing (dead-node replacement + pruning) are handled
automatically.

## Sentinel

```rust
// redis-rs
let mut sentinel = redis::sentinel::SentinelClient::build(
    vec!["redis://127.0.0.1:26379/"], "mymaster".into(),
    Some(redis::sentinel::SentinelNodeConnectionInfo::default()),
    redis::sentinel::SentinelServerType::Master,
)?;

// redis-tower
use redis_tower_sentinel::MultiplexedSentinelClient;
let client = MultiplexedSentinelClient::connect(&["127.0.0.1:26379"], "mymaster").await?;
// with separate sentinel-hop and node-hop credentials / TLS:
let client = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
    .sentinel_credentials(StaticCredentials::password("sentinel_pw"))
    .node_credentials(StaticCredentials::password("redis_pw"))
    .connect_with_reconnect()
    .await?;
```

The client verifies `ROLE` after discovery and re-authenticates across failover.

## TLS, custom CA, and mTLS

```rust
// redis-rs: rediss:// + the tls features
let client = redis::Client::open("rediss://host:6380/")?;

// redis-tower: rediss:// uses rustls by default
let conn = RedisConnection::connect_url("rediss://host:6380").await?;

// custom CA / mutual TLS:
use redis_tower_core::tls::TlsConfig;
let tls = TlsConfig::default_rustls()
    .with_root_ca_pem(std::fs::read("ca.pem")?)
    .with_client_auth_pem(std::fs::read("client.pem")?, std::fs::read("client.key")?);
let conn = RedisConnection::connect_url_with_tls("rediss://host:6380", &tls).await?;
```

## Automatic reconnection

```rust
// redis-rs
let mut con = redis::aio::ConnectionManager::new(client).await?;

// redis-tower
use redis_tower::ResilientRedisClient;
let client = ResilientRedisClient::connect("127.0.0.1:6379").await?;
```

`ResilientRedisClient` reconnects with bounded exponential backoff + jitter,
single-flights reconnects across clones, and applies the configured
`connect_timeout`. For finer control, build a `MultiplexedClient` /
`ResilientConnection` from a `ConnectionFactory` (e.g. `UrlConnectionFactory`,
which replays AUTH/SELECT -- and TLS, via `.with_tls()` -- on every reconnect).

## Error handling

`redis_tower_core::RedisError` replaces `redis::RedisError`. Instead of matching
on `ErrorKind`, use the classification helpers:

| redis-rs | redis-tower |
|---|---|
| `e.kind() == ErrorKind::TypeError` (WRONGTYPE) | `e.is_wrongtype()` |
| `e.kind() == ErrorKind::Moved` / `Ask` | `e.is_moved()` / `e.is_ask()` |
| `e.kind() == ErrorKind::TryAgain` | `e.is_tryagain()` |
| `e.kind() == ErrorKind::ClusterDown` | `e.is_clusterdown()` |
| `e.kind() == ErrorKind::ReadOnly` | `e.is_readonly()` |
| `e.is_connection_dropped()` | `e.is_connection_error()` |
| (retry heuristics by hand) | `e.is_retryable()` |

## Middleware (no redis-rs equivalent)

Because every client is a Tower `Service`, you can wrap the frame-level service
in a stack and hand it to `MultiplexedClient::from_layered`:

```rust
use redis_tower::{TracingLayer, MetricsLayer, CircuitBreakerLayer, CommandTimeoutLayer};
```

- `TracingLayer` -- per-command spans with stable OpenTelemetry DB conventions.
- `MetricsLayer` -- latency/error metrics via a `MetricsRecorder` hook.
- `CircuitBreakerLayer` -- three-state breaker, shared across clones.
- `CommandTimeoutLayer` -- per-command deadline.

These compose the same way in front of standalone, cluster, and sentinel
clients.
