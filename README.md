# redis-tower

[![Crates.io](https://img.shields.io/crates/v/redis-tower.svg)](https://crates.io/crates/redis-tower)
[![Documentation](https://docs.rs/redis-tower/badge.svg)](https://docs.rs/redis-tower)
[![CI](https://github.com/joshrotenberg/redis-tower/actions/workflows/ci.yml/badge.svg)](https://github.com/joshrotenberg/redis-tower/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/crates/l/redis-tower.svg)](LICENSE)

A Redis client for Rust where every connection is a `tower::Service`.

Commands are typed structs with compile-time response types. Middleware
(timeouts, retries, circuit breaking, caching, metrics) composes via
standard Tower layers. 360+ commands, including Redis Stack modules
behind feature flags.

**Coming from redis-rs?** See the [migration guide](docs/MIGRATING-FROM-REDIS-RS.md)
for side-by-side idiom mappings.

## Quick start

```rust,ignore
use redis_tower::{MultiplexedClient, RedisValueExt, commands::*};

// MultiplexedClient is the recommended default: one auto-pipelined
// connection, cheap to clone and share across tasks.
let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
client.execute(Set::new("key", "hello")).await?;

let val: String = client.execute(Get::new("key")).await?.parse_into()?;
```

## Choosing a client

| Client | When to use |
|--------|-------------|
| `MultiplexedClient` | **The default.** One connection, concurrent commands auto-pipelined; cheap to clone and share across tasks. |
| `RedisConnection` | A single exclusive connection (`&mut self`), or a building block for the others. |
| `RedisClient` | `Arc<Mutex<RedisConnection>>` -- a simple shared handle, but serializes commands through one lock (lower throughput than `MultiplexedClient`; a naive benchmark will under-report it). |
| `ResilientRedisClient` | A shared handle with automatic reconnection + backoff, for long-running services. |
| `ConnectionPool<S>` | N connections -- for blocking commands (`BLPOP`) or CPU-bound reply parsing, where one multiplexed connection would head-of-line block. |
| `MultiplexedClusterClient` | Redis Cluster, high concurrency (`redis-tower-cluster`). |
| `MultiplexedSentinelClient` | Sentinel-managed failover, high concurrency (`redis-tower-sentinel`). |
| `SyncClient` | Blocking (non-`async`) contexts (`redis-tower-sync`). |

## Connection pool

```rust,ignore
use redis_tower::pool::{ConnectionPool, PoolConfig, DispatchStrategy};

let pool = ConnectionPool::connect(4, || async {
    redis_tower::RedisConnection::connect("127.0.0.1:6379").await
}).await?;

// Clone and share across tasks.
let p = pool.clone();
tokio::spawn(async move { p.execute(Ping::new()).await });
```

Dispatch strategies: `RoundRobin` (default), `Random`, `LeastConnections`.

Works with any connection type -- standalone, cluster, or sentinel.

## Tower middleware

```rust,ignore
use tower::ServiceBuilder;
use redis_tower::{FrameService, CommandAdapter, TracingLayer, MetricsLayer};

let svc = CommandAdapter::new(
    ServiceBuilder::new()
        .layer(TracingLayer::new())
        .layer(MetricsLayer::new(my_recorder))
        .service(FrameService::connect("127.0.0.1:6379").await?)
);
```

Built-in layers: `TracingLayer`, `MetricsLayer`, `CacheService`, `ReconnectService`.

Composes with [tower-resilience](https://crates.io/crates/tower-resilience) for
circuit breaking, retry with backoff, rate limiting, and bulkhead isolation.

## Auto-pipelining

```rust,ignore
use redis_tower::{AutoPipelineService, AutoPipelineConfig, CommandAdapter};

let conn = RedisConnection::connect("127.0.0.1:6379").await?;
let mut svc = CommandAdapter::new(
    AutoPipelineService::new(conn, AutoPipelineConfig::default()),
);
// Concurrent calls from multiple tasks are batched into pipelines.
```

## Pipeline and transactions

```rust,ignore
let results = Pipeline::new()
    .push(Set::new("a", "1"))
    .push(Set::new("b", "2"))
    .push(Get::new("a"))
    .execute(&mut conn).await?;

let result = Transaction::new()
    .watch(["key"])
    .push(Incr::new("key"))
    .execute(&mut conn).await?;
```

## Pub/sub

```rust,ignore
let mut pubsub = PubSubConnection::from_connection(conn)?;
pubsub.subscribe(&["events"]).await?;

while let Some(msg) = pubsub.next().await {
    let msg = msg?;
    println!("{}: {:?}", msg.channel, msg.payload);
}
```

## Streams

```rust,ignore
use redis_tower::consumer::{StreamConsumer, ConsumerConfig};

let consumer = StreamConsumer::new("my-group", "worker-1", ["events"])
    .config(ConsumerConfig { batch_size: 20, auto_ack: true, ..Default::default() });

let mut stream = consumer.into_stream(conn);
while let Some(msg) = stream.next().await {
    let msg = msg?;
    println!("{}: {} fields", msg.id, msg.fields.len());
}
```

## Lua scripting

```rust,ignore
let script = Script::new("return redis.call('GET', KEYS[1])");
let result = script.execute(&mut conn, &["mykey"], &[]).await?;
```

`Script` pre-computes the SHA1 and tries EVALSHA first, falling back to
EVAL on NOSCRIPT.

## Client-side caching

```rust,ignore
let mut client = CachedClient::connect("127.0.0.1:6379").await?;
let val = client.execute(Get::new("key")).await?;  // cache miss
let val = client.execute(Get::new("key")).await?;  // cache hit
```

Uses two RESP3 connections with CLIENT TRACKING BCAST for invalidation.
Also available as `CacheService` for Tower layer composition.

## JSON API

Requires the `serde` feature.

```rust,ignore
use redis_tower::Json;

let mut json = Json::new(&mut conn);
json.set("user:1", "$", &User { name: "Alice".into(), age: 30 }).await?;
let user: User = json.get("user:1", "$").await?;
```

## Search API

Requires the `serde` feature.

```rust,ignore
use redis_tower::search_api::{Search, SortDir};

let results = Search::new("idx", "shoes")
    .filter("@price:[0 100]")
    .sort_by("price", SortDir::Asc)
    .limit(0, 10)
    .search::<Product>(&mut conn).await?;
```

## Cluster

Two cluster clients for different workloads:

- **`ClusterConnection`** / **`ClusterClient`** -- simple, mutex-based sharing.
  Good for single-task workloads or when you need connection-level features
  like `MULTI`/`EXEC`.
- **`MultiplexedClusterClient`** -- per-node connections with automatic
  pipelining. Designed for high-concurrency sharing across many tokio tasks
  (~35x higher throughput than `ClusterClient` under load).

```rust,ignore
use redis_tower_cluster::{ClusterConnection, ReadPreference};

// Simple single-connection usage
let mut cluster = ClusterConnection::builder("127.0.0.1:7000")
    .read_preference(ReadPreference::PreferReplica)
    .connect().await?;

cluster.execute(Set::new("{user:1}:name", "Alice")).await?;
```

```rust,ignore
use redis_tower_cluster::MultiplexedClusterClient;

// High-concurrency shared client
let client = MultiplexedClusterClient::builder("127.0.0.1:7000")
    .read_preference(ReadPreference::PreferReplica)
    .connect().await?;

// Clone and share across tasks
let c = client.clone();
tokio::spawn(async move {
    c.execute(Set::new("{user:1}:name", "Alice")).await.unwrap();
});
```

MOVED/ASK redirects handled automatically.

## Sentinel

```rust,ignore
use redis_tower_sentinel::SentinelConnection;

let mut conn = SentinelConnection::connect(
    &["127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"],
    "mymaster",
).await?;
```

Automatic master rediscovery on failover.

## Resilience

`ResilientRedisClient` handles auto-reconnection with exponential backoff:

```rust,ignore
let client = ResilientRedisClient::connect("127.0.0.1:6379").await?;
```

For circuit breaking, retry, and rate limiting, compose with
[tower-resilience](https://crates.io/crates/tower-resilience):

```rust,ignore
use tower_resilience_circuitbreaker::circuit_breaker_builder;

let cb = circuit_breaker_builder()
    .failure_rate_threshold(50.0)
    .wait_duration_in_open(Duration::from_secs(30))
    .build();

let svc = CommandAdapter::new(
    ServiceBuilder::new()
        .layer(cb)
        .service(FrameService::connect("127.0.0.1:6379").await?)
);
```

`RedisError::is_retryable()` classifies which errors are worth retrying.

## Credential provider

```rust,ignore
use redis_tower::credentials::{AuthenticatedConnection, StaticCredentials};

let conn = AuthenticatedConnection::connect(
    "127.0.0.1:6379",
    StaticCredentials::password("secret"),
).await?;
```

Implement `CredentialProvider` for dynamic auth (AWS IAM, Azure Entra ID).
Call `reauthenticate()` on token rotation.

## TLS

```rust,ignore
let conn = RedisConnection::connect_url("rediss://my-redis:6380").await?;
```

A `rediss://` URL uses rustls by default (system roots with a webpki-roots
fallback). For a private CA or mutual TLS (mTLS) -- the standard enterprise
posture -- build a `TlsConfig` from PEM and pass it explicitly:

```rust,ignore
use redis_tower_core::tls::TlsConfig;

let tls = TlsConfig::default_rustls()
    .with_root_ca_pem(std::fs::read("ca.pem")?)                                  // trust a private CA
    .with_client_auth_pem(std::fs::read("client.pem")?, std::fs::read("client.key")?); // present a client cert (mTLS)

// URL provides host/port/AUTH; the TlsConfig drives the handshake:
let conn = RedisConnection::connect_url_with_tls("rediss://default:secret@redis.internal:6379", &tls).await?;

// To keep custom TLS across reconnects, wire it into the factory:
use redis_tower::reconnect::UrlConnectionFactory;
let factory = UrlConnectionFactory::new("rediss://default:secret@redis.internal:6379").with_tls(tls);
```

`with_root_ca_pem` / `with_client_auth_pem` work with both the `native-tls`
and `rustls` backends (selected via feature flags).

## Sync client

`redis-tower-sync` provides a blocking wrapper for scripts and CLI tools:

```rust,ignore
use redis_tower_sync::SyncClient;
use redis_tower_sync::commands::*;

let client = SyncClient::connect("127.0.0.1:6379")?;
client.execute(Set::new("key", "hello"))?;
```

## Feature flags

| Feature | Description |
|---------|-------------|
| `commands-stack` (default) | All Redis Stack module commands |
| `commands-json` | RedisJSON commands |
| `commands-search` | RediSearch commands |
| `commands-bloom` | Bloom and Cuckoo filter commands |
| `commands-sketch` | Count-Min Sketch and Top-K commands |
| `commands-tdigest` | t-digest commands |
| `commands-timeseries` | TimeSeries commands |
| `commands-vector-sets` | Vector Set commands |
| `serde` | JSON and Search high-level APIs |
| `tls-native-tls` | TLS via native-tls |
| `tls-rustls` | TLS via rustls |

## Benchmarks

Cluster throughput at c=128 on a local 3-master cluster (Apple M3 Max):

| Client | SET ops/s | GET ops/s | GET p99 (us) |
|--------|----------:|----------:|-------------:|
| ClusterClient (baseline) | 13,786 | 13,944 | 9,955 |
| redis-rs cluster_async | 448,851 | 448,206 | 537 |
| MultiplexedClusterClient | 502,306 | 522,441 | 383 |

See [`crates/cluster-bench`](crates/cluster-bench/) for full results and
how to reproduce.

## Workspace

```
redis-tower              Facade crate
redis-tower-core         Command trait, RedisConnection, FrameService
redis-tower-protocol     RESP3 codec
redis-tower-commands     360+ typed command structs
redis-tower-cluster      Cluster routing and topology
redis-tower-sentinel     Sentinel discovery and failover
redis-tower-modules      High-level Redis Stack clients (JSON, Search, TimeSeries, probabilistic, Vector)
redis-tower-sync         Blocking wrapper
redis-tower-client       UniversalClient over standalone/cluster/sentinel
```

## License

MIT OR Apache-2.0
