# redis-tower

A Tower-native Redis client for Rust with typed commands, composable middleware,
and complete Redis Stack coverage.

## Why redis-tower?

- **Tower middleware composition.** Every connection is a `tower::Service`.
  Timeouts, retries, rate limiting, circuit breaking -- use any Tower layer
  with no adaptation needed. No other Rust Redis client offers this.
- **Typed commands with compile-time safety.** 360+ command structs with
  strongly-typed responses. `Get` returns `Option<Bytes>`, `Incr` returns
  `i64`. Wrong types are caught at compile time.
- **Complete Redis Stack coverage.** Core commands plus feature-gated modules
  for JSON, Search, Bloom, t-digest, TimeSeries, Count-Min Sketch, and
  Vector Sets -- including FCALL and the full scripting API.
- **Auto-pipelining and connection pooling.** Transparent batching of
  concurrent requests, configurable pool with round-robin, random, or
  least-connections dispatch.
- **RESP3 native.** Full RESP3 support with push message infrastructure for
  client-side caching invalidation.

## Quick Start

```rust,ignore
use redis_tower::{RedisClient, commands::*};

let client = RedisClient::connect("127.0.0.1:6379").await?;
client.execute(Set::new("key", "value")).await?;
let val: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
```

## Type-Safe Responses

Command responses are strongly typed. Use `RedisValueExt::parse_into` for
ergonomic conversion to standard Rust types:

```rust,ignore
use redis_tower::{RedisClient, RedisValueExt, commands::*};

let client = RedisClient::connect("127.0.0.1:6379").await?;
client.execute(Set::new("counter", "42")).await?;

let raw: Option<bytes::Bytes> = client.execute(Get::new("counter")).await?;
let count: i64 = raw.parse_into()?;
```

## Connection Pool

`ConnectionPool` manages N connections with configurable dispatch:

```rust,ignore
use redis_tower::{ConnectionPool, DispatchStrategy, PoolConfig};
use redis_tower::commands::*;

let pool = ConnectionPool::connect(4, || async {
    redis_tower::RedisConnection::connect("127.0.0.1:6379").await
}).await?;

// Pool is Clone -- share across tasks.
let p = pool.clone();
tokio::spawn(async move {
    p.execute(Set::new("key", "val")).await.unwrap();
});
```

Dispatch strategies: `RoundRobin` (default), `Random`, `LeastConnections`.

## Tower Middleware

Since `RedisConnection` implements `Service`, compose it with any Tower layer:

```rust,ignore
use tower::ServiceBuilder;
use tower::timeout::TimeoutLayer;
use tower::buffer::BufferLayer;
use redis_tower::{RedisConnection, TracingLayer, MetricsLayer};

let conn = RedisConnection::connect("127.0.0.1:6379").await?;
let svc = ServiceBuilder::new()
    .layer(BufferLayer::new(64))
    .layer(TimeoutLayer::new(std::time::Duration::from_secs(5)))
    .layer(TracingLayer)
    .service(conn);
```

## Auto-Pipelining

`AutoPipelineService` transparently batches concurrent requests into Redis
pipelines for higher throughput:

```rust,ignore
use redis_tower::{AutoPipelineService, AutoPipelineConfig, CommandAdapter, RedisConnection};
use redis_tower::commands::*;
use tower::Service;

let conn = RedisConnection::connect("127.0.0.1:6379").await?;
let mut svc = CommandAdapter::new(
    AutoPipelineService::new(conn, AutoPipelineConfig::default()),
);
let val: Option<bytes::Bytes> = svc.call(Get::new("key")).await?;
```

## Pipeline and Transactions

```rust,ignore
use redis_tower::{Pipeline, Transaction, RedisConnection};
use redis_tower::commands::*;

let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;

// Pipeline: multiple commands in one roundtrip.
let results = Pipeline::new()
    .push(Set::new("a", "1"))
    .push(Set::new("b", "2"))
    .push(Get::new("a"))
    .execute(&mut conn)
    .await?;

// Transaction: atomic MULTI/EXEC with WATCH support.
let result = Transaction::new()
    .watch(["key"])
    .push(Incr::new("key"))
    .execute(&mut conn)
    .await?;
```

## Pub/Sub

```rust,ignore
use redis_tower::PubSubConnection;
use redis_tower::RedisConnection;

let conn = RedisConnection::connect("127.0.0.1:6379").await?;
let mut pubsub = PubSubConnection::from_connection(conn)?;
pubsub.subscribe(&["events"]).await?;

while let Some(msg) = pubsub.next().await {
    let msg = msg?;
    println!("{}: {:?}", msg.channel, msg.payload);
}
```

## Streams

`StreamConsumer` wraps XREADGROUP into a Rust `Stream` with automatic
acknowledgement and consumer group management:

```rust,ignore
use redis_tower::consumer::{StreamConsumer, ConsumerConfig};
use redis_tower::RedisConnection;
use tokio_stream::StreamExt;

let conn = RedisConnection::connect("127.0.0.1:6379").await?;
let consumer = StreamConsumer::new("my-group", "worker-1", ["my-stream"])
    .config(ConsumerConfig {
        batch_size: 20,
        auto_ack: true,
        ..Default::default()
    });

let mut stream = consumer.into_stream(conn);
while let Some(msg) = stream.next().await {
    let msg = msg?;
    println!("{}: {} fields", msg.id, msg.fields.len());
}
```

## Lua Scripting

`Script` pre-computes the SHA1 digest and tries EVALSHA first, falling back
to EVAL on NOSCRIPT:

```rust,ignore
use redis_tower::Script;
use redis_tower::RedisConnection;

let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
let script = Script::new("return redis.call('GET', KEYS[1])");

// EVALSHA with automatic NOSCRIPT fallback.
let result = script.execute(&mut conn, &["mykey"], &[]).await?;
```

## Client-Side Caching

`CachedClient` uses two RESP3 connections -- one for data, one for
CLIENT TRACKING BCAST invalidations:

```rust,ignore
use redis_tower::CachedClient;
use redis_tower::commands::*;

let client = CachedClient::connect("127.0.0.1:6379").await?;
let val = client.execute(Get::new("key")).await?;  // cache miss
let val = client.execute(Get::new("key")).await?;  // cache hit
```

Or use `CacheService` as a composable Tower layer in your service stack.

## JSON API (serde feature)

The `Json` wrapper provides typed get/set with automatic serde
serialization on top of RedisJSON commands:

```rust,ignore
use redis_tower::Json;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct User { name: String, age: u32 }

let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
let mut json = Json::new(&mut conn);

json.set("user:1", "$", &User { name: "Alice".into(), age: 30 }).await?;
let user: User = json.get("user:1", "$").await?;
```

Requires the `serde` feature flag.

## Search API (serde feature)

`Search` provides a query builder with automatic result deserialization
on top of RediSearch:

```rust,ignore
use redis_tower::search_api::{Search, SortDir};
use serde::Deserialize;

#[derive(Deserialize)]
struct Product { name: String, price: String }

let results = Search::new("products_idx", "shoes")
    .filter("@price:[0 100]")
    .sort_by("price", SortDir::Asc)
    .limit(0, 10)
    .search(&mut conn)
    .await?;

for doc in &results.docs {
    println!("{}: {:?}", doc.key, doc.doc);
}
```

Requires the `serde` feature flag.

## Cluster

```rust,ignore
use redis_tower_cluster::{ClusterConnection, ReadPreference};

let mut cluster = ClusterConnection::builder("127.0.0.1:7000")
    .read_preference(ReadPreference::PreferReplica)
    .connect()
    .await?;

// Commands are routed by key slot. MOVED/ASK handled automatically.
cluster.execute(Set::new("{user:1}:name", "Alice")).await?;
```

## Sentinel

```rust,ignore
use redis_tower_sentinel::SentinelConnection;

let mut conn = SentinelConnection::connect(
    &["127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"],
    "mymaster",
).await?;

// Commands go to the current master. Automatic rediscovery on failover.
conn.execute(Set::new("key", "value")).await?;
```

## Resilience

`ResilientRedisClient` provides shared, auto-reconnecting access with
exponential backoff:

```rust,ignore
use redis_tower::ResilientRedisClient;
use redis_tower::commands::*;

let client = ResilientRedisClient::connect("127.0.0.1:6379").await?;

let c = client.clone();
tokio::spawn(async move {
    c.execute(Set::new("key", "value")).await.unwrap();
});
```

### tower-resilience integration

For production-grade fault tolerance, compose with
[tower-resilience](https://crates.io/crates/tower-resilience) -- circuit
breaker, retry with backoff, rate limiting, and bulkhead isolation as
stackable Tower layers:

```rust,ignore
use tower::ServiceBuilder;
use tower_resilience_circuitbreaker::circuit_breaker_builder;
use tower_resilience_retry::RetryLayer;
use redis_tower::{FrameService, CommandAdapter, TracingLayer};

// Circuit breaker: trip at 50% failure rate, 30s recovery window
let cb_layer = circuit_breaker_builder()
    .failure_rate_threshold(50.0)
    .sliding_window_size(10)
    .wait_duration_in_open(Duration::from_secs(30))
    .minimum_number_of_calls(5)
    .build();

// Retry: 3 attempts with exponential backoff, only retry connection errors
let retry_layer = RetryLayer::<Frame, Frame, RedisError>::builder()
    .max_attempts(3)
    .exponential_backoff(Duration::from_millis(100))
    .retry_on(|err: &RedisError| err.is_retryable())
    .build();

// Compose: retry -> circuit breaker -> tracing -> connection
let svc = CommandAdapter::new(
    ServiceBuilder::new()
        .layer(retry_layer)
        .layer(cb_layer)
        .layer(TracingLayer::new())
        .service(FrameService::connect("127.0.0.1:6379").await?)
);
```

The `is_retryable()` method on `RedisError` distinguishes connection errors
(worth retrying) from command errors like WRONGTYPE (not worth retrying).
See `examples/resilience.rs` for the full pattern.

## TLS

```rust,ignore
// Via URL (rediss:// enables TLS):
let conn = RedisConnection::connect_url("rediss://my-redis:6380").await?;

// Programmatic:
let tls = TlsConfig::default_rustls();
let conn = RedisConnection::connect_tls("host:6380", "host", &tls).await?;
```

Supports both `native-tls` and `rustls` backends via feature flags.

## Sync Client

`redis-tower-sync` provides a blocking wrapper for scripts and CLI tools:

```rust,ignore
use redis_tower_sync::SyncClient;
use redis_tower::commands::*;
use redis_tower::RedisValueExt;

let client = SyncClient::connect("127.0.0.1:6379")?;
client.execute(Set::new("key", "hello"))?;
let val: String = client.execute(Get::new("key"))?.parse_into()?;
```

## Feature Flags

| Feature | Description |
|---------|-------------|
| `commands-stack` (default) | All Redis Stack module commands |
| `commands-json` | RedisJSON commands (`JSON.GET`, `JSON.SET`, etc.) |
| `commands-search` | RediSearch commands (`FT.SEARCH`, `FT.CREATE`, etc.) |
| `commands-bloom` | Bloom filter commands (`BF.ADD`, `BF.EXISTS`, etc.) |
| `commands-sketch` | Count-Min Sketch commands (`CMS.INCRBY`, etc.) |
| `commands-tdigest` | t-digest commands (`TDIGEST.ADD`, etc.) |
| `commands-timeseries` | TimeSeries commands (`TS.ADD`, `TS.RANGE`, etc.) |
| `commands-vector-sets` | Vector Set commands (`VADD`, `VSIM`, etc.) |
| `serde` | `Json` and `Search` high-level APIs with serde |
| `tls-native-tls` | TLS via native-tls backend |
| `tls-rustls` | TLS via rustls backend |

## Architecture

```
redis-tower                  Facade crate (what users depend on)
redis-tower-core             Command trait, RedisConnection, FrameService
redis-tower-protocol         RESP3 codec (backed by resp-rs)
redis-tower-commands         360+ typed command implementations
redis-tower-cluster          Cluster routing, MOVED/ASK, read preference
redis-tower-sentinel         Sentinel discovery and failover
redis-tower-sync             Blocking wrapper with internal tokio runtime
```

Service stack:

```
CommandAdapter          Cmd -> Frame, Frame -> Response
    |
CacheService            Frame caching + invalidation
    |
ReconnectService        Auto-reconnection via poll_ready
    |
FrameService            Raw RESP frame I/O
```

All connection types (standalone, cluster, sentinel) implement
`tower::Service`, making them interchangeable with Tower middleware.

## License

MIT OR Apache-2.0
