# redis-tower

A Tower-native Redis client for Rust. Every connection is a `tower::Service`.
Commands are typed request/response pairs. Resilience, caching, and middleware
are composed via standard Tower layers.

## Why redis-tower?

- **Tower all the way down.** Connections implement `Service<Cmd>`. Timeout,
  retry, rate limiting, circuit breaking -- use any Tower middleware, no
  adaptation needed.
- **Typed commands.** `Get` returns `Option<Bytes>`, `Incr` returns `i64`.
  Wrong types are caught at compile time, not runtime.
- **Composable middleware.** Client-side caching, reconnection, and metrics
  are Tower layers that compose with each other and the ecosystem.
- **RESP3 native.** Full RESP3 support with push message infrastructure for
  client-side caching invalidation and future server-push features.

## Quick Start

```rust
use redis_tower::{RedisClient, commands::*};

let client = RedisClient::connect("127.0.0.1:6379").await?;
client.execute(Set::new("key", "value")).await?;
let val: Option<Bytes> = client.execute(Get::new("key")).await?;
```

## Tower Middleware Composition

```rust
use redis_tower::{FrameService, CommandAdapter, CacheService, ReconnectService};
use redis_tower::cache_layer::CacheConfig;
use redis_tower::reconnect::{AddrConnectionFactory, ReconnectConfig};

// Build a composable service stack:
// Commands -> Cache -> Reconnection -> Raw I/O
let svc = CommandAdapter::new(
    CacheService::new(
        ReconnectService::new(
            AddrConnectionFactory::new("127.0.0.1:6379"),
            ReconnectConfig::default(),
        ).await?,
        CacheConfig::default(),
    )
);

// Use like any Tower service.
let val: Option<Bytes> = svc.call(Get::new("key")).await?;
```

## Commands

50 typed commands across 7 categories:

| Category | Commands |
|----------|----------|
| Strings | GET, SET, INCR, MGET, MSET, APPEND |
| Keys | DEL, EXISTS, EXPIRE, TTL, RENAME, TYPE |
| Hashes | HGET, HSET, HDEL, HEXISTS, HGETALL, HINCRBY, HKEYS, HVALS, HLEN |
| Lists | LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET, LMOVE |
| Sets | SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SINTER |
| Sorted Sets | ZADD, ZREM, ZRANGE, ZSCORE, ZCARD, ZINCRBY, ZRANK, ZRANGEBYSCORE |
| Server | PING, FLUSHDB, DBSIZE, SELECT, AUTH, CLIENT TRACKING |

Builder patterns for complex commands:

```rust
Set::new("key", "value").ex(3600).nx()
ZAdd::new("leaderboard").member(100.0, "alice").member(200.0, "bob")
ClientTracking::on().bcast().prefix("user:")
```

## Pipeline and Transactions

```rust
// Pipeline: multiple commands in one roundtrip.
let results = Pipeline::new()
    .push(Set::new("a", "1"))
    .push(Set::new("b", "2"))
    .push(Get::new("a"))
    .execute(&conn)
    .await?;

// Transaction: atomic MULTI/EXEC with WATCH support.
let result = Transaction::new()
    .watch(["key"])
    .push(Incr::new("key"))
    .execute(&conn)
    .await?;
```

## Pub/Sub

```rust
let mut pubsub = PubSubConnection::from_connection(conn)?;
pubsub.subscribe(&["events"]).await?;

while let Some(msg) = pubsub.next().await {
    let msg = msg?;
    println!("{}: {:?}", msg.channel, msg.payload);
}
```

## Client-Side Caching

Two options: simple wrapper or composable Tower layer.

```rust
// Simple: CachedClient with automatic invalidation.
let client = CachedClient::connect("127.0.0.1:6379").await?;
let val = client.execute(Get::new("key")).await?; // cache miss
let val = client.execute(Get::new("key")).await?; // cache hit

// Tower: CacheService composes with other middleware.
let svc = CommandAdapter::new(CacheService::new(frame_svc, config));
```

## Cluster

```rust
use redis_tower_cluster::{ClusterConnection, ReadPreference};

let mut cluster = ClusterConnection::builder("127.0.0.1:7000")
    .read_preference(ReadPreference::PreferReplica)
    .connect()
    .await?;

// Commands are routed to the correct node by key slot.
// MOVED/ASK redirects are handled automatically.
cluster.execute(Set::new("{user:1}:name", "Alice")).await?;
```

## Sentinel

```rust
use redis_tower_sentinel::SentinelConnection;

let mut conn = SentinelConnection::connect(
    &["127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"],
    "mymaster",
).await?;

// Commands go to the current master.
// Automatic rediscovery on failover.
conn.execute(Set::new("key", "value")).await?;
```

## Resilience

```rust
use redis_tower::{ResilientRedisClient, reconnect::ReconnectConfig};

// Auto-reconnecting shared client.
let client = ResilientRedisClient::connect("127.0.0.1:6379").await?;

// Or compose reconnection as a Tower layer:
let svc = ReconnectService::new(
    AddrConnectionFactory::new("127.0.0.1:6379"),
    ReconnectConfig::default().max_retries(5),
).await?;
```

## TLS

```rust
// Via URL:
let conn = RedisConnection::connect_url("rediss://my-redis:6380").await?;

// Programmatic:
let tls = TlsConfig::default_rustls();
let conn = RedisConnection::connect_tls("host:6380", "host", &tls).await?;
```

Supports both `native-tls` and `rustls` backends via feature flags.

## RESP3

```rust
// Negotiate RESP3 for push messages and native types.
let conn = RedisConnection::connect_resp3("127.0.0.1:6379").await?;

// Subscribe to push messages (for client-side caching, etc.)
let mut pushes = conn.subscribe_pushes().await;
```

## Architecture

```
redis-tower                  Facade crate (what users depend on)
redis-tower-core             Command trait, RedisConnection, FrameService
redis-tower-protocol         RESP codec (backed by resp-rs)
redis-tower-commands         50 typed command implementations
redis-tower-cluster          Cluster routing, MOVED/ASK, read preference
redis-tower-sentinel         Sentinel discovery and failover
```

## Service Stack

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
`tower::Service<Cmd>`, making them interchangeable with Tower middleware.

## License

MIT OR Apache-2.0
