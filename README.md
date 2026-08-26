# redis-tower

[![Crates.io](https://img.shields.io/crates/v/redis-tower.svg)](https://crates.io/crates/redis-tower)
[![Documentation](https://docs.rs/redis-tower/badge.svg)](https://docs.rs/redis-tower)
[![CI](https://github.com/joshrotenberg/redis-tower/actions/workflows/ci.yml/badge.svg)](https://github.com/joshrotenberg/redis-tower/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/crates/l/redis-tower.svg)](https://github.com/joshrotenberg/redis-tower#license)

A typed, async Redis client built around Tower services.

- Typed commands and responses for Redis 7.x, Redis 8.x, and Redis Stack.
- A cloneable, auto-pipelined client for normal application traffic.
- Standalone, Cluster, Sentinel, blocking, and topology-neutral clients.
- Tower middleware, reconnection, pooling, client-side caching, metrics, TLS,
  and rotating credentials.

## Install

```bash
cargo add redis-tower
```

The default feature set includes the Redis Stack command builders. A smaller
core-only dependency looks like this:

```toml
[dependencies]
redis-tower = { version = "0.1", default-features = false }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Add the topology or higher-level crates only when you need them:

```bash
cargo add redis-tower-cluster redis-tower-sentinel redis-tower-client
cargo add redis-tower-modules redis-tower-primitives redis-tower-sync
```

The minimum supported Rust version is 1.88.

## Quick start

[`MultiplexedClient`](https://docs.rs/redis-tower/latest/redis_tower/struct.MultiplexedClient.html)
is the recommended default. It is cheap to clone, accepts concurrent commands,
and automatically pipelines work over one connection.

```rust
use redis_tower::{MultiplexedClient, RedisValueExt};
use redis_tower::commands::{Get, Set};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = MultiplexedClient::connect("127.0.0.1:6379").await?;

client.execute(Set::new("greeting", "hello")).await?;
let greeting: String = client
    .execute(Get::new("greeting"))
    .await?
    .parse_into()?;

assert_eq!(greeting, "hello");
# Ok(())
# }
```

Redis URLs support authentication, database selection, Unix sockets, and TLS:

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
# use redis_tower::MultiplexedClient;
let client = MultiplexedClient::connect_url(
    "rediss://default:secret@redis.example.com:6380/0",
).await?;
# let _ = client;
# Ok(())
# }
```

## Choose a client

| Client | Use it for |
|---|---|
| `MultiplexedClient` | Most async applications; cloneable and auto-pipelined |
| `CachedMultiplexedClient` | Read-heavy standalone workloads using RESP3 client-side caching |
| `RedisConnection` | Exclusive access to one connection or custom Tower stacks |
| `RedisClient` | Simple serialized sharing behind a mutex |
| `ResilientRedisClient` | Automatic reconnect with optional bounded offline queuing |
| `ConnectionPool` | Blocking commands, multiple sockets, or CPU-heavy response parsing |
| `MultiplexedClusterClient` | High-concurrency Redis Cluster workloads |
| `MultiplexedSentinelClient` | Sentinel discovery, failover, and replica reads |
| `UniversalClient` | One application type selected by standalone, Cluster, or Sentinel URL |
| `SyncClient` | CLI tools and other blocking code |

The [production tuning guide](https://github.com/joshrotenberg/redis-tower/blob/main/docs/PRODUCTION-TUNING.md)
goes deeper on client selection, backpressure, timeouts, pooling, reconnects,
and shutdown.

## Common operations

Commands are ordinary typed values. Optional arguments use builder methods.

```rust
use redis_tower::commands::{Set, XAdd, ZAdd};

let set = Set::new("session:42", "active").ex(60).nx();
let score = ZAdd::new("leaders").member(100.0, "alice");
let event = XAdd::new("events").field("kind", "created");
# let _ = (set, score, event);
```

Pipelines batch a known group of commands. Transactions use Redis
`WATCH`/`MULTI`/`EXEC` semantics.

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
# use redis_tower::{Pipeline, RedisConnection, Transaction};
# use redis_tower::commands::{Get, Incr, Set};
# let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
let results = Pipeline::new()
    .push(Set::new("a", "1"))
    .push(Incr::new("counter"))
    .push(Get::new("a"))
    .execute(&mut conn)
    .await?;

let transaction = Transaction::new()
    .watch(["counter"])
    .push(Incr::new("counter"))
    .execute(&mut conn)
    .await?;
# let _ = (results, transaction);
# Ok(())
# }
```

## Deployment topologies

### Cluster

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use redis_tower::commands::{Get, Set};
use redis_tower_cluster::{MultiplexedClusterClient, ReadPreference};

let client = MultiplexedClusterClient::builder("127.0.0.1:7000")
    .read_preference(ReadPreference::PreferReplica)
    .connect()
    .await?;

client.execute(Set::new("{user:42}:name", "Ada")).await?;
let name = client.execute(Get::new("{user:42}:name")).await?;
# let _ = name;
# Ok(())
# }
```

The Cluster crate also provides explicit cross-node pipelines, split multi-key
helpers, cluster-wide scans, regular and sharded Pub/Sub, replica routing, and
automatic MOVED/ASK handling.

### Sentinel

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use redis_tower::commands::Ping;
use redis_tower_sentinel::{MultiplexedSentinelClient, ReadPreference};

let client = MultiplexedSentinelClient::builder(
    &["127.0.0.1:26379", "127.0.0.1:26380"],
    "mymaster",
)
.read_preference(ReadPreference::PreferReplica)
.connect()
.await?;

client.execute(Ping::new()).await?;
# Ok(())
# }
```

### One client for every topology

`redis-tower-client` chooses a variant from the URL scheme:

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use redis_tower::commands::Ping;
use redis_tower_client::UniversalClient;

let standalone = UniversalClient::connect_url("redis://127.0.0.1:6379").await?;
let cluster = UniversalClient::connect_url("redis+cluster://127.0.0.1:7000").await?;
let sentinel = UniversalClient::connect_url(
    "redis+sentinel://127.0.0.1:26379/mymaster",
).await?;

standalone.execute(Ping::new()).await?;
# let _ = (cluster, sentinel);
# Ok(())
# }
```

Use the `rediss`, `rediss+cluster`, and `rediss+sentinel` variants for TLS.

## Data access modules

The `redis-tower-modules` crate provides higher-level clients over the lower-
level typed command builders.

| Module | Client | Typical use |
|---|---|---|
| RedisJSON | `json::JsonClient` | Serialize application types with Serde |
| RediSearch | `search::SearchClient` | Define indexes and deserialize query results |
| RedisTimeSeries | `timeseries::TimeSeriesClient` | Store and query timestamped samples |
| Probabilistic | `probabilistic::*` | Bloom, Cuckoo, CMS, TopK, and T-Digest |
| Vector Sets | `vector::VectorSetClient` | Add vectors and run similarity search |

### JSON

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use redis_tower::MultiplexedClient;
use redis_tower_modules::json::JsonClient;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct User { name: String }

let client = MultiplexedClient::connect("127.0.0.1:6379").await?;
let mut json = JsonClient::new(client);
json.set("user:42", "$", &User { name: "Ada".into() }).await?;
let user: Option<User> = json.get("user:42", "$").await?;
# let _ = user;
# Ok(())
# }
```

### Search, time series, probabilistic structures, and vectors

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use redis_tower::RedisConnection;
use redis_tower_modules::probabilistic::BloomFilter;
use redis_tower_modules::search::{IndexBuilder, SearchClient};
use redis_tower_modules::timeseries::{TimeSeriesClient, TsKeyConfig, TsTimestamp};
use redis_tower_modules::vector::VectorSetClient;

let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;

SearchClient::new(&mut conn)
    .create_index(IndexBuilder::new("users").on_hash().text_field("name"))
    .await?;

TimeSeriesClient::new(&mut conn)
    .create("temperature", TsKeyConfig::new().label("room", "lab"))
    .await?;
TimeSeriesClient::new(&mut conn)
    .add("temperature", TsTimestamp::Auto, 21.5)
    .await?;

{
    let mut bloom = BloomFilter::new(&mut conn, "seen-users");
    bloom.add("user:42").await?;
}

let mut vectors = VectorSetClient::new(&mut conn, "embeddings");
vectors.add(vec![1.0, 0.0, 0.0], "user:42").await?;
# Ok(())
# }
```

## Pub/Sub, streams, and caching

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
# use redis_tower::RedisConnection;
let conn = RedisConnection::connect("127.0.0.1:6379").await?;
let mut pubsub = redis_tower::PubSubConnection::from_connection(conn)?;
pubsub.subscribe(&["events"]).await?;

while let Some(message) = pubsub.next().await {
    println!("{:?}", message?);
}
# Ok(())
# }
```

For durable processing, use Redis Streams through the typed `X*` commands or
[`StreamConsumer`](https://docs.rs/redis-tower/latest/redis_tower/struct.StreamConsumer.html).

RESP3 client-side caching is available as a managed client:

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use std::time::Duration;
use redis_tower::{CachedClientConfig, CachedMultiplexedClient};
use redis_tower::commands::Get;

let config = CachedClientConfig::new()
    .max_entries(10_000)
    .client_ttl(Some(Duration::from_secs(30)));
let client = CachedMultiplexedClient::connect_with_config(
    "127.0.0.1:6379",
    config,
).await?;

let value = client.execute(Get::new("user:42")).await?;
# let _ = value;
# Ok(())
# }
```

Read the [client-side caching guide](https://github.com/joshrotenberg/redis-tower/blob/main/docs/CLIENT-SIDE-CACHING.md)
for invalidation, failure, and consistency semantics.

## Tower middleware

`FrameService` is a `tower::Service<Frame>`. `CommandAdapter` converts a frame
stack back into a typed command service.

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use redis_tower::{CommandAdapter, FrameService, TracingLayer};
use tower::ServiceBuilder;

let service = ServiceBuilder::new()
    .layer(TracingLayer::new())
    .service(FrameService::connect("127.0.0.1:6379").await?);
let typed = CommandAdapter::new(service);
# let _ = typed;
# Ok(())
# }
```

The facade also includes request deadlines, Redis-aware retries and circuit
breaking, reconnection, cache middleware, and metrics-facade integration.

## Distributed primitives

`redis-tower-primitives` builds coordination tools on the same typed executor:

- Fenced distributed locks and leader election.
- Expirable semaphores and countdown latches.
- Delayed queues and block-allocated IDs.
- Redis-time GCRA rate limiting.

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use std::time::Duration;
use redis_tower::MultiplexedClient;
use redis_tower_primitives::DistributedLock;

let mut client = MultiplexedClient::connect("127.0.0.1:6379").await?;
let lock = DistributedLock::new(
    "{invoice:42}:lock",
    "{invoice:42}:fence",
    Duration::from_secs(10),
)?;

if let Some(lease) = lock.acquire(&mut client).await? {
    let fencing_token = lease.fencing_token();
    // Pass fencing_token to the guarded resource and reject stale writers.
    lease.release(&mut client).await?;
}
# Ok(())
# }
```

These are Redis-backed leases, not consensus primitives. Read the
[distributed primitives guide](https://github.com/joshrotenberg/redis-tower/blob/main/docs/PRIMITIVES.md)
before using them for correctness-sensitive work.

## Authentication, TLS, and blocking code

Static credentials can be supplied through URLs or builders. Dynamic
credentials implement `CredentialProvider`; companion crates provide AWS
ElastiCache IAM and Microsoft Entra ID integrations:

- `redis-tower-auth-aws`
- `redis-tower-auth-azure`

The [cloud authentication guide](https://github.com/joshrotenberg/redis-tower/blob/main/docs/CLOUD-AUTH.md)
covers reconnect-time refresh, proactive reauthentication, TLS, and topology-
specific configuration.

Blocking applications can use `redis-tower-sync`:

```rust
use redis_tower_sync::SyncClient;
use redis_tower_sync::commands::{Get, Set};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = SyncClient::connect("127.0.0.1:6379")?;
client.execute(Set::new("key", "value"))?;
let value = client.execute(Get::new("key"))?;
# let _ = value;
# Ok(())
# }
```

## Workspace crates

| Crate | Purpose |
|---|---|
| `redis-tower` | Main client facade, middleware, pooling, caching, and resilience |
| `redis-tower-core` | Connection, command trait, Tower frame service, URL/TLS, conversions |
| `redis-tower-protocol` | RESP3 frames and bounded Tokio codec |
| `redis-tower-commands` | Typed Redis and Redis Stack command builders |
| `redis-tower-cluster` | Cluster topology, routing, redirects, scans, pipelines, and Pub/Sub |
| `redis-tower-sentinel` | Sentinel discovery, failover, and replica reads |
| `redis-tower-client` | Topology-neutral `UniversalClient` |
| `redis-tower-modules` | High-level JSON, Search, TimeSeries, probabilistic, and vector clients |
| `redis-tower-primitives` | Distributed coordination primitives |
| `redis-tower-sync` | Blocking wrapper |
| `redis-tower-auth-aws` | ElastiCache IAM credentials |
| `redis-tower-auth-azure` | Microsoft Entra ID credentials |
| `redis-tower-test` | Mocks and managed Redis test fixtures |

## Learn more

- [Documentation home](https://joshrotenberg.com/redis-tower/)
- [Production tuning](https://github.com/joshrotenberg/redis-tower/blob/main/docs/PRODUCTION-TUNING.md)
- [Serverless and scale-to-zero](https://github.com/joshrotenberg/redis-tower/blob/main/docs/SERVERLESS.md)
- [Client-side caching](https://github.com/joshrotenberg/redis-tower/blob/main/docs/CLIENT-SIDE-CACHING.md)
- [Cloud and rotating credentials](https://github.com/joshrotenberg/redis-tower/blob/main/docs/CLOUD-AUTH.md)
- [Distributed primitives](https://github.com/joshrotenberg/redis-tower/blob/main/docs/PRIMITIVES.md)
- [Migrating from redis-rs](https://github.com/joshrotenberg/redis-tower/blob/main/docs/MIGRATING-FROM-REDIS-RS.md)
- [Migrating from Fred](https://github.com/joshrotenberg/redis-tower/blob/main/docs/MIGRATING-FROM-FRED.md)
- [Feature matrix](https://github.com/joshrotenberg/redis-tower/blob/main/docs/FEATURE-MATRIX.md)
- [Test conformance](https://github.com/joshrotenberg/redis-tower/blob/main/docs/TEST-CONFORMANCE.md)

Runnable programs live in the
[`examples`](https://github.com/joshrotenberg/redis-tower/tree/main/examples)
directory.

## Compatibility

Redis 7.x and 8.x are the supported server lines. CI exercises Redis 7.4 and
8.0 on every change, with a broader nightly Redis and Valkey matrix. Redis
Stack command groups are feature-gated and return normal server errors when a
module is unavailable.

redis-tower is pre-1.0. A `0.x` minor release may contain breaking changes;
those changes are called out in crate changelogs.

## Security

See [SECURITY.md](https://github.com/joshrotenberg/redis-tower/blob/main/SECURITY.md)
for the vulnerability disclosure policy. The workspace forbids unsafe code and
runs dependency policy and advisory checks in CI.

## Contributing

See [CONTRIBUTING.md](https://github.com/joshrotenberg/redis-tower/blob/main/CONTRIBUTING.md)
for development setup and validation commands.

## License

Licensed under either the
[MIT License](https://github.com/joshrotenberg/redis-tower/blob/main/LICENSE-MIT)
or the
[Apache License, Version 2.0](https://github.com/joshrotenberg/redis-tower/blob/main/LICENSE-APACHE),
at your option.
