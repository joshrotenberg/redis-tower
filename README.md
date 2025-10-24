# redis-tower

A Tower-based Redis client with strong typing, composable middleware, and comprehensive command support.

[![Crates.io](https://img.shields.io/crates/v/redis-tower.svg)](https://crates.io/crates/redis-tower)
[![Documentation](https://docs.rs/redis-tower/badge.svg)](https://docs.rs/redis-tower)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

## Features

- **Tower-native Architecture**: Built on Tower's `Service` trait for composable middleware
- **Type-safe Commands**: 200+ strongly typed commands with compile-time validation
- **Zero-cost Abstractions**: Optional features for cluster, sentinel, modules, and deprecated commands
- **Resilience Built-in**: Ready for circuit breakers, retries, and timeouts via Tower ecosystem
- **High Performance**: Uses efficient RESP2/3 parser with zero-copy parsing
- **Modular Design**: Feature flags for cluster, sentinel, Redis modules, and backwards compatibility

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
redis-tower = "0.1"
tower = "0.5"
tokio = { version = "1.0", features = ["full"] }
```

Basic usage:

```rust
use redis_tower::commands::{Get, Set, Incr};
use tower::ServiceExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to Redis
    let mut client = redis_tower::connect("localhost:6379").await?;

    // Strongly typed commands
    client.call(Set::new("counter", "0")).await?;
    
    let count: i64 = client.call(Incr::new("counter")).await?;
    println!("Counter: {}", count);

    let value: Option<String> = client.call(Get::new("counter")).await?;
    println!("Value: {:?}", value);

    Ok(())
}
```

## Command Support

**200 commands implemented** across all major Redis categories:

- **Strings** (28): GET, SET, INCR, APPEND, LCS, etc.
- **Hashes** (14): HGET, HSET, HINCRBY, HRANDFIELD, etc.
- **Lists** (22): LPUSH, RPOP, LRANGE, LMPOP, BLMOVE, etc.
- **Sets** (17): SADD, SINTER, SUNION, SRANDMEMBER, etc.
- **Sorted Sets** (32): ZADD, ZRANGE, ZMPOP, ZUNIONSTORE, etc.
- **Streams** (14): XADD, XREAD, XREADGROUP, XACK, XPENDING, etc.
- **Geo** (6): GEOADD, GEOSEARCH, GEOSEARCHSTORE, etc.
- **HyperLogLog** (3): PFADD, PFCOUNT, PFMERGE
- **Bitmap** (5): SETBIT, GETBIT, BITCOUNT, BITOP, etc.
- **Keys** (17): DEL, EXPIRE, DUMP, RESTORE, SCAN, etc.
- **Pub/Sub** (3): PUBLISH, PUBSUB NUMSUB, PUBSUB NUMPAT
- **Scripting** (5): EVAL, EVALSHA, SCRIPT LOAD, etc.
- **Server** (9): INFO, DBSIZE, FLUSHDB, SAVE, etc.
- **Connection** (8): AUTH, SELECT, CLIENT SETNAME, etc.

See [COMMANDS_TRACKING.md](COMMANDS_TRACKING.md) for complete command list and coverage details.

## Tower Middleware Integration

Compose resilience layers around your Redis client:

```rust
use tower::ServiceBuilder;
use tower::timeout::TimeoutLayer;
use std::time::Duration;

let client = ServiceBuilder::new()
    .layer(TimeoutLayer::new(Duration::from_secs(5)))
    // Add circuit breaker, retry, rate limiting, etc.
    .service(redis_tower::connect("localhost:6379").await?);
```

When using with `tower-resilience`:

```rust
use tower::ServiceBuilder;
use tower_resilience::{TimeoutLayer, RetryLayer, CircuitBreakerLayer};

let client = ServiceBuilder::new()
    .layer(TimeoutLayer::new(Duration::from_secs(5)))
    .layer(CircuitBreakerLayer::new(5, Duration::from_secs(30)))
    .layer(RetryLayer::new(ExponentialBackoff::default()))
    .service(redis_tower::connect("localhost:6379").await?);
```

## Optional Features

Enable features based on your needs:

```toml
[dependencies]
redis-tower = { version = "0.1", features = ["cluster", "sentinel", "bloom"] }
```

Available features:

- `cluster` - Redis Cluster support with automatic slot routing
- `sentinel` - Redis Sentinel support for high availability
- `deprecated` - Deprecated commands (GETSET, RPOPLPUSH) with migration guides
- `modules` - Parent feature for Redis modules
- `bloom` - Bloom filter commands (11 commands)
- `json` - RedisJSON commands (planned)
- `search` - RediSearch commands (planned)
- `timeseries` - RedisTimeSeries commands (planned)

## Type Safety Examples

Commands know their response types at compile time:

```rust
// Compiler knows this returns Option<Bytes>
let value: Option<Bytes> = client.call(Get::new("key")).await?;

// Compiler knows this returns i64
let count: i64 = client.call(Incr::new("counter")).await?;

// Compiler knows this returns Vec<(String, f64)>
let members: Vec<(String, f64)> = client
    .call(ZRangeByScore::new("leaderboard", 0.0, 100.0).withscores())
    .await?;
```

Builder patterns for complex commands:

```rust
// Pipeline multiple operations
let result = client.call(
    Set::new("key", "value")
        .ex(3600)  // Expire in 1 hour
        .nx()      // Only set if not exists
        .get()     // Return old value
).await?;

// Stream consumer groups
let messages = client.call(
    XReadGroup::new("mygroup", "consumer1")
        .stream("mystream", ">")
        .count(10)
        .block(5000)
).await?;

// Geospatial search
let locations = client.call(
    GeoSearch::new("places")
        .from_member("Paris")
        .by_radius(100.0, GeoUnit::Kilometers)
        .count(10)
        .with_coord()
        .with_dist()
).await?;
```

## Cluster Support

Enable cluster routing with the `cluster` feature:

```rust
use redis_tower::cluster::ClusterClient;

let client = ClusterClient::new(vec![
    "redis://127.0.0.1:7000",
    "redis://127.0.0.1:7001",
    "redis://127.0.0.1:7002",
]).await?;

// Automatic slot-based routing
client.call(Set::new("key", "value")).await?;

// Read-only commands can use replicas
client.call(Get::new("key")).await?;  // May route to replica
```

## Sentinel Support

Enable high availability with the `sentinel` feature:

```rust
use redis_tower::sentinel::SentinelClient;

let client = SentinelClient::builder()
    .master_name("mymaster")
    .sentinel("127.0.0.1:26379")
    .sentinel("127.0.0.1:26380")
    .sentinel("127.0.0.1:26381")
    .build()
    .await?;
```

## Architecture

- **Commands**: Each Redis command is a strongly-typed struct implementing `Command` trait
- **Codec**: Efficient RESP2/3 parser using nom for zero-copy parsing
- **Service**: Tower `Service` trait for composable middleware
- **Connection**: Tokio-based async connection handling with framing
- **Pool**: Connection pooling with configurable size and timeouts
- **Cluster/Sentinel**: Optional deployment topology support

See [CLAUDE.md](CLAUDE.md) for detailed architecture documentation.

## Performance

Built on a high-performance RESP parser:
- ~34-48ns per parse operation
- 4.8-8.0 GB/s throughput
- Zero-copy parsing where possible

Benchmarks coming soon comparing to `redis-rs` and `fred`.

## Development

```bash
# Build with all features
cargo build --all-features

# Run all tests
cargo test --all-features

# Run clippy
cargo clippy --all-targets --all-features -- -D warnings

# Format code
cargo fmt --all

# Run examples
cargo run --example basic
cargo run --example cluster_with_tower --features cluster
cargo run --example resilient --features cluster
```

## Project Status

**Version**: 0.1.0 (Pre-release)

Currently implements:
- ✅ 200 Redis commands (50% coverage)
- ✅ Cluster support with slot routing
- ✅ Sentinel support for HA
- ✅ Bloom filter module (11 commands)
- ✅ Type-safe command API
- ✅ Builder patterns for complex commands
- ✅ ReadOnly trait for replica routing
- ✅ Feature flags for zero-cost abstractions

Planned for v0.2.0:
- Connection pooling improvements
- Additional Redis modules (JSON, Search, TimeSeries)
- Performance benchmarks
- Client-side caching
- Transaction builder improvements
- Pipeline builder
- More comprehensive examples

## Contributing

Contributions welcome! This project follows standard Rust development practices:

- Run tests before submitting: `cargo test --all-features`
- Run clippy: `cargo clippy --all-targets --all-features -- -D warnings`
- Format code: `cargo fmt --all`
- Use conventional commits: `feat:`, `fix:`, `docs:`, etc.

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## Comparison to Other Clients

| Feature | redis-tower | redis-rs | fred |
|---------|-------------|----------|------|
| Type Safety | ✅ Strong | ⚠️ Weak | ⚠️ Weak |
| Tower Integration | ✅ Native | ❌ No | ❌ No |
| Middleware | ✅ Composable | ❌ No | ⚠️ Some |
| Command Count | 200 | ~280 | ~350 |
| Cluster | ✅ Yes | ✅ Yes | ✅ Yes |
| Sentinel | ✅ Yes | ✅ Yes | ✅ Yes |
| Async | ✅ Tokio | ✅ Multi | ✅ Tokio |
| RESP3 | 🚧 Planned | ✅ Yes | ✅ Yes |
| Pipelining | 🚧 Planned | ✅ Yes | ✅ Yes |
| Maturity | ⚠️ Experimental | ✅ Stable | ✅ Stable |

`redis-tower` prioritizes type safety and composability over command coverage. It's ideal for projects that want to leverage Tower's middleware ecosystem.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Acknowledgments

- Built on the excellent [Tower](https://github.com/tower-rs/tower) ecosystem
- Inspired by the patterns in `redis-rs` and `fred`
- Uses high-performance RESP parsing from `resp-parser`
