# Cache-Aside Pattern Comparison

Compares redis-tower, redis-rs, and fred implementing the same cache-aside
pattern: a local `HashMap` backed by Redis for cache misses.

## What it does

1. Populates Redis with 100 keys (`item:0` through `item:99`)
2. Runs 10,000 random GET requests across 10 concurrent tasks
3. Checks a local `Arc<RwLock<HashMap>>` first (cache hit) and falls back to
   Redis on miss
4. Reports total time, hit rate, and requests/sec

All three use the same fixed random seed so the access pattern is identical.

## Running

Requires a Redis server on `127.0.0.1:6379`.

```bash
cargo run -p caching-tower
cargo run -p caching-redis-rs
cargo run -p caching-fred
```

Or build all at once:

```bash
cargo build --release
./target/release/caching-tower
./target/release/caching-redis-rs
./target/release/caching-fred
```

## What to look for

- **redis-tower**: Typed commands (`Get::new`, `Set::new`) with
  `RedisValueExt::parse_into::<String>()` for type conversion. Client is
  shared via `Clone`.
- **redis-rs**: `MultiplexedConnection` with `AsyncCommands` trait methods
  (`conn.get()`, `conn.set()`). Type conversion via `FromRedisValue`.
- **fred**: `Client` with typed generic methods (`client.get::<String, _>()`).
  Shared via `Clone`.

The comparison focuses on client ergonomics for read-heavy workloads, not raw
throughput (the local cache dominates performance in all three).
