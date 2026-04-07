# Rate Limiter Comparison

Sliding window rate limiter implemented with three Redis client libraries:
redis-tower, redis-rs, and fred.

Each implementation uses a sorted set with MULTI/EXEC to atomically check
and update rate limit state. The benchmark runs 1000 rate limit checks across
10 concurrent tasks (100 per task) against 50 distinct user keys.

## Running

Requires a Redis server on `127.0.0.1:6379`.

```bash
# Run all three
cargo run -p rate-limiter-tower
cargo run -p rate-limiter-redis-rs
cargo run -p rate-limiter-fred

# Or in release mode for more realistic numbers
cargo run --release -p rate-limiter-tower
cargo run --release -p rate-limiter-redis-rs
cargo run --release -p rate-limiter-fred
```

## Ergonomic Observations

- **redis-tower**: TODO
- **redis-rs**: TODO
- **fred**: TODO
