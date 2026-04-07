# Leaderboard Comparison

Side-by-side comparison of a gaming leaderboard implemented with three Redis
client libraries: **redis-tower**, **redis-rs**, and **fred**.

## What it does

Each binary performs the same operations against a local Redis server:

1. ZADD 1000 players with deterministic random scores (seeded RNG)
2. ZREVRANGE top-10 with scores
3. ZRANK for 100 random players (pipelined)
4. ZINCRBY 500 score updates (pipelined)
5. Final top-10

Timings and results are printed to stdout.

## Running

```bash
# Start Redis locally, then:
cargo run -p leaderboard-tower
cargo run -p leaderboard-redis-rs
cargo run -p leaderboard-fred
```

## Key differences

| Aspect | redis-tower | redis-rs | fred |
|--------|-------------|----------|------|
| Pipelining | `Pipeline::new().push(cmd)` | `pipe().zadd(...)` | `client.pipeline()` |
| Type safety | Typed command structs | Method-level generics | Turbofish on methods |
| ZREVRANGE | `RawCommand` fallback | `zrevrange_withscores()` | `zrevrange()` |
| Connection | `RedisConnection` (Tower Service) | `MultiplexedConnection` | `Client` with init |
