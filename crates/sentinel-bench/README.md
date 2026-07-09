# sentinel-bench

Throughput benchmark for the Redis Sentinel clients, measuring the steady-state
overhead of the sentinel-discovered path relative to a direct connection to the
same master:

- `redis_tower_sentinel::SentinelClient` (`Arc<Mutex<SentinelConnection>>`, mutex baseline)
- `redis_tower_sentinel::MultiplexedSentinelClient` (factory-reconnect + auto-pipeline, production path)
- `redis_tower::MultiplexedClient` connected straight to the master (`DirectMux`, the reference line)

redis-rs ships no sentinel client benchmark, so there is no external client to
compare against. Compare `MultiplexedSentinelClient` against `DirectMux` at
matched concurrency to read the cost of routing through the sentinel-discovered
connection. No failover is exercised; this measures steady state only.

## Running

Requires a full sentinel topology (1 master, 2 replicas, 3 sentinels). The
harness spins one up automatically via `redis-server-wrapper`:

```bash
cargo run -p sentinel-bench --release            # human-readable table
cargo run -p sentinel-bench --release -- --json  # JSON array on stdout
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BENCH_SECS` | `10` | Measured window per run, in seconds |
| `BENCH_WARMUP` | `2` | Warmup window discarded per run, in seconds |
| `BENCH_RUNS` | `3` | Repeats per cell; results report mean +/- stddev |
| `BENCH_CONCURRENCY` | `1,8,32,128` | Comma-separated concurrency levels |
| `BENCH_MASTER_PORT` | `6490` | Master port for the throwaway topology |
| `BENCH_REPLICA_BASE` | `6491` | Base port for the replicas |
| `BENCH_SENTINEL_BASE` | `26490` | Base port for the sentinels |

## Interpreting results

- `ops/s`: higher is better; `p50`/`p90`/`p99`/`p999`: lower latency is better.
- `ops/s` is reported as a mean across `BENCH_RUNS` with the standard deviation;
  latency percentiles are HDR-histogram values averaged across runs.
- `MultiplexedSentinelClient` should track `DirectMux` closely: sentinel
  discovery is a one-time connect cost, so steady-state throughput is dominated
  by the underlying multiplexed connection, not the sentinel hop.
- `SentinelClient` serializes commands behind a single mutex, so it should trail
  both multiplexed clients as concurrency rises, mirroring the `RedisClient` vs
  `MultiplexedClient` gap in `standalone-bench`.

These numbers are a relative comparison, not absolute guarantees; hardware,
network, and payload size shift the raw figures.
