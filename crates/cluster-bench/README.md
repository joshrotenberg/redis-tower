# cluster-bench

Throughput benchmark comparing four Redis Cluster clients side-by-side:

- `redis_tower_cluster::ClusterClient` (mutex-based baseline)
- `redis_tower_cluster::MultiplexedClusterClient` (per-node auto-pipeline)
- `redis::cluster::ClusterClient` (redis-rs sync)
- `redis::cluster_async::ClusterConnection` (redis-rs async)

## Running

Requires a 3-master Redis cluster. The harness spins one up automatically
via `redis-test-harness`:

```bash
cargo run -p cluster-bench --release
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BENCH_SECS` | `10` | Duration per run in seconds |
| `BENCH_CONCURRENCY` | `1,8,32,64,128` | Comma-separated concurrency levels |
| `BENCH_BASE_PORT` | `17000` | Starting port for the throwaway cluster |

## Results

Measured on Apple M3 Max, 3-master local cluster, 10s per run.
Last updated: 2026-04-12.

### c=128 (high concurrency)

| Client | SET ops/s | GET ops/s | GET p99 (us) |
|--------|----------:|----------:|-------------:|
| ClusterClient (baseline) | 13,786 | 13,944 | 9,955 |
| redis-rs cluster sync | 170,762 | 171,524 | 1,147 |
| redis-rs cluster_async | 448,851 | 448,206 | 537 |
| MultiplexedClusterClient | 502,306 | 522,441 | 383 |

`MultiplexedClusterClient` delivers ~35x the throughput of `ClusterClient`
and outperforms redis-rs `cluster_async` by ~12% with lower tail latency.

These numbers are a relative comparison, not absolute guarantees -- your
hardware, network, and payload size will shift the raw figures.
