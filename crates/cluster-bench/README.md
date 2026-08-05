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

The default remains the stable throughput matrix. Topology-churn workloads
are opt-in, so existing weekly runs and their JSON schema do not change.

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BENCH_SECS` | `10` | Duration per run in seconds |
| `BENCH_WARMUP` | `2` | Unmeasured warmup per run in seconds |
| `BENCH_RUNS` | `3` | Repeated runs per throughput cell |
| `BENCH_CONCURRENCY` | `1,8,32,128` | Comma-separated concurrency levels |
| `BENCH_BASE_PORT` | `17000` | Starting port for the throwaway cluster |

Add `--json` for machine-readable output. Throughput JSON remains the
historical array of throughput cells.

## Reshard and failover churn

The churn modes start a fresh 3-master + 3-replica fixture on ports 17500+
and drive `MultiplexedClusterClient` and redis-rs `cluster_async` concurrently
against one affected hash slot. This makes their error and recovery windows
comparable under the same topology event.

```bash
# Held MIGRATING/IMPORTING window, then slot handoff (ASK followed by MOVED)
cargo run -p cluster-bench --release -- --scenario reshard

# Kill the slot owner and wait for its replica to be elected
cargo run -p cluster-bench --release -- --scenario failover

# Equivalent environment selection, with JSON on stdout
BENCH_SCENARIO=reshard cargo run -p cluster-bench --release -- --json
```

Churn-specific configuration:

| Variable | Default | Description |
|----------|---------|-------------|
| `BENCH_SCENARIO` | `throughput` | `throughput`, `reshard`, or `failover` |
| `BENCH_CHURN_RUNS` | `1` | Fresh six-node fixture runs per scenario |
| `BENCH_CHURN_CONCURRENCY` | `16` | Workers per client under the same event |
| `BENCH_BASELINE_SECS` | `3` | Stable pre-event measurement window |
| `BENCH_RECOVERY_SECS` | `3` | Post-event recovery measurement window |
| `BENCH_CHURN_HOLD_MS` | `1000` | Held ASK/post-convergence sampling windows |
| `BENCH_CHURN_SLOT` | `42` | Exact Redis Cluster hash slot exercised |
| `BENCH_CHURN_WORKLOAD` | `get` | Affected-slot `get` or `set` workload |
| `BENCH_CHURN_BASE_PORT` | `17500` | First of six fixture client ports |
| `BENCH_CLUSTER_NODE_TIMEOUT_MS` | `1000` | Redis failure-detection timeout |
| `BENCH_TOPOLOGY_TIMEOUT_SECS` | `15` | Bound for owner-change convergence |

For a quick local smoke check, shorten the sampling windows while keeping the
event itself real:

```bash
BENCH_WARMUP=0 \
BENCH_BASELINE_SECS=1 \
BENCH_RECOVERY_SECS=1 \
BENCH_CHURN_HOLD_MS=250 \
BENCH_CHURN_CONCURRENCY=2 \
cargo run -p cluster-bench -- --scenario reshard
```

The churn report includes stable/churn p99 and p999, their deltas, dropped
(failed) operations, the first successful completion after the exact trigger,
recovery after the final surfaced error, the first-to-last error window, and
external topology-convergence time. A tail percentile and its delta are
`null`/`n/a` when that phase has no successful samples.
Repeated-run output also reports how many runs reached a first success and how
many erroring runs recovered. The corresponding timing mean is `null`/`n/a`
unless every applicable run recovered, so one wedged run cannot disappear into
an average of the successful runs.
For redis-tower it also reports ASK/MOVED counters and topology-refresh
outcomes through the client's metrics hooks. redis-rs does not expose those
hooks, so its redirect and refresh fields are `null`, not a misleading zero.

All timing and tail-latency values are informational. There are intentionally
no pass/fail thresholds: local process scheduling and Redis election timing
vary by host. In particular, confirm each phase's sample count before comparing
p999 from a short smoke run. For a baseline worth publishing, use release mode,
at least the default three-second windows and 16 workers, repeat with
`BENCH_CHURN_RUNS=3`, and compare clients from the same invocation rather than
against absolute numbers from another machine.

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
