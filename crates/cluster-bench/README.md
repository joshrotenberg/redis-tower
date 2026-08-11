# cluster-bench

Throughput benchmark comparing five Redis Cluster clients side-by-side:

- `redis_tower_cluster::ClusterClient` (mutex-based baseline)
- `redis_tower_cluster::MultiplexedClusterClient` (per-node auto-pipeline)
- `redis::cluster::ClusterClient` (redis-rs sync)
- `redis::cluster_async::ClusterConnection` (redis-rs async)
- `fred::clients::Client` (fred async cluster client)

## Running

Requires a 3-master Redis cluster. The harness spins one up automatically
via `redis-test-harness`:

```bash
cargo run -p cluster-bench --release
```

The throughput scenario launches `redis-server` from `PATH` with Redis Stack
module auto-loading disabled, so the measured runtime matches the version
recorded by the publication fingerprint.

The default stable matrix covers GET and SET with 64 B, 1 KiB, and 16 KiB
values. Replica reads and topology-churn workloads are opt-in scenarios.

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BENCH_SECS` | `10` | Duration per run in seconds |
| `BENCH_WARMUP` | `2` | Unmeasured warmup per run in seconds |
| `BENCH_RUNS` | `3` | Repeated runs per throughput cell |
| `BENCH_CONCURRENCY` | `1,8,32,128` | Comma-separated concurrency levels |
| `BENCH_PAYLOAD_SIZES` | `64,1024,16384` | Comma-separated value sizes; `K`/`KiB` and `M`/`MiB` suffixes are accepted |
| `BENCH_CLIENTS` | all five clients | Comma-separated client aliases (`redis-tower`, `redis-tower-mux`, `redis-rs-sync`, `redis-rs-async`, `fred`) |
| `BENCH_INCLUDE_SAMPLES` | `false` | Retain every bounded per-run sample in JSON output (`--include-samples`) |
| `BENCH_BASE_PORT` | `17000` | Starting port for the throwaway cluster |

The matrix axes also have CLI forms, for example:

```bash
cargo run -p cluster-bench --release -- \
  --payload-sizes 64,1K,16K \
  --concurrency 1,32,128 \
  --clients redis-tower-mux,redis-rs-async,fred \
  --json
```

JSON output contains one object per cell with the payload size, successful
command count, error count, commands/s mean and standard deviation, and HDR
p50/p90/p99/p999/max latency. A missing GET or a value of the wrong size is an
error, not a successful operation. Failed seed writes, worker connection/setup
errors, and worker panics abort the benchmark with a non-zero exit status.
Latency uses a checked HDR histogram with an explicit two-minute range; an
out-of-range successful operation fails loudly instead of being clipped. The
aggregate percentiles are means of per-run percentiles, while `max_us` is the
largest HDR-reported run maximum.

Each record declares `schema_version: 2` and adds a stable kebab-case
`client_id`. The historical `client` variant name and `total_ops` /
`ops_per_sec_*` fields remain available; `total_batches` / `batches_per_sec_*`
and `total_commands` / `commands_per_sec_*` make their units explicit.
Pass `--include-samples` for publication evidence. It adds the raw bounded run
samples—with measured wall time, counts, rates, and latency—needed to recompute
each rate, mean, and standard deviation while remaining omitted from ordinary
schema-v2 output.

## Replica-read scenario

The replica scenario starts the managed three-master/three-replica
`redis_tower_test::ClusterFixture`. It seeds each key directly through its slot
owner, requires `WAIT 1` on every master, and verifies every key through a
strict `ReadPreference::Replica` client before collecting measurements. The
default comparison is the multiplexed master route versus the same client with
strict replica routing.

```bash
cargo run -p cluster-bench --release -- --scenario replica --json
```

Set `BENCH_REPLICA_BASE_PORT`, or pass `--replica-base-port`, only when a fixed
six-port range is required. Otherwise the fixture leases an available range.

## Reshard and failover churn

The churn modes start a fresh 3-master + 3-replica fixture on ports 17800+
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
| `BENCH_SCENARIO` | `throughput` | `throughput`, `replica`, `reshard`, or `failover` |
| `BENCH_CHURN_RUNS` | `1` | Fresh six-node fixture runs per scenario |
| `BENCH_CHURN_CONCURRENCY` | `16` | Workers per client under the same event |
| `BENCH_BASELINE_SECS` | `3` | Stable pre-event measurement window |
| `BENCH_RECOVERY_SECS` | `3` | Post-event recovery measurement window |
| `BENCH_CHURN_HOLD_MS` | `1000` | Held ASK/post-convergence sampling windows |
| `BENCH_CHURN_SLOT` | `42` | Exact Redis Cluster hash slot exercised |
| `BENCH_CHURN_WORKLOAD` | `get` | Affected-slot `get` or `set` workload |
| `BENCH_CHURN_BASE_PORT` | `17800` | First of six fixture client ports |
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

The weekly workflow uploads the complete stable and replica JSON matrices for
90 days. Static headline numbers are intentionally kept out of this runner's
README: CPU policy, Redis version, payload size, concurrency, and client version
all materially affect them. Record those inputs alongside any published result
and compare clients from the same invocation.
