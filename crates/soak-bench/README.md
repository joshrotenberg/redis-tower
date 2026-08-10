# soak-bench

`soak-bench` is an hours-long, constant-memory reliability harness for
redis-tower. It continuously performs and validates `GET` operations against a
managed standalone Redis process or a managed six-node Redis Cluster.

Every worker owns two fixed-range HDR histograms: one for the current reporting
interval and one for the complete measured run. Workers hand snapshots to the
coordinator between commands; the hot path has no shared mutex and does not
retain one allocation per operation. Warmup samples are reset at a barrier and
never enter reported counts or latency distributions.

## Output semantics

The default reporting interval is one minute. Human output is the default;
`--jsonl` writes a metadata record, one record per interval, and a final summary
to stdout. Diagnostics go to stderr, so stdout can be redirected directly to an
artifact.

Each interval includes successful operations/second, attempts, errors,
p50/p99/p999/max latency, recovery counters, chaos injection counters, and the
current RSS of the soak process. Percentiles cover successful, payload-validated
GET completions only. `attempted_ops_per_sec` is also emitted so fail-fast
errors cannot masquerade as useful throughput. Workers apply the configured
one-millisecond default backoff after an error to avoid a tight retry loop. RSS
excludes the managed Redis child processes.

Reconnect and recovery fields are intentionally topology-specific:

- Standalone `reconnects` and `recoveries` count exact
  `ConnectionEvent::Reconnected` events from the reconnecting multiplexed
  client. A lagged event subscriber makes the run fail instead of publishing an
  inexact total.
- The cluster client does not expose one global reconnect stream. Cluster JSON
  therefore reports `reconnects: null`, not a misleading zero. A cluster
  `recovery` is explicitly harness-observed: the fixture saw the killed slot's
  owner change and the benchmark client then completed a validated GET.

Errors during a deliberate outage remain errors. They are neither hidden nor
subtracted from throughput.

JSON metadata records the workload, payload, concurrency, warmup, duration,
report cadence, operation/error-backoff/startup/recovery timeouts, topology
controls, selected standalone port, and the exact reconnect/recovery,
latency, and RSS accounting modes. This lets publication tooling reject a run
that relied on an unintended default.

## Four-hour runs

The standalone chaos mode sends a real SIGKILL through
`redis_server_wrapper::chaos::kill_node`, consumes the killed handle so it
cannot target a replacement during cleanup, starts a fresh process on the exact
same port, reseeds the validation key, and requires the existing client to
recover within the configured bound:

```bash
SOAK_MODE=standalone \
SOAK_CHAOS=standalone-sigkill \
SOAK_DURATION_SECS=14400 \
SOAK_WARMUP_SECS=60 \
SOAK_REPORT_INTERVAL_SECS=60 \
SOAK_CHAOS_AFTER_SECS=7200 \
SOAK_CONCURRENCY=32 \
cargo run --release -p soak-bench -- --jsonl > standalone-soak.jsonl
```

The cluster mode starts three masters and one replica per master, synchronously
replicates its slot-pinned key, SIGKILLs that slot's current master through the
fixture chaos API, and waits for bounded promotion plus a successful client
probe:

```bash
SOAK_MODE=cluster \
SOAK_CHAOS=cluster-master-kill \
SOAK_DURATION_SECS=14400 \
SOAK_WARMUP_SECS=60 \
SOAK_REPORT_INTERVAL_SECS=60 \
SOAK_CHAOS_AFTER_SECS=7200 \
SOAK_CONCURRENCY=32 \
cargo run --release -p soak-bench -- --jsonl > cluster-soak.jsonl
```

Both modes require `redis-server` and `redis-cli` on `PATH`. Run them on an
otherwise idle host, retain stderr with the JSONL artifact, and capture the
Redis/Rust versions and git commit alongside the result. The short CI job is a
functional smoke test and deliberately sets no performance threshold; a
publishable four-hour result belongs in the benchmark evidence report.
For release evidence, prefer the repository's
[`run_publication.sh`](../../scripts/benchmarks/run_publication.sh) protocol;
it sets every input, protects the whole run from sleep where supported, and
refuses to finalize without validating all 240 one-minute records.

## Configuration

Run `cargo run -p soak-bench -- --help` for all environment variables. Useful
short-run overrides are:

```text
SOAK_DURATION_SECS=12
SOAK_WARMUP_SECS=1
SOAK_REPORT_INTERVAL_SECS=2
SOAK_CHAOS_AFTER_SECS=3
SOAK_OPERATION_TIMEOUT_MS=1000
SOAK_ERROR_BACKOFF_MS=1
SOAK_STARTUP_TIMEOUT_SECS=30
SOAK_RECOVERY_TIMEOUT_SECS=15
SOAK_PAYLOAD_BYTES=16
SOAK_CLUSTER_SLOT=42
SOAK_CLUSTER_NODE_TIMEOUT_MS=500
SOAK_STANDALONE_PORT=<dedicated port>
```

When `SOAK_STANDALONE_PORT` is omitted, the harness briefly reserves an
ephemeral loopback port and then starts Redis on it. An explicit port must be
dedicated to the run because `redis-server-wrapper` owns and cleans that port.
