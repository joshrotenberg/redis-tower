# soak-bench

`soak-bench` is an hours-long, constant-memory reliability harness for
redis-tower. It continuously performs and validates `GET` operations against a
managed standalone Redis process or a managed six-node Redis Cluster.

Every worker owns two fixed-range HDR histograms: one for the current reporting
interval and one for the complete measured run. Workers hand snapshots to the
coordinator between commands; the hot path has no shared mutex and does not
retain one allocation per operation. Warmup samples are reset at a barrier and
never enter reported counts or latency distributions. Workers share absolute
interval deadlines and stop starting work at each boundary. A command crossing
an intermediate boundary is carried into the first later interval containing
its completion; a command completing after the final deadline is excluded.
Reported windows and the final duration therefore describe exactly the samples
they bound rather than the time it happened to take the coordinator to collect
them.

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

Lifecycle counters take their own baseline at the measurement barrier, so a
warmup reconnect is discarded with the warmup latency samples. A requested
fault must finish recovery before the measurement deadline while workload
workers are still active. A late or failed recovery aborts the chaos task and
fails the run without publishing a misleading summary. Standalone boundaries
are ordered on the same connection-event stream, which drains queued warmup
events and freezes the final counter snapshot before client shutdown.

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

## Four-hour runs

The standalone chaos mode sends a real SIGKILL through
`redis_server_wrapper::chaos::kill_node`, consumes the killed handle so it
cannot target a replacement during cleanup only after both its PID and listener
are confirmed dead, starts a fresh process on the exact same port, reseeds the
validation key, and requires the existing client to recover within the
configured bound. Startup owns a unique PID-identity process cleanup guard,
including when its future is cancelled or times out. Cleanup requires the
pidfile, generated config, Redis command/start fingerprint, and process working
directory to match that unique run before sending any signal. Port state is
used only to observe shutdown; an occupied or raced port never authorizes a
shutdown command:

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
SIGINT and SIGTERM cancel abort-owned chaos work and synchronously clean the
managed standalone or cluster processes before the CLI exits.

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

`SOAK_ERROR_BACKOFF_MS` must be non-zero. `SOAK_OPERATION_TIMEOUT_MS` must be
at most 60000 because the constant-size latency histogram has an explicit
two-minute recording range, leaving scheduler headroom above the accepted
60-second operation timeout. The harness rejects a larger timeout instead of
silently clamping latency.

When `SOAK_STANDALONE_PORT` is omitted, the harness briefly reserves an
ephemeral loopback port and then starts Redis on it. An explicit port must be
dedicated to the run to avoid startup failure. Cleanup signals only the exact
PID whose pidfile, generated config, command/start fingerprint, and working
directory prove ownership; port state is observation only.
