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
p50/p90/p99/p999/max latency. Every successful GET must match the exact fixture
bytes (`x` repeated to the configured size), including strict replica reads.
Missing, wrong-sized, and same-sized corrupt values are errors; they contribute
neither successful commands nor success latency samples. Replica preflight also
checks exact contents. GET timing includes byte validation for every adapter;
older length-only runs are not directly equivalent measurements.
Failed seed writes, worker connection/setup errors, and worker panics abort the
benchmark with a non-zero exit status.
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
| `BENCH_CHURN_PROTOCOL` | `client-defaults` | Configured policies: historical defaults, or explicit `resp2` / `resp3` for both clients |
| `BENCH_CHURN_PROFILE` | `client-defaults` | Historical settings, or fixed opt-in `socket-deadlines-v1`; strict selector, validated before startup |
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
The first-success timestamp is a completion after the confirmed event marker:
it can be an already-buffered pre-event reply, not proof of routing to the new
owner. Use useful post-election work and final-error recovery observations when
assessing recovery.
Repeated-run output also reports how many runs reached a first success and how
many erroring runs recovered. The corresponding timing mean is `null`/`n/a`
unless every applicable run recovered, so one wedged run cannot disappear into
an average of the successful runs.
For redis-tower it also reports ASK/MOVED counters and topology-refresh
outcomes through the client's metrics hooks. redis-rs does not expose those
hooks, so its redirect and refresh fields are `null`, not a misleading zero.

### Churn correctness and failure accounting

Churn JSON now declares `schema_version: 1` (previous churn output was
unversioned). Existing counts and timings remain present. `stable_failures`,
`churn_failures`, and `recovery_failures` partition each phase's legacy error
count into three bounded categories:

- `client`: a surfaced client error, without asserting equivalent internal
  error/retry policies or emitting raw error messages or payloads.
- `invalid_payload`: missing, wrong-length or equal-length corrupt GET content.
  These are fatal correctness failures, never permitted outage errors.
- `unresolved`: an in-flight request canceled at worker teardown. Redis
  execution is unknown; cancellation does not prove the command did not execute.

`warmup_failures` is diagnostic only, excluded from measured counts and latency.
A payload violation in **any** window fails the campaign with a nonzero exit;
it cannot disappear in warmup, acceptable outage errors, or an average of later
recovered runs. Invalid-payload workers stop, all clients are shut down, and
normal fixture ownership cleans up the managed Redis processes. Runs failing
inside the churn driver emit a `churn_failure_diagnostics=` JSON record on
stderr containing prior completed runs and the failed run's per-client,
per-phase accounting; stdout
does not contain an ordinary successful campaign result. Retain stderr with
the successful JSON outputs. Earlier fixture/seed/client setup failures retain
their ordinary stderr but do not emit this driver diagnostic record.
Worker panics and injection failures also fail the campaign rather than
becoming ordinary successful results.

By default, tower negotiates RESP automatically and redis-rs uses RESP2.
Set `BENCH_CHURN_PROTOCOL=resp2` or `resp3` to explicitly select the same wire
protocol for both churn adapters, including topology-created/reconnected
connections. The selector is strict and validated before fixture startup;
`client-defaults` preserves the historical comparison. Throughput and replica
scenarios are unchanged.

Churn schema version 1 adds `configured_protocol` to each raw success/failure
report and aggregate: `auto`, `resp2`, or `resp3`. This records the policy passed
to the adapter builder, not an observed negotiation; live socket observations
are separate evidence. Mixed configured policies cannot be aggregated.
Protocol selection alone leaves socket, timeout, retry and batching defaults
unchanged. Do not infer a reliability or performance ranking from differing
surfaced error counts alone.

### Versioned socket/deadline profile

```bash
BENCH_CHURN_PROTOCOL=resp3 BENCH_CHURN_PROFILE=socket-deadlines-v1 \
BENCH_WARMUP=1 BENCH_BASELINE_SECS=2 BENCH_RECOVERY_SECS=2 \
BENCH_CHURN_CONCURRENCY=2 BENCH_CHURN_RUNS=2 \
cargo run --release -p cluster-bench -- --failover --json
```

This diagnostic profile configures TCP_NODELAY=true, keepalive idle=60s,
interval=10s and probes=3 (probe count unsupported on Windows), connect=1s
and response=2s. Values are fixed; no numeric profile overrides are accepted.
Keep protocol selection independent; `client-defaults` is still tower Auto
and redis-rs RESP2. The existing primary route, five-byte validated GET payload,
WAIT-confirmed seeding, fixture persistence and event/accounting are unchanged.

These are **configured values, not observed kernel/socket attestation**. Both
builders retain them for replacement and topology-created connections. The
deadline boundaries are deliberately disclosed rather than labeled matched:

| Policy | redis-tower mux | redis-rs ClusterClient async |
|---|---|---|
| Connect deadline | TCP connect only; excludes protocol/auth/TLS setup | Multiplexed socket/handshake creation; subsequent cluster READONLY/PING use response deadlines |
| Response deadline | Batch `execute_pipeline` write/reply wait; excludes queue, reconnect and outer redirect loop | Each node request plus 2s overall request including retries, redirects and reconnections |
| Overall command deadline | None added | 2s |
| Unchanged retry policy | 5 redirects; 3 node reconnect retries, jittered 100ms base / 5s cap | Source-derived redis-rs 1.7.0 defaults: 16 cluster retries; jitter formula min1280ms/max655360ms/base2/factor10 |
| Queue/batching | Queue1024, batch100, zero batch window | Internal driver details unavailable, reported as null |

The 1s connect value is shorter than the 2s response value, but this is **not
a fully matched failure policy or an end-to-end tower request budget**. Tower
connection setup can still wait outside the TCP connect deadline, and repeated
client-internal routing/reconnection can exceed a single batch deadline. No
application retry is added, including around writes of unknown execution state.
The default profile keeps tower connect/response deadlines absent and redis-rs
connect=1s with response/overall deadlines absent; tower already uses NODELAY
and 60s/10s/3 keepalive, while redis-rs defaults to NODELAY=false/no keepalive.
Default disclosures are source-derived, not an independent client-version
probe; retain the exact lockfile and revisions alongside experiment results.

Each raw report (including failed campaign reports) and aggregate now retains
`configured_policy`: profile identity, socket/deadline values and boundaries,
retry/queue differences and `fully_matched_failure_policy: false`. Mixed policy
identities **or values** cannot be aggregated. Disabled deadline fields and
unavailable internal queue/driver metrics are null, not measured zero. Startup
and campaign errors also emit a `churn_configuration_diagnostics=` JSON record
with both clients' configured policies even when no workers/reports exist;
campaign accounting remains in `churn_failure_diagnostics=`. Invalid selectors
fail before starting a fixture.

The live `live_get_payload_integrity` filter covers exact payloads and protocol
after slot handoff for both profile modes. A controlled owned-primary
`CLIENT PAUSE` test checks that both native response deadlines fire before the
outer test guard, on initial, CLIENT KILL replacement and moved-slot sockets;
the historical-default negative control reaches only the outer guard. These
are bounded functional checks, not a real packet-loss/partition or long-soak
qualification. Run with `--ignored --test-threads=1 --nocapture`. Keep raw
repeated CLI reports/errors, source/lock/binary/toolchain, Redis topology and
settings, cleanup and separate wire observations. Shared-host smokes do not
support throughput, latency or reliability rankings.

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
