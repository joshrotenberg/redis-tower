# Redis client resource benchmark

`resource-bench` compares redis-tower, redis-rs, and Fred on the cost axes that
throughput benchmarks do not show:

- peak resident memory added by a configured number of independent live
  connections;
- process CPU at a fixed offered GET rate;
- clean release-build time; and
- release binary size before and after `strip`.

Each client is compiled into and run as a separate executable. A measurement
process therefore links exactly one subject client, and one client's runtime or
dependency graph cannot inflate another client's result.

## Live probes

Start Redis separately, then run each subject with the same environment:

```bash
redis-server --port 6481 --save '' --appendonly no

export REDIS_URL=redis://127.0.0.1:6481/
export RESOURCE_CONNECTIONS=100
export RESOURCE_TARGET_OPS_PER_SEC=5000
export RESOURCE_WARMUP_SECS=2
export RESOURCE_DURATION_SECS=10
export RESOURCE_DRAIN_TIMEOUT_MS=1000
export RESOURCE_PAYLOAD_BYTES=1024

cargo run -p resource-bench --release \
  --no-default-features --features client-redis-tower \
  --bin resource-redis-tower -- --json
cargo run -p resource-bench --release \
  --no-default-features --features client-redis-rs \
  --bin resource-redis-rs -- --json
cargo run -p resource-bench --release \
  --no-default-features --features client-fred \
  --bin resource-fred -- --json
```

Every connection is independently established and validates the fixture before
measurement. The fixed-rate workload records attempts, successful GETs,
misses/payload mismatches, and command errors. `attempted_ops_per_sec` shows
whether the host delivered the requested schedule;
`achieved_ops_per_sec` includes only payload-validated responses.
Operations are phase-staggered across one aggregate schedule, including when
the requested rate is lower than the connection count. The duration is a launch
deadline: an in-flight tail GET may finish during the separate bounded drain.
Only a request still incomplete at the drain deadline is canceled and counted
in `cutoff_ops`, never as a client error. A canceled warmup connection is
dropped, and every warmup connection is revalidated or re-established before
CPU measurement.

The raw `REDIS_URL` is never serialized. `redis_endpoint` also masks the
hostname and omits usernames, passwords, query strings, and fragments by
default; it retains only an unambiguous Redis scheme, explicit port, and numeric
database. Ambiguous credentials, Unix socket paths, malformed URLs, and unknown
schemes are reported as `<redacted>`. This makes artifacts safer to publish
without silently treating URL delimiters inside a password as endpoint data.

RSS comes from `getrusage(RUSAGE_SELF).ru_maxrss`. It is a process high-water
mark, not a live heap gauge. The reported connection delta subtracts a baseline
captured after runtime startup and amortizes fixed initialization over
`RESOURCE_CONNECTIONS`; use a sufficiently large, identical connection count.
CPU is user plus system process time divided by wall time and can exceed 100%
when work spans cores.

## Clean builds and binary size

The build probe resolves and fetches the workspace once outside the timer, then
creates a new target directory for every sample. It builds only one subject
feature and binary, records elapsed wall time, copies and strips the binary,
then removes the isolated target directory. The JSON records the resolved
client versions alongside the toolchain and repository SHA:

```bash
# Three cold builds per client by default. This is intentionally slow.
RESOURCE_BUILD_RUNS=3 cargo run -p resource-bench --release \
  --no-default-features --features build-measure \
  --bin resource-build-measure -- --json
```

Every subject dependency disables default features. The exact selections are:

| subject | dependency defaults | explicit dependency features |
| --- | --- | --- |
| redis-tower | disabled | none |
| redis-rs | disabled | `tokio-comp` |
| Fred | disabled | `i-keys` |

The live and build JSON both record these selections. The build report also
records the workspace-relative Git SHA and dirty state, SHA-256 of the exact
generated `Cargo.lock`, and a normalized, bounded `cargo tree` containing
normal, build, and feature edges for each subject. Workspace paths are written
as `$WORKSPACE`, so equivalent checkouts produce the same graph. The weekly
artifact retains the lockfile next to the JSON, since this workspace does not
commit `Cargo.lock`.

Clean-build target directories are registered before creation and removed by
RAII on ordinary errors. The build-only signal handler also removes every
registered directory on SIGINT or termination signals, after stopping and
draining the active compiler or strip process group. SIGKILL cannot be handled;
after a killed run, inspect and remove only stale
`redis-tower-resource-build-*` directories whose owning process is gone.
When capturing JSON manually, redirect stdout outside the checkout if
`git_dirty` must describe only source changes.

Preserve these selections when comparing results. Record the host, Rust
toolchain, repository SHA, power state, and background workload with published
results. Run on an otherwise-idle dedicated host; GitHub-hosted smoke artifacts
are useful for detecting broken probes, not for declaring small winners.

## Short smoke

```bash
RESOURCE_CONNECTIONS=4 \
RESOURCE_TARGET_OPS_PER_SEC=100 \
RESOURCE_WARMUP_SECS=0 \
RESOURCE_DURATION_SECS=1 \
cargo run -p resource-bench --release \
  --no-default-features --features client-redis-tower \
  --bin resource-redis-tower -- --json

RESOURCE_BUILD_RUNS=1 cargo run -p resource-bench --release \
  --no-default-features --features build-measure \
  --bin resource-build-measure -- --json
```

Both outputs use a versioned JSON schema. Measurements are informational and
have no pass/fail performance threshold.
