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
  --bin resource-build-measure -- --json
```

The redis-tower subject uses the public facade with default Redis Stack modules
disabled, redis-rs enables its Tokio compatibility feature, and Fred uses its
default features. These exact feature selections are part of the comparison and
should be preserved when results are compared. Record the host, Rust toolchain,
repository SHA, power state, and background workload with published results.
Run on an otherwise-idle dedicated host; GitHub-hosted smoke artifacts are
useful for detecting broken probes, not for declaring small winners.

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
  --bin resource-build-measure -- --json
```

Both outputs use a versioned JSON schema. Measurements are informational and
have no pass/fail performance threshold.
