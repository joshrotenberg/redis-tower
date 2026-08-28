# redis-tower

A Tower-based Redis client with strong typing, composable middleware, and resilience primitives. The GitHub repo is public, but no current release is available: all published crates were yanked on 2026-06-11 and the Release workflow is manual-dispatch only. PR #690 is the pending re-launch. See Release State below.

## Architecture

Workspace of 19 members: 13 publishable crates and 6 internal ones.

```
redis-tower-protocol     (RESP2/3 codec, frame types)
redis-tower-core         (RedisConnection, RedisError, Command trait, TLS)
redis-tower-commands     (typed command builders -- one file per command group)
redis-tower              (clients, middleware layers, pool, pipeline, pub/sub)
  redis-tower-cluster    (cluster topology, routing, MOVED/ASK handling)
  redis-tower-sentinel   (sentinel discovery, failover)
  redis-tower-sync       (blocking wrapper around MultiplexedClient)
  redis-tower-modules    (high-level module clients: JSON, Search, TimeSeries, Probabilistic, Vector)
  redis-tower-primitives (distributed lock, rate limiter, leader election, semaphore, latch, queue, IDs)
  redis-tower-auth-aws   (ElastiCache IAM CredentialProvider)
  redis-tower-auth-azure (Entra ID CredentialProvider)
  redis-tower-client     (UniversalClient: one type over standalone/cluster/sentinel)
redis-tower-test         (test utilities: MockConnection, command_tests! macro, cluster fixtures)
```

Internal, `publish = false`: `redis-chaos-tests` (Docker-tier chaos matrix),
`cluster-bench`, `standalone-bench`, `sentinel-bench`, `soak-bench`,
`resource-bench`.

`redis-tower-cluster` and `redis-tower-sentinel` both depend on `redis-tower`.
`redis-tower-modules` depends on `redis-tower` and `redis-tower-commands`.
`redis-tower-primitives` and the two cloud-auth crates depend on `redis-tower`.
`redis-tower-client` is the top of the graph -- it depends on `redis-tower`,
`redis-tower-cluster`, and `redis-tower-sentinel` so it can unify all three
multiplexed clients (the only crate that sees all of them).

**Directory vs package name.** `redis-tower-test` lives in
`crates/redis-test-harness/`; the directory kept its original name when the
package was renamed and made publishable (#578). Every other crate's directory
matches its package name.

## Key Client Types

| Type | Location | Notes |
|------|----------|-------|
| `RedisConnection` | `redis-tower-core` | Basic single-connection client |
| `RedisClient` | `redis-tower/client.rs` | Arc<Mutex<RedisConnection>>, cloneable |
| `MultiplexedClient` | `redis-tower/multiplexed.rs` | Auto-pipeline, single TCP conn, high concurrency |
| `CachedMultiplexedClient` | `redis-tower/{multiplexed,cache_layer,caching}.rs` | Cloneable standalone CSC, auto-pipelined misses, owned RESP3 invalidation tracking |
| `ConnectionPool<S>` | `redis-tower/pool.rs` | Generic pool; works with any `RedisExecutor` impl |
| `ClusterConnection` | `redis-tower-cluster/connection.rs` | Cluster-aware, MOVED/ASK redirect handling |
| `MultiplexedClusterClient` | `redis-tower-cluster/multiplexed.rs` | Per-node auto-pipeline, no global mutex |
| `CachedMultiplexedClusterClient` | `redis-tower-cluster/{caching,multiplexed}.rs` | Cloneable master-routed Cluster CSC with one shared cache and per-master invalidation coverage |
| `SentinelConnection` | `redis-tower-sentinel/connection.rs` | Discovers master via sentinels, auto-rediscovers on failure |
| `SentinelClient` | `redis-tower-sentinel/client.rs` | Arc<Mutex<SentinelConnection>>, cloneable |
| `MultiplexedSentinelClient` | `redis-tower-sentinel/multiplexed.rs` | Auto-pipeline + sentinel discovery, both static and factory-reconnect ctors |
| `SyncClient` | `redis-tower-sync/lib.rs` | Blocking wrapper, uses tokio Runtime internally |
| `UniversalClient` | `redis-tower-client/lib.rs` | Enum over Standalone/Cluster/Sentinel multiplexed clients; `connect_url` picks the variant by scheme (`redis://`, `redis+cluster://`, `redis+sentinel://h1,h2/master`). All schemes carry percent-decoded `user:pass@` credentials; `rediss`/`rediss+cluster`/`rediss+sentinel` enable TLS (error without a TLS feature, never silent plaintext); sentinel URLs take `?sentinel_username/sentinel_password` and reconnect on failover |

`ConnectionPool<S>` requires `S: RedisExecutor`. Impls exist for `RedisConnection`, `RedisClient`, `ResilientRedisClient`, `CachedClient`, `MultiplexedClient`, `CachedMultiplexedClient`, `ClusterConnection`, `SentinelConnection`, `MultiplexedClusterClient`, `CachedMultiplexedClusterClient`, and `UniversalClient`.

### Cluster-wide SCAN (`redis-tower-cluster/scan_stream.rs`)

`SCAN` is keyless, so `MultiplexedClusterClient::execute` routes it to the default node and returns one node's keys. `ScanClusterStream::scan(&client, pattern)` (and `scan_with_count`) runs the cursor loop against every master and yields `ClusterScanItem { node, key }`. `ClusterScan::new(pattern).count(n).concurrency(w).run(&client)` is the configurable form behind them.

`concurrency` defaults to 1 (sequential, sorted address order, each node's keys contiguous) and is clamped to `MAX_SCAN_CONCURRENCY` (16). Above 1, `flatten_unordered` pages `w` masters at once and keys interleave in completion order; the sequential path stays an explicit loop so its documented visit order does not depend on a combinator's polling order. Within a node there is nothing to parallelize -- the next cursor is only known once the previous page returns. Redis's per-node `SCAN` guarantee is unaffected by width.

The node set is **not** snapshotted once. The scan works in rounds: each round scans the masters the client holds that it has not scanned yet, then re-checks. A settled cluster spends one round on every master and a second that finds nothing, so the traversal matches what a snapshot would give. A master published mid-scan gets its own later round; a master the client drops mid-scan is skipped (`scan_node` breaks cleanly when `execute_on_node` fails and `client.holds_master(&node)` is false, so a *present* node that fails still ends the stream). Rounds are capped at `MAX_MEMBERSHIP_ROUNDS` (8) and exceeding it errors, because stopping quietly would report a scan that knowingly left masters unscanned.

Re-checking alone only sees membership the client already learned, and `SCAN` never teaches it any -- keyless, so never MOVED. `ClusterScan::refresh_membership(true)` (off by default) runs `refresh_topology()` between rounds so the check sees the live slot map, at the cost of one `CLUSTER SLOTS` per round, the refresh's usual service reconciliation, and a failed refresh ending the scan. Refreshes only happen at round boundaries, when no `SCAN` is in flight. The irreducible gap either way: a slot migrating from an unscanned master to an already-scanned one is missed (and the reverse is double-counted), because a per-node cursor tracks no slots. Closing that needs slot-level scan state.

### Cluster read routing (`redis-tower/read_routing.rs`)

`ReadPreference` (defined in `redis-tower`, honored by `ClusterClient` and
`MultiplexedClusterClient`) has three variants: `Master` (default), `Replica`
(strict -- errors when no usable replica serves the key's slot), and
`PreferReplica` (falls back to the master). Writes always go to the master. The
cached cluster client requires `Master` and rejects the replica variants.

When reads go to replicas, `ReadRoutingStrategy` picks which one.
Implementations: `RoundRobinRouting` (default), `RandomRouting`,
`FirstReplicaRouting`, and `AdaptiveReplicaRouting` (built via
`AdaptiveReplicaRoutingBuilder`). The adaptive strategy composes caller-owned
availability-zone metadata, inverse-EWMA latency weighting, consecutive
transport-failure ejection, lazy timed recovery, and a configurable
minimum-candidate floor. Both ordinary clients feed replica attempt outcomes back
to the strategy; writes and master reads never enter that path.

`CLUSTER SLOTS` carries no AZ metadata, so AZ mappings are explicit application
configuration and must map the final node addresses seen after `host_override`
or `address_map` rewriting. Unmapped replicas stay eligible as cross-AZ
fallbacks.

### Cluster pub/sub (`redis-tower-cluster/pubsub.rs`)

`ClusterPubSubConnection` and `ShardedClusterPubSubConnection` (SSUBSCRIBE /
SPUBLISH). Both reconnect on connection loss and resubscribe.

## Middleware Layers (Tower)

All live in `redis-tower/src/`:
- `reconnect_layer.rs` / `reconnect.rs` -- `ConnectionFactory`-based reconnect with exponential backoff + jitter; the `ResilientConnection` success log carries `elapsed_ms` (total time from connection loss to reconnect, threaded through every attempt)
- `auto_pipeline.rs` -- `AutoPipelineService`: batches concurrent calls; bounded queue with real back-pressure (`poll_ready` awaits capacity via `PollSender`), opt-in `QueueFull` load-shedding (`AutoPipelineConfig::shed_load_on_full`)
- `tracing_layer.rs` -- span per command with OTel DB semconv fields (`db.system`, `db.statement`, `server.address`). Separately, `redis-tower-core`'s connectors emit a `redis.connect` span (fields `server.address`, `tls`, plus `server.tls.hostname` for TLS) around every transport connect, so connection setup is observable even without the command layer.
- `metrics_layer.rs` -- `MetricsRecorder` hook with `ErrorKind` enum (7 variants, not just `bool`)
- `cache_layer.rs` / `caching.rs` -- cloneable standalone client-side caching;
  broadcast/server-default/opt-in tracking, per-key epochs, local write
  invalidation, bounded TTL/capacity/statistics, and cache-disable/reconnect
  lifecycle
- `circuit_breaker.rs` -- Redis-aware adapter over `tower-resilience-circuitbreaker`; connection/timeout classifier, shared state handle, deprecated legacy aliases
- `command_timeout.rs` -- `CommandTimeoutLayer`: per-command deadline
- `retry.rs` -- `RetryLayer`/`RetryService`: idempotent-aware automatic retries at the **command altitude** (needs `Command::idempotent`, so it sits above the frame lowering). Default policy `idempotent && err.is_retryable()`, configurable attempt budget and exponential backoff + jitter (`RetryPolicy`). Opt in on a client via `.retry(policy)`, which returns a `RetryClient` bridging through `ExecutorService`. A non-idempotent write is never re-sent, so a retry cannot silently duplicate data.
- `resilient.rs` -- `ResilientRedisClient` combining reconnect + auto-pipeline

## Command Groups

`redis-tower-commands/src/` -- one file per group:

`array`, `strings`, `keys`, `hashes`, `lists`, `sets`, `sorted_sets`, `bitmap`, `geo`, `hyperloglog`, `streams`, `pubsub`, `scan`, `scripting`, `blocking`, `server`, `diagnostics`, `acl`, `cluster`, `transaction`, `raw`, `search`, `search_util`, `json`, `bloom`, `sketch`, `tdigest`, `timeseries`, `vector_sets`

Redis Stack commands (`json`, `search`, `bloom`, `sketch`, `tdigest`, `timeseries`, `vector_sets`) are behind feature flags, all enabled by default via `commands-stack`.

Notable additions since initial audit: `transaction` module (MULTI/EXEC/DISCARD/WATCH/UNWATCH), HMGET, LPOP/RPOP count variants, ZDiff/ZUnion/ZInter, EXPIREAT/PTTL, HELLO, EVAL_RO/EVALSHA_RO, ZAdd flags (NX/XX/GT/LT/CH/INCR), Expire condition flags (Redis 7.0), CLIENT subcommands, Redis 8.x DELEX/DIGEST/MSETEX/INCREX/XCFGSET/XIDMPRECORD/XNACK/HOTKEYS/FT.HYBRID/FT.EXPLAIN/FT.EXPLAINCLI/VISMEMBER/VRANGE builders, the complete Redis 8.8 `AR*` Array command family, the server/operations sweep (MIGRATE, MODULE, MEMORY, LATENCY, ACL, FUNCTION, COMMAND, and MONITOR), and the cluster admin sweep (SETSLOT, ADDSLOTS/DELSLOTS + RANGE variants, REPLICAS/SLAVES, LINKS, SET-CONFIG-EPOCH, BUMPEPOCH, FLUSHSLOTS, SAVECONFIG). Search, Vector Set, and Array now have typed builders for every scoped command name; generated coverage lives in `COMMAND_COVERAGE.md`.

## Module Clients (`redis-tower-modules`)

High-level ergonomic clients for Redis Stack modules. Feature-gated; all enabled by default via `full`.

| Client | Feature | Description |
|--------|---------|-------------|
| `JsonClient<C>` | `json` | Typed serde get/set/merge/arr/obj; requires `serde` |
| `SearchClient<C>` | `search` | Index lifecycle, `SearchQuery` builder, typed `SearchResults<T>` |
| `TimeSeriesClient<C>` | `timeseries` | `TsSample`, `TsLabel`, range/mrange queries |
| `BloomFilter<C>`, `CuckooFilter<C>`, `CountMinSketch<C>`, `TopK<C>`, `TDigest<C>` | `probabilistic` | Key-bound ergonomic wrappers with typed `*Info` structs |
| `VectorSetClient<C>` | `vector` | KNN search, `SimilarityResult`, VADD/VREM/VSIM |

The old `Json<>` and `Search` prototypes in `redis-tower` are deprecated aliases — use `redis-tower-modules` instead.

## Distributed Primitives (`redis-tower-primitives`)

Redisson-style coordination primitives, minus the magic. Each is generic over a
`RedisExecutor` and owns no background threads unless you ask for one.

| Type | File | Notes |
|------|------|-------|
| `DistributedLock` | `lock.rs` | Lease-based mutex; `LockLease`, `LockRenewalHandle`, `RenewalOutcome` |
| `GcraRateLimiter` | `rate_limiter.rs` | GCRA (leaky-bucket) limiter returning `RateLimitDecision` |
| `LeaderElection` | `leader.rs` | `Campaign`, `Leadership`, `LeadershipEvents` stream, `LeadershipOutcome` |
| `ExpirableSemaphore` | `semaphore.rs` | Counted permits with TTL; `SemaphorePermit` |
| `CountDownLatch` | `latch.rs` | `LatchCountDown`, `LatchWaitOutcome`, `LatchWaitError` |
| `DelayedQueue` | `delayed_queue.rs` | Scheduled delivery; `ClaimBatch`, `DelayedQueueError` |
| `IdGenerator` | `id_generator.rs` | Block-allocating unique IDs; `IdBlock` |

Live integration tests are in `crates/redis-tower-primitives/tests/live.rs`.
See `docs/PRIMITIVES.md` for the usage guide.

## Cloud Auth

`redis-tower/src/credentials.rs` defines `CredentialProvider` (and
`StreamingCredentialProvider` for providers that push rotations). Managed-service
providers live in their own crates so the core does not carry cloud SDKs:

| Crate | Type | Notes |
|-------|------|-------|
| `redis-tower-auth-aws` | `ElastiCacheIamProvider` | IAM token auth; `ElastiCacheResourceType` selects cluster vs serverless |
| `redis-tower-auth-azure` | `EntraIdProvider` | Azure Entra ID token auth |

Tokens expire, so these pair with the provider-backed connection factory (#670)
that re-authenticates on reconnect. See `docs/CLOUD-AUTH.md` and
`docs/SERVERLESS.md`.

## Test Infrastructure

### Standalone tests (`crates/redis-tower/tests/`)

`common/mod.rs` starts `redis-server` on port **6399** via `redis-server-wrapper`. Set `REDIS_URL` env var to use an external server instead.

```bash
cargo test --test test_strings --all-features
cargo test --test '*' --all-features   # all standalone integration tests
```

Test files: `integration.rs`, `client_side_caching_v2.rs`, `redis_8x_commands.rs`, `test_acl.rs`, `test_auth.rs`, `test_bitmap.rs`, `test_bloom.rs`, `test_errors.rs`, `test_exotic.rs`, `test_geo.rs`, `test_hashes.rs`, `test_hyperloglog.rs`, `test_infrastructure.rs`, `test_keys.rs`, `test_large_values.rs`, `test_lists.rs`, `test_maintenance.rs`, `test_monitor.rs`, `test_object.rs`, `test_pool.rs`, `test_resilience_integration.rs`, `test_scan_stream.rs`, `test_scripting.rs`, `test_server.rs`, `test_sets.rs`, `test_sorted_sets.rs`, `test_streams.rs`, `test_strings.rs`

The admin-command fixtures stay in this tier rather than requiring Docker.
`test_acl.rs` provisions an `aclfile`-backed Redis process and verifies the
`ACL SAVE`/`ACL LOAD` persistence round trip. `test_server.rs` uses dedicated
primary and replica processes to exercise `REPLICAOF` and coordinated
`FAILOVER`. Both run in the normal per-PR standalone integration job.

### Cluster tests (`crates/redis-tower-cluster/tests/`)

Starts a 3-master cluster. **Ports 17200-17202** (plain), **17300-17302** (auth), **17400-17402** (TLS). Avoids 7000 which conflicts with macOS Control Center.

```bash
cargo test -p redis-tower-cluster --test cluster_integration -- --ignored
```

Must run **single-threaded** (`-- --ignored`, no `--test-threads`). Tests are `#[ignore]` -- they won't run in the normal `cargo test` pass.

### Sentinel tests (`crates/redis-tower-sentinel/tests/`)

Two binaries. `sentinel_integration.rs` (healthy suite) starts master on **6390**, 2 replicas on **6391-6392**, 3 sentinels on **26389-26391**, quorum 2. `sentinel_failover.rs` (destructive suite) starts its own topology on a separate port block: master **6393**, replicas **6394-6395**, sentinels **26392-26394**.

```bash
cargo test -p redis-tower-sentinel --test 'sentinel_*' -- --ignored
```

Also single-threaded. The healthy suite shares a topology via `OnceCell` but never kills it, so its tests are robust to reordering and parallel execution. The destructive phases (kill a sentinel, fail the master over, reconnect afterward) live in `sentinel_failover.rs` as a single orchestrating `sentinel_failover_sequence` test on the separate port block, so they no longer degrade the healthy topology and their internal order is fixed regardless of how the runner schedules tests (#509).

### Criterion benches (`crates/redis-tower/benches/`)

`commands.rs` starts its own `redis-server` on port **6482** and stops it when the run ends, so it needs no server set up in advance. Set `REDIS_URL` to run against an existing one instead.

```bash
cargo bench -p redis-tower --bench commands
cargo test -p redis-tower --benches --all-features   # criterion test mode: one iteration each, also runs in CI
```

### `command_tests!` macro (`redis-tower-test`)

Generates a suite of cross-backend tests (strings, hashes, lists, sets, sorted sets, bitmap, geo, HyperLogLog, streams). Used in standalone, cluster, and sentinel test files. **SCAN is intentionally excluded** from the macro -- SCAN is not cluster-compatible (only scans one node). For a cluster-wide scan use `ScanClusterStream` (see below); the macro still excludes SCAN because it asserts identical behavior across backends and a per-node SCAN does not have any.

### Benchmark and chaos crates

Five internal bench crates plus a Docker-tier chaos suite, none published:

| Crate | Purpose |
|-------|---------|
| `standalone-bench` | redis-rs and fred comparison benchmarks |
| `cluster-bench` | criterion benchmarks for cluster clients |
| `sentinel-bench` | sentinel client benchmarks |
| `soak-bench` | long-running soak and chaos harness; backs the Soak Harness Smoke workflow |
| `resource-bench` | memory and connection footprint comparisons |
| `redis-chaos-tests` | Docker-tier only: image-based version/Stack/Valkey matrices and true network partitions |

Benchmark evidence is generated and contract-checked in CI. Regression gates run
via `scripts/check_criterion_regressions.py`; the reporting scripts under
`scripts/` each have a paired `test_*.py` unit test that also runs in CI.

## Definition of Done

An issue is **not** done when the code compiles. Every issue -- including ones
dispatched to agents -- must ship, in the same PR:

- **Tests**: unit and/or integration as appropriate to the change. New behavior
  gets a test that would fail without it. Mechanical changes (e.g. bulk derives)
  are covered by a clean `--all-features` build plus at least one assertion test
  demonstrating the intent.
- **Documentation**: doc comments on any new public surface, and updates to the
  relevant guide/README/CLAUDE.md where behavior or usage changes.

A PR with code but no tests or docs is incomplete, not a follow-up.

## Pre-commit Checklist

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib --all-features
cargo test --test '*' --all-features
```

## CI

Nine workflows. Four fire on every PR and contribute **22 required checks**:

| Workflow | Checks |
|----------|--------|
| `ci.yml` | Format, Clippy, Documentation, MSRV (1.88), Feature Checks, Unit Tests (stable), Unit Tests (beta), Integration Tests (Redis 7.4.3), Integration Tests (Redis 8.0.6), Coverage, Command Coverage, Fuzz Targets, Codec Benchmark Regression, Benchmark Evidence Contract |
| `hygiene.yml` | Release hygiene, Public API compatibility, macOS arm64, Windows x64, Linux arm64 |
| `supply-chain.yml` | cargo-audit, cargo-deny |
| `soak-smoke.yml` | Standalone and cluster chaos (path-filtered) |

Five more jobs appear on the PR as `SKIPPED` by design -- the four mutation jobs
and CI wall-clock and flake budget are gated on
`github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'`.
They are not gaps.

The remaining workflows do not run on PRs: `docs.yml` (mdBook build plus GitHub
Pages deploy, `push: main` only), `nightly.yml`, `nightly-modules.yml`, and
`release-plz.yml` (manual dispatch).

`docs.yml` only *deploys*. The documentation content is verified on every PR by
CI's Documentation job, which runs `cargo doc` with `RUSTDOCFLAGS: -D warnings`,
`mdbook build`, `mdbook test`, and both link checkers. Its absence from a PR's
check list is expected.

Coverage uses cargo-llvm-cov with `--no-report` accumulation across the
unit/doc/standalone/cluster/sentinel runs, then uploads an lcov report to Codecov
(informational, not a hard gate).

The version-gated Redis 8.x live-command suite runs nightly against every Redis
minor in the 8.0, 8.2, 8.4, 8.6, and 8.8 matrix.

Merges are manual -- GitHub auto-merge is **not** enabled (`gh pr merge --auto` is rejected for this repo). Merge with `gh pr merge --squash`; merged head branches are auto-deleted.

## Executor Model

`RedisExecutor` trait uses `&mut self`. Key impls:
- `RedisConnection` -- direct `&mut self` access
- `Arc<Mutex<C: RedisExecutor>>` -- blanket impl; locks to get `&mut C` (enables `RedisClient` to satisfy the trait)
- `MultiplexedClient` -- direct impl; `&mut self` is the trait contract, internally uses `&self` channel send
- `Pipeline::execute` and `Transaction::execute` accept `&mut impl PipelineExecutor` / `&mut impl TransactionExecutor` (separate traits with impls for `RedisConnection`, `Arc<Mutex<C>>`, `RedisClient`)

## Known Quirks

- **`handle.master_addr()` is static** -- `RedisSentinelHandle::master_addr()` returns the original master address from the struct, not the dynamically elected master after a failover. Use `handle.poke()` to query the sentinel for the current elected master post-failover.
- **`OBJECT ENCODING` response** -- returns `SimpleString`, not `BulkString`. Both must be handled in `parse_response`.
- **`BLMove` timeout response** -- Redis 7.4+ returns `Frame::Array(None)` on a blocking timeout for BLMOVE (not `Frame::Null`). Fixed in `blocking.rs`.
- **`Cargo.lock` is gitignored** -- this is a library workspace, so the lock is
  not committed and CI always resolves fresh. A long-lived working copy can hold
  a stale lock that CI never sees. The known symptom is `cargo clippy` failing in
  `redis-tower-test` with `no method named cluster_base` on
  `redis_server_wrapper::RedisClusterHandle`: the workspace requires
  `redis-server-wrapper = "0.4.1"`, `cluster_base()` landed in 0.4.3, and a lock
  pinned at 0.4.1 satisfies the requirement without providing the method. Fix
  with `cargo update -p redis-server-wrapper`. Reach for `cargo update` before
  assuming a local-only build failure is a real regression.
- **Let-chains** -- MSRV is 1.88; clippy will suggest let-chains and they are valid.
- **`FunctionFlush` ordering** -- global operation; tests using it should run with `--test-threads=1` to avoid interfering with function-load tests.
- **Sentinel failover sim is destructive** -- the failover phases kill processes in their topology (a sentinel, then the master). As of #509 they live in their own binary (`sentinel_failover.rs`) on a separate port block, wrapped in a single `sentinel_failover_sequence` test that fixes their order, so they no longer degrade the healthy `sentinel_integration` suite and the sentinel tests are robust to parallel and reordered execution.
- **`idempotent()` on `Command` trait** -- defaults to `false`. Read-only commands override to `true`. `ReconnectService` will not retry non-idempotent commands on `ConnectionClosed` to prevent silent data duplication.
- **RESP3 changes response frame shapes** -- as of #478 `connect()`/`connect_url` (and siblings) negotiate RESP3 by default (Auto + `HELLO 3`, RESP2 fallback). RESP3 swaps several wire types vs RESP2, so any command's `parse_response` that touches them must accept BOTH: map-shaped replies arrive as `Frame::Map(pairs)` instead of a flat `Frame::Array` (FUNCTION STATS, COMMAND DOCS, XINFO STREAM/GROUPS/CONSUMERS, XREAD/XREADGROUP), and human-readable text arrives as `Frame::VerbatimString(format, data)` instead of `Frame::BulkString` (INFO, CLIENT INFO, CLIENT LIST, MEMORY DOCTOR, LOLWUT). The fix pattern is to add the RESP3 arm alongside the RESP2 one (flatten maps to the `[k,v,...]` array shape; treat verbatim like bulk). `standalone_cmd` in `integration.rs` is pinned to RESP2 via `connect_with_protocol(.., Resp2)` so the suite still exercises both wire formats; every other standalone test runs RESP3 through `conn()`.

## Current Status

**The issue queue is empty: 0 open, 376 closed.** Every audit pass, the
architecture/bug/feature queues, and the Go-Hard Backlog below have been worked
to completion. There is no standing backlog to pull from; new work starts by
filing an issue.

The last major tranche (roughly #654-#689) added distributed primitives, cloud
auth, adaptive replica routing, topology-aware client-side caching, reconnecting
cluster pub/sub, maintenance push-notification handling, explicit pool lifecycle
controls, and an evidence-generation layer under `scripts/` (command coverage,
test conformance, mutation score, CI health, benchmark contracts) with unit tests
for each reporter.

Guides live in `docs/`: `MIGRATING-FROM-REDIS-RS.md`, `MIGRATING-FROM-FRED.md`,
`PRODUCTION-TUNING.md`, `PRIMITIVES.md`, `CLOUD-AUTH.md`, `SERVERLESS.md`,
`CLIENT-SIDE-CACHING.md`, `POOL-HEALTH-PROBING.md`, `FEATURE-MATRIX.md`,
`TEST-CONFORMANCE.md`, `ENGINEERING-HYGIENE.md`. `COMMAND_COVERAGE.md` at the
repo root is generated.

**Every per-file test suite runs in CI.** The standalone integration job runs
`cargo test -p redis-tower --test '*' -- --test-threads=1` (all `tests/*.rs`
suites, single-threaded for the `FunctionFlush` quirk above). Module-client
integration tests remain `#[ignore]` and need Redis Stack (`-- --ignored`).

**What's been hardened:**
- Circuit breaker, command/connect timeouts, pool acquisition timeout
- TCP keepalive, reconnect backoff jitter, graceful shutdown across `MultiplexedClient`, `MultiplexedClusterClient::shutdown()`, and `ConnectionPool::close()` (the SIGTERM drain path)
- Non-idempotent write retry guard, structured reconnect/MOVED/ASK/failover logs
- Dead pool connection replacement after health check failure
- Cluster MOVED/ASK refresh, CROSSSLOT errors, eager sentinel rediscovery on failover

## Release State

**Nothing is currently published.** crates.io holds only `redis-tower` 0.1.0 and
siblings, all yanked on 2026-06-11. A yanked version can never be reused, so the
re-launch must bump.

**PR #690 is the release vehicle.** It is open, ready for review, and green
(22/22 checks). It covers 73 files and:

| Package group | Version |
|---|---:|
| `redis-tower-protocol` | 0.1.2 |
| `redis-tower-core`, `redis-tower-commands`, `redis-tower`, `redis-tower-cluster`, `redis-tower-sentinel` | 0.1.1 |
| `redis-tower-sync`, `redis-tower-modules`, `redis-tower-client`, `redis-tower-primitives`, `redis-tower-auth-aws`, `redis-tower-auth-azure`, `redis-tower-test` | 0.1.0, first publication |

It also adds changelogs for all 13 publishable crates and enforces them in
release hygiene, rewrites the README, removes `roba.toml`, and makes release-plz
manual, operation-specific, serialized, and unavailable in forks.

**Publication order** (each tier must be on crates.io before the next resolves):

1. `redis-tower-protocol`
2. `redis-tower-core`
3. `redis-tower-commands` and `redis-tower-test`
4. `redis-tower`
5. cluster, sentinel, modules, primitives, sync, cloud-auth
6. `redis-tower-client`

**Known limit on #690's evidence.** release-plz cannot dry-run the downstream
crates until each upstream tier is actually published, so the whole-workspace
dry-run stops at `redis-tower-core` by design. The remaining tiers are covered by
the staged-publication procedure and the partial-release recovery steps in the
release guide, not by a full rehearsal. Treat that as a documented constraint,
not a missing check.

**Release workflow is manual-dispatch only** (PR #410): the `push: main` trigger
was removed so merges never auto-publish. Dispatch with
`gh workflow run Release --ref main`. Do not restore the `push: main` trigger
without an explicit decision to resume auto-publish.

`CARGO_REGISTRY_TOKEN` must be set in repo secrets; `GITHUB_TOKEN` comes from
Actions.

## History: the Go-Hard Backlog

Closed out. On 2026-06-11 three competitive-analysis passes filed 107 issues
(customer axes vs redis-rs/fred; verifiable dimensions across testing, perf, and
command coverage; and a "great Redis client in 2026" study including a
Redisson-minus-magic primitives review). Those issues, and the audit passes
before them, are all closed. The label taxonomy they introduced is still in use
for new work: `kind:` (architecture/bug/feature), `priority:` (high/medium/low),
and `area:` (cluster, resilience, observability, client-caching, commands,
performance, testing, tower, pubsub, transactions, documentation).

**Test architecture decision (still current).** Per-PR tests run on
`redis-server-wrapper` processes, which provide chaos kill/freeze/failover, ACL
files, `replicaof`, and full TLS. The Docker tier (`redis-chaos-tests`) covers
only what processes cannot: image-based version/Stack/Valkey matrices and true
network partitions. `ACL SAVE`/`ACL LOAD` and `REPLICAOF`/`FAILOVER` live in the
process tier.
