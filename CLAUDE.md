# redis-tower

A Tower-based Redis client with strong typing, composable middleware, and resilience primitives. Private GitHub repo.

## Architecture

Workspace of 11 crates with a clear dependency direction:

```
redis-tower-protocol   (RESP2/3 codec, frame types)
redis-tower-core       (RedisConnection, RedisError, Command trait, TLS)
redis-tower-commands   (typed command builders -- one file per command group)
redis-tower            (clients, middleware layers, pool, pipeline, pub/sub)
  redis-tower-cluster  (cluster topology, routing, MOVED/ASK handling)
  redis-tower-sentinel (sentinel discovery, failover)
  redis-tower-sync     (blocking wrapper around MultiplexedClient)
  redis-tower-modules  (high-level module clients: JSON, Search, TimeSeries, Probabilistic, Vector)
redis-test-harness     (test utilities: MockConnection, command_tests! macro)
cluster-bench          (criterion benchmarks for cluster clients)
standalone-bench       (redis-rs comparison benchmarks)
```

`redis-tower-cluster` and `redis-tower-sentinel` both depend on `redis-tower`.
`redis-tower-modules` depends on `redis-tower` and `redis-tower-commands`.

## Key Client Types

| Type | Location | Notes |
|------|----------|-------|
| `RedisConnection` | `redis-tower-core` | Basic single-connection client |
| `RedisClient` | `redis-tower/client.rs` | Arc<Mutex<RedisConnection>>, cloneable |
| `MultiplexedClient` | `redis-tower/multiplexed.rs` | Auto-pipeline, single TCP conn, high concurrency |
| `ConnectionPool<S>` | `redis-tower/pool.rs` | Generic pool; works with any `RedisExecutor` impl |
| `ClusterConnection` | `redis-tower-cluster/connection.rs` | Cluster-aware, MOVED/ASK redirect handling |
| `MultiplexedClusterClient` | `redis-tower-cluster/multiplexed.rs` | Per-node auto-pipeline, no global mutex |
| `SentinelConnection` | `redis-tower-sentinel/connection.rs` | Discovers master via sentinels, auto-rediscovers on failure |
| `SentinelClient` | `redis-tower-sentinel/client.rs` | Arc<Mutex<SentinelConnection>>, cloneable |
| `MultiplexedSentinelClient` | `redis-tower-sentinel/multiplexed.rs` | Auto-pipeline + sentinel discovery, both static and factory-reconnect ctors |
| `SyncClient` | `redis-tower-sync/lib.rs` | Blocking wrapper, uses tokio Runtime internally |

`ConnectionPool<S>` requires `S: RedisExecutor`. Impls exist for `RedisConnection`, `RedisClient`, `ResilientRedisClient`, `CachedClient`, `ClusterConnection`, `SentinelConnection`.

## Middleware Layers (Tower)

All live in `redis-tower/src/`:
- `reconnect_layer.rs` / `reconnect.rs` -- `ConnectionFactory`-based reconnect with exponential backoff + jitter
- `auto_pipeline.rs` -- `AutoPipelineService`: batches concurrent calls; bounded queue with `QueueFull` back-pressure
- `tracing_layer.rs` -- span per command with OTel DB semconv fields (`db.system`, `db.statement`, `server.address`)
- `metrics_layer.rs` -- `MetricsRecorder` hook with `ErrorKind` enum (7 variants, not just `bool`)
- `cache_layer.rs` / `caching.rs` -- client-side caching
- `circuit_breaker.rs` -- `CircuitBreakerLayer`: three-state machine, Arc-shared across clones
- `command_timeout.rs` -- `CommandTimeoutLayer`: per-command deadline
- `resilient.rs` -- `ResilientRedisClient` combining reconnect + auto-pipeline

## Command Groups

`redis-tower-commands/src/` -- one file per group:

`strings`, `keys`, `hashes`, `lists`, `sets`, `sorted_sets`, `bitmap`, `geo`, `hyperloglog`, `streams`, `pubsub`, `scan`, `scripting`, `blocking`, `server`, `diagnostics`, `acl`, `cluster`, `transaction`, `raw`, `search`, `search_util`, `json`, `bloom`, `sketch`, `tdigest`, `timeseries`, `vector_sets`

Redis Stack commands (`json`, `search`, `bloom`, `sketch`, `tdigest`, `timeseries`, `vector_sets`) are behind feature flags, all enabled by default via `commands-stack`.

Notable additions since initial audit: `transaction` module (MULTI/EXEC/DISCARD/WATCH/UNWATCH), HMGET, LPOP/RPOP count variants, ZDiff/ZUnion/ZInter, EXPIREAT/PTTL, HELLO, EVAL_RO/EVALSHA_RO, ZAdd flags (NX/XX/GT/LT/CH/INCR), Expire condition flags (Redis 7.0), CLIENT subcommands.

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

## Test Infrastructure

### Standalone tests (`crates/redis-tower/tests/`)

`common/mod.rs` starts `redis-server` on port **6399** via `redis-server-wrapper`. Set `REDIS_URL` env var to use an external server instead.

```bash
cargo test --test test_strings --all-features
cargo test --test '*' --all-features   # all standalone integration tests
```

Test files: `integration.rs`, `test_acl.rs`, `test_bitmap.rs`, `test_errors.rs`, `test_geo.rs`, `test_hashes.rs`, `test_hyperloglog.rs`, `test_infrastructure.rs`, `test_keys.rs`, `test_lists.rs`, `test_object.rs`, `test_pool.rs`, `test_scan_stream.rs`, `test_scripting.rs`, `test_server.rs`, `test_sets.rs`, `test_sorted_sets.rs`, `test_streams.rs`, `test_strings.rs`

### Cluster tests (`crates/redis-tower-cluster/tests/`)

Starts a 3-master cluster. **Ports 17200-17202** (plain), **17300-17302** (auth), **17400-17402** (TLS). Avoids 7000 which conflicts with macOS Control Center.

```bash
cargo test -p redis-tower-cluster --test cluster_integration -- --ignored
```

Must run **single-threaded** (`-- --ignored`, no `--test-threads`). Tests are `#[ignore]` -- they won't run in the normal `cargo test` pass.

### Sentinel tests (`crates/redis-tower-sentinel/tests/`)

Starts master on **6390**, 2 replicas on **6391-6392**, 3 sentinels on **26389-26391**, quorum 2.

```bash
cargo test -p redis-tower-sentinel --test sentinel_integration -- --ignored
```

Also single-threaded. The sentinel topology is a shared `OnceCell` -- the `sentinel_failover_simulation` test kills the master (kills `pids()[0]`), which degrades the topology for subsequent tests. Run it last or in isolation.

### `command_tests!` macro (`redis-test-harness`)

Generates a suite of cross-backend tests (strings, hashes, lists, sets, sorted sets, bitmap, geo, HyperLogLog, streams). Used in standalone, cluster, and sentinel test files. **SCAN is intentionally excluded** from the macro -- SCAN is not cluster-compatible (only scans one node).

## Pre-commit Checklist

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib --all-features
cargo test --test '*' --all-features
```

## CI

9 checks on every PR: Format, Clippy, Documentation, Unit Tests (stable), Unit Tests (beta), MSRV (1.88), Feature Checks, Integration Tests (Redis 7.4.3), Integration Tests (Redis 8.0.6). All must be green before merge.

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
- **Let-chains** -- MSRV is 1.88; clippy will suggest let-chains and they are valid.
- **`FunctionFlush` ordering** -- global operation; tests using it should run with `--test-threads=1` to avoid interfering with function-load tests.
- **Sentinel failover sim is destructive** -- `sentinel_failover_simulation` kills the shared topology. Run it last or alone; `sentinel_reconnects_after_failover` creates a fresh connection and works correctly after.
- **`idempotent()` on `Command` trait** -- defaults to `false`. Read-only commands override to `true`. `ReconnectService` will not retry non-idempotent commands on `ConnectionClosed` to prevent silent data duplication.

## Current Status

All three audit passes are complete and merged: the initial audit, the second (#289–#353), and a third test-coverage/completeness pass (#390–#396). TimeSeriesClient (#344) and the high-level module clients (JSON/Search/TimeSeries/probabilistic/Vector) all shipped.

**Every per-file test suite now runs in CI.** As of #400 the standalone integration job runs `cargo test -p redis-tower --test '*' -- --test-threads=1` (all `tests/*.rs` suites, not just `integration.rs`; single-threaded for the `FunctionFlush` quirk above). The #390–#396 pass added: live-server circuit-breaker/command-timeout failure injection (`test_resilience_integration.rs`), `#[ignore]` module-client integration tests (need Redis Stack), server/CLIENT command coverage, EVAL_RO/EVALSHA_RO + XSETID, ACL DRYRUN, an `#[ignore]` ObjectFreq LFU fixture, and `command_tests!` applied to `MultiplexedSentinelClient`. Dead `todo!()` infra stubs were removed.

**What's been hardened (since the second audit):**
- Circuit breaker, command/connect timeouts, pool acquisition timeout, AutoPipeline back-pressure
- TCP keepalive, reconnect backoff jitter, graceful `MultiplexedClient` shutdown
- Non-idempotent write retry guard, structured reconnect/MOVED/ASK/failover logs
- Dead pool connection replacement after health check failure
- Cluster MOVED/ASK refresh, CROSSSLOT errors, eager sentinel rediscovery on failover

**What Is Not Yet Done**

- **#399** -- adopt `tower-resilience-circuitbreaker` in place of the custom `CircuitBreakerLayer`. Recommendation posted on the issue (adopt behind a thin error-mapping adapter; its `failure_classifier` fixes the current "any `Err` trips the breaker" gap). Spike not yet started.
- `ACL SAVE`/`ACL LOAD` -- require a Redis server started with an ACL file
- `REPLICAOF`, `FAILOVER` -- require multi-server setups
- Module-client integration tests are `#[ignore]` (require Redis Stack) -- run with `-- --ignored`
