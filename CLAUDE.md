# redis-tower

A Tower-based Redis client with strong typing, composable middleware, and resilience primitives. Private GitHub repo.

## Architecture

Workspace of 9 crates with a clear dependency direction:

```
redis-tower-protocol   (RESP2/3 codec, frame types)
redis-tower-core       (RedisConnection, RedisError, Command trait, TLS)
redis-tower-commands   (typed command builders -- one file per command group)
redis-tower            (clients, middleware layers, pool, pipeline, pub/sub)
  redis-tower-cluster  (cluster topology, routing, MOVED/ASK handling)
  redis-tower-sentinel (sentinel discovery, failover)
  redis-tower-sync     (blocking wrapper around MultiplexedClient)
redis-test-harness     (test utilities: MockConnection, command_tests! macro)
cluster-bench          (criterion benchmarks for cluster clients)
```

`redis-tower-cluster` and `redis-tower-sentinel` both depend on `redis-tower`.

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
- `reconnect_layer.rs` / `reconnect.rs` -- `ConnectionFactory`-based reconnect with backoff
- `auto_pipeline.rs` -- `AutoPipelineService`: batches concurrent calls into pipelined requests
- `tracing_layer.rs` -- span per command
- `metrics_layer.rs` -- counter/histogram hooks
- `cache_layer.rs` / `caching.rs` -- client-side caching
- `resilient.rs` -- `ResilientRedisClient` combining reconnect + auto-pipeline

## Command Groups

`redis-tower-commands/src/` -- one file per group:

`strings`, `keys`, `hashes`, `lists`, `sets`, `sorted_sets`, `bitmap`, `geo`, `hyperloglog`, `streams`, `pubsub`, `scan`, `scripting`, `blocking`, `server`, `diagnostics`, `acl`, `cluster`, `raw`, `search`, `search_util`, `json`, `bloom`, `sketch`, `tdigest`, `timeseries`, `vector_sets`

Redis Stack commands (`json`, `search`, `bloom`, `sketch`, `tdigest`, `timeseries`, `vector_sets`) are behind feature flags, all enabled by default via `commands-stack`.

## Test Infrastructure

### Standalone tests (`crates/redis-tower/tests/`)

`common/mod.rs` starts `redis-server` on port **6399** via `redis-server-wrapper`. Set `REDIS_URL` env var to use an external server instead.

```bash
cargo test --test test_strings --all-features
cargo test --test '*' --all-features   # all standalone integration tests
```

Test files: `integration.rs`, `test_acl.rs`, `test_bitmap.rs`, `test_geo.rs`, `test_hashes.rs`, `test_hyperloglog.rs`, `test_infrastructure.rs`, `test_keys.rs`, `test_lists.rs`, `test_object.rs`, `test_scan_stream.rs`, `test_scripting.rs`, `test_server.rs`, `test_sets.rs`, `test_sorted_sets.rs`, `test_strings.rs`

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

Auto-merge on green is enabled (squash merge, branch deleted).

## Known Quirks

- **`handle.master_addr()` is static** -- `RedisSentinelHandle::master_addr()` returns the original master address from the struct, not the dynamically elected master after a failover. Use `handle.poke()` to query the sentinel for the current elected master post-failover.
- **`OBJECT ENCODING` response** -- returns `SimpleString`, not `BulkString`. Both must be handled in `parse_response`.
- **`BLMove` timeout response** -- Redis 7.4+ returns `Frame::Array(None)` on a blocking timeout for BLMOVE (not `Frame::Null`). Fixed in `blocking.rs`.
- **Let-chains** -- MSRV is 1.88; clippy will suggest let-chains and they are valid.
- **`FunctionFlush` ordering** -- global operation; tests using it should run with `--test-threads=1` to avoid interfering with function-load tests.

## Current Status

All planned issues from the initial audit (#249-#267, #282-#283) are closed. No open issues or PRs.

The codebase has full integration test coverage across all command groups, all three client topologies (standalone, cluster, sentinel), and all three client variants (connection, client, multiplexed). The `ConnectionPool` is exercised with all backing connection types.

## What Is Not Yet Done

- Sentinel failover simulation test runs but kills the shared topology -- run it in isolation
- `ObjectFreq` integration test -- requires LFU `maxmemory-policy`, not tested
- `ACL SAVE`/`ACL LOAD` -- require a Redis server started with an ACL file
- `REPLICAOF`, `FAILOVER` -- require multi-server setups
- `ConnectionPool` exhaustion behavior -- tested at single-connection level but not under sustained load
