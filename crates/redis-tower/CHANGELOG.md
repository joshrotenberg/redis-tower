# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- opt-in Redis Smart Client Handoff maintenance handling for factory-backed
  `MultiplexedClient`, with required RESP3 registration, half-TTL `MOVING`
  replacement through the original factory, observational `MIGRATING` events,
  queued-work gating, and an owned shutdown handle (#498)
- reconnect-with-backoff support for dedicated Pub/Sub connections, with
  subscription replay completed before a replacement becomes visible
- provider-backed `CredentialConnectionFactory` for resilient, multiplexed,
  and retained-pool connections, with per-connection credential fetch,
  authentication before requested protocol negotiation, and one bounded
  provider refresh when connection-establishment authentication is rejected
  (#475)
- explicit `Pipeline` execution through the standard `MultiplexedClient`, sent
  as one ordered, cancellation-aware auto-pipeline batch
- `WithDeadline<Cmd>` end-to-end absolute deadlines honored by deadline-aware
  command timeout middleware, typed retries, multiplexed dispatch, resilient
  offline queueing and replay, circuit breakers, and pool acquisition and I/O
- an opt-in bounded offline queue for `ResilientRedisClient`, with ticket-order
  replay restricted to idempotent typed commands, cancellation-safe connection
  quarantine, a finite per-command replay budget, and one shared structured
  reconnect-exhaustion cause for every released waiter
- bounded, clone-friendly connection lifecycle event streams for connect,
  disconnect, reconnect, exhaustion, intentional shutdown, and
  topology-confirmed failover events
- a dedicated, binary-safe `MonitorStream` over an exclusively owned connection, plus the complete server and operations command sweep
- complete Redis 8.8 Array command family with version-gated live coverage and cluster-aware read routing
- Redis-aware `tower-resilience` circuit breaker adapter, operational handle, and high-level client options
- version-gated live command coverage across Redis 8.0, 8.2, 8.4, 8.6, and 8.8
- tier-1 process integration coverage for `ACL SAVE`/`ACL LOAD` persistence and `REPLICAOF`/`FAILOVER` transitions (#414, #415)
- metrics-facade recorder, named pool lifecycle and stats metrics, multiplexed queue-depth access/export, and Prometheus/OpenTelemetry examples
- backward-compatible cluster observability hooks on `MetricsRecorder`, with metrics-facade redirect, topology-refresh, and node-latency metrics
- connection-configurable RESP frame-size and nesting limits, retained by shared/multiplexed clients and built-in reconnect factories
- cloneable `CachedMultiplexedClient` with broadcast/server-default/opt-in
  tracking, address/URL/factory/existing-connection constructors, owned
  invalidation reconnection, read-your-own-writes eviction, race-safe epochs,
  bounded TTL/capacity, aggregate statistics, and cache metric events

### Changed

- Pub/Sub confirmation handling now preserves interleaved messages, validates
  confirmation kind and channel, rejects empty subscribe calls before I/O, and
  deduplicates repeated names so the wire remains aligned

- `CachedClient` now uses the same bounded actor and owned tracking lifecycle as
  `CachedMultiplexedClient`, with batching limited to one request for serialized
  compatibility
- closure-based transaction helpers now reject shared and routed executors
  before WATCH; direct `Transaction::watch` remains atomic for already-known
  bodies, while read/compute/build retries require an exclusive connection
- reconnectors consistently treat `max_retries` as retries after the first
  reconnect attempt and apply `connect_timeout` to initial and replacement
  factory calls
- circuit health now counts only connection and timeout failures; Redis command errors such as `WRONGTYPE` no longer trip the breaker

### Deprecated

- `CircuitBreakerLayer`, `CircuitBreakerConfig`, and `CircuitBreakerService` are compatibility aliases for the new `RedisCircuitBreaker*` names

### Fixed

- fail client-side caching closed when either the invalidation receiver or the
  fixed data connection is lost, including idle socket loss, and reject caller
  commands that would mutate the internally managed tracking/session state
- prune canceled auto-pipeline requests before wire dispatch and quarantine
  pooled or shared connections after a deadline interrupts in-flight I/O
- return inner Tower readiness reservations on local cache hits through the
  explicit `ReleaseReadiness` backend contract, preventing bounded services
  from losing capacity
- skip the process-owned restart test when the integration suite targets an externally managed Redis server

## [0.1.0](https://github.com/joshrotenberg/redis-tower/releases/tag/redis-tower-v0.1.0) - 2026-06-05

### Added

- RedisError::Json variant and From<serde_json::Error> impl (closes #308) ([#385](https://github.com/joshrotenberg/redis-tower/pull/385))
- redis-tower-modules crate scaffold with feature-gated module client stubs (closes #312) ([#376](https://github.com/joshrotenberg/redis-tower/pull/376))
- connect timeout in ConnectionConfig, CommandTimeoutLayer Tower middleware (closes #300) ([#374](https://github.com/joshrotenberg/redis-tower/pull/374))
- Arc<Mutex<C>> blanket impl for RedisExecutor, Pipeline/Transaction accept any executor, Json<C> works with MultiplexedClient (closes #291, #314) ([#375](https://github.com/joshrotenberg/redis-tower/pull/375))
- metrics error dimensions, OTel semconv spans, pool stats API, health_check (closes #293, #297, #307, #315) ([#366](https://github.com/joshrotenberg/redis-tower/pull/366))
- CircuitBreakerLayer Tower middleware (closes #295) ([#363](https://github.com/joshrotenberg/redis-tower/pull/363))
- PipelineResults has_errors/into_typed_vec, TransactionResult committed/aborted helpers (closes #318, closes #304) ([#361](https://github.com/joshrotenberg/redis-tower/pull/361))
- MultiplexedClient graceful shutdown (closes #311) ([#360](https://github.com/joshrotenberg/redis-tower/pull/360))
- MultiplexedClusterClient for redis-tower-cluster ([#235](https://github.com/joshrotenberg/redis-tower/pull/235))
- CLIENT SETINFO, credential rotation, cluster routing strategies ([#233](https://github.com/joshrotenberg/redis-tower/pull/233))
- add MultiplexedClient and optimize auto-pipeline ([#228](https://github.com/joshrotenberg/redis-tower/pull/228))
- final audit round -- cluster NAT, TLS flex, pool health, 87 new tests, CI ([#226](https://github.com/joshrotenberg/redis-tower/pull/226))
- add high-level JSON and Search APIs with serde integration
- add LeastConnections dispatch strategy and fix CI issues
- add Tower-native connection pool with round-robin and random dispatch
- add auto-pipelining service and managed stream consumer
- add TracingLayer, MetricsLayer, Script helper, and RedisExecutor trait
- add type conversion traits for Redis response values
- add error classification methods for retry policies
- add blocking commands (BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX)
- add SCAN family commands (SCAN, SSCAN, HSCAN, ZSCAN)
- *(commands)* add streams commands (XCLAIM, XAUTOCLAIM, XPENDING, XINFO, XGROUP SETID/CREATECONSUMER/DELCONSUMER) closes #111
- *(commands)* add sharded pub/sub and hash field expiration commands closes #119
- add Redis Streams commands
- Tower integration improvements (#103-#107)
- add FrameService, CommandAdapter, and CacheLayer for Tower-native CSC
- add RESP3 push infrastructure and client-side caching
- add RESP3 protocol support and test matrix
- shared command test macro for standalone/cluster matrix
- add criterion benchmarking suite
- add connection resilience (auto-reconnect, ResilientRedisClient)
- add remaining commands from #54
- add TLS support (native-tls and rustls backends)
- update CI for v2 workspace
- use docker-wrapper for integration test infrastructure
- add hash, list, set, and sorted set commands
- [**breaking**] v2 rewrite -- workspace scaffold with core features
- add comprehensive TLS support for Redis connections

### Fixed

- structured reconnect/redirect/failover logs, non-idempotent retry guard, middleware unit tests (closes #303, #306, #340) ([#369](https://github.com/joshrotenberg/redis-tower/pull/369))
- TCP keepalive and reconnect backoff jitter (closes #331, #335) ([#359](https://github.com/joshrotenberg/redis-tower/pull/359))
- ConnectionPool health check replaces dead connections, from_connections uses PoolConfig (closes #329, #339) ([#357](https://github.com/joshrotenberg/redis-tower/pull/357))
- remove duplicate ZMPOP test functions in test_sorted_sets.rs ([#284](https://github.com/joshrotenberg/redis-tower/pull/284))
- audit round 2 -- PubSub, RawCommand, errors, SCAN stream ([#225](https://github.com/joshrotenberg/redis-tower/pull/225))
- address 5 pre-release critical bugs ([#224](https://github.com/joshrotenberg/redis-tower/pull/224))
- [**breaking**] honor Tower backpressure contract in poll_ready
- resolve rustdoc broken intra-doc link warnings

### Other

- integration coverage for ACL DRYRUN and an ObjectFreq LFU fixture (closes #392) ([#405](https://github.com/joshrotenberg/redis-tower/pull/405))
- remove dead todo!() stubs in test_infrastructure.rs (closes #396) ([#404](https://github.com/joshrotenberg/redis-tower/pull/404))
- integration tests for HELLO and SERVER/CLIENT commands ([#403](https://github.com/joshrotenberg/redis-tower/pull/403))
- integration tests for EVAL_RO/EVALSHA_RO and XSETID (closes #391) ([#402](https://github.com/joshrotenberg/redis-tower/pull/402))
- circuit breaker + command-timeout failure-injection integration tests ([#398](https://github.com/joshrotenberg/redis-tower/pull/398))
- ScanStream methods accept impl Into<String> for pattern and key (closes #299) ([#384](https://github.com/joshrotenberg/redis-tower/pull/384))
- fix alloc hotpaths, add MultiplexedClient/ConnectionPool benchmarks, redis-rs comparison (closes #301, #309, #320) ([#373](https://github.com/joshrotenberg/redis-tower/pull/373))
- error paths, pool concurrent tests, stream consumer groups, sort/client/hash-expiry coverage (closes #321, #342, #345, #349, #350, #352, #353) ([#372](https://github.com/joshrotenberg/redis-tower/pull/372))
- thread-safety docs, missing examples, #[deny(missing_docs)] (closes #337, #341, #343) ([#370](https://github.com/joshrotenberg/redis-tower/pull/370))
- MultiplexedClient connect_resp3 and SyncClient live-server integration tests (#264, #266, #267) ([#278](https://github.com/joshrotenberg/redis-tower/pull/278))
- ScanStream integration tests ([#283](https://github.com/joshrotenberg/redis-tower/pull/283)) ([#285](https://github.com/joshrotenberg/redis-tower/pull/285))
- Redis Functions integration tests (closes #252) ([#281](https://github.com/joshrotenberg/redis-tower/pull/281))
- SINTERSTORE and SINTERCARD integration tests ([#260](https://github.com/joshrotenberg/redis-tower/pull/260)) ([#279](https://github.com/joshrotenberg/redis-tower/pull/279))
- OBJECT subcommand family integration tests ([#261](https://github.com/joshrotenberg/redis-tower/pull/261)) ([#275](https://github.com/joshrotenberg/redis-tower/pull/275))
- ACL command integration tests ([#256](https://github.com/joshrotenberg/redis-tower/pull/256)) ([#271](https://github.com/joshrotenberg/redis-tower/pull/271))
- BLMOVE integration test ([#263](https://github.com/joshrotenberg/redis-tower/pull/263)) ([#273](https://github.com/joshrotenberg/redis-tower/pull/273))
- DUMP, RESTORE, SORT, LCS, GETSET, MSETNX integration tests ([#253](https://github.com/joshrotenberg/redis-tower/pull/253)) ([#270](https://github.com/joshrotenberg/redis-tower/pull/270))
- GEOSEARCHSTORE integration test ([#258](https://github.com/joshrotenberg/redis-tower/pull/258)) ([#274](https://github.com/joshrotenberg/redis-tower/pull/274))
- LMPOP and ZMPOP integration tests ([#257](https://github.com/joshrotenberg/redis-tower/pull/257)) ([#276](https://github.com/joshrotenberg/redis-tower/pull/276))
- diagnostics and server/admin command integration tests (#254, #255) ([#272](https://github.com/joshrotenberg/redis-tower/pull/272))
- sorted set aggregate and range command integration tests ([#269](https://github.com/joshrotenberg/redis-tower/pull/269))
- replace test harness server lifecycle with redis-server-wrapper ([#248](https://github.com/joshrotenberg/redis-tower/pull/248))
- bump MSRV to 1.88, apply let-chain suggestions ([#247](https://github.com/joshrotenberg/redis-tower/pull/247))
- surface MultiplexedClusterClient, add benchmarks, fix flaky tests ([#243](https://github.com/joshrotenberg/redis-tower/pull/243))
- clean up README, move examples, add licenses, remove stale files ([#204](https://github.com/joshrotenberg/redis-tower/pull/204))
- add tower-resilience integration guide and example
- Merge pull request #202 from joshrotenberg/docs/comprehensive-documentation
- comprehensive documentation rewrite
- cargo fmt
- fmt
- add feature gates for Redis Stack modules and 274 unit tests
- add crate metadata, module docs, and 10 runnable examples
- add integration tests for resilience, caching, pubsub, pipelines, and infra stubs
- Merge pull request #179 from joshrotenberg/fix/poll-ready-backpressure
- format
- split tests into per-category files with shared setup
- add 58 integration tests for untested commands
- add README with full API overview
- migrate harness to use wrapper module internally
- replace docker-wrapper with redis-test-harness for all tests
- improve coverage from 65% to 73%
- polish phase for v0.1.0 release
- initialize redis-tower experimental project skeleton
