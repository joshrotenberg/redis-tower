# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/joshrotenberg/redis-tower/releases/tag/redis-tower-core-v0.1.0) - 2026-06-11

### Added

- RedisError::Json variant and From<serde_json::Error> impl (closes #308) ([#385](https://github.com/joshrotenberg/redis-tower/pull/385))
- blanket RedisConvert impls for Vec<T: FromRedisBytes> and HashMap<String, V> (closes #296) ([#383](https://github.com/joshrotenberg/redis-tower/pull/383))
- connect timeout in ConnectionConfig, CommandTimeoutLayer Tower middleware (closes #300) ([#374](https://github.com/joshrotenberg/redis-tower/pull/374))
- CircuitBreakerLayer Tower middleware (closes #295) ([#363](https://github.com/joshrotenberg/redis-tower/pull/363))
- PipelineResults has_errors/into_typed_vec, TransactionResult committed/aborted helpers (closes #318, closes #304) ([#361](https://github.com/joshrotenberg/redis-tower/pull/361))
- CLIENT SETINFO, credential rotation, cluster routing strategies ([#233](https://github.com/joshrotenberg/redis-tower/pull/233))
- final audit round -- cluster NAT, TLS flex, pool health, 87 new tests, CI ([#226](https://github.com/joshrotenberg/redis-tower/pull/226))
- add LeastConnections dispatch strategy and fix CI issues
- add type conversion traits for Redis response values
- add error classification methods for retry policies
- Tower integration improvements (#103-#107)
- implement Service<Cmd> for ClusterConnection and SentinelConnection
- add FrameService, CommandAdapter, and CacheLayer for Tower-native CSC
- add RESP3 push infrastructure and client-side caching
- add RESP3 protocol support and test matrix
- add connection resilience (auto-reconnect, ResilientRedisClient)
- add TLS support (native-tls and rustls backends)
- [**breaking**] v2 rewrite -- workspace scaffold with core features
- add comprehensive TLS support for Redis connections

### Fixed

- structured reconnect/redirect/failover logs, non-idempotent retry guard, middleware unit tests (closes #303, #306, #340) ([#369](https://github.com/joshrotenberg/redis-tower/pull/369))
- TCP keepalive and reconnect backoff jitter (closes #331, #335) ([#359](https://github.com/joshrotenberg/redis-tower/pull/359))
- audit round 2 -- PubSub, RawCommand, errors, SCAN stream ([#225](https://github.com/joshrotenberg/redis-tower/pull/225))
- address 5 pre-release critical bugs ([#224](https://github.com/joshrotenberg/redis-tower/pull/224))
- [**breaking**] honor Tower backpressure contract in poll_ready
- resolve rustdoc broken intra-doc link warnings

### Other

- thread-safety docs, missing examples, #[deny(missing_docs)] (closes #337, #341, #343) ([#370](https://github.com/joshrotenberg/redis-tower/pull/370))
- surface MultiplexedClusterClient, add benchmarks, fix flaky tests ([#243](https://github.com/joshrotenberg/redis-tower/pull/243))
- clean up README, move examples, add licenses, remove stale files ([#204](https://github.com/joshrotenberg/redis-tower/pull/204))
- add tower-resilience integration guide and example
- comprehensive documentation rewrite
- fmt
- add feature gates for Redis Stack modules and 274 unit tests
- add crate metadata, module docs, and 10 runnable examples
- format
- add README with full API overview
- apply cargo fmt
- polish phase for v0.1.0 release
- initialize redis-tower experimental project skeleton
