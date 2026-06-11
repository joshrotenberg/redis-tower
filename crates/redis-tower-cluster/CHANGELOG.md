# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/joshrotenberg/redis-tower/releases/tag/redis-tower-cluster-v0.1.0) - 2026-06-11

### Added

- RedisExecutor impls for ClusterConnection and SentinelConnection, ConnectionPool tests ([#259](https://github.com/joshrotenberg/redis-tower/pull/259)) ([#286](https://github.com/joshrotenberg/redis-tower/pull/286))
- TLS for ClusterConnection + automated TLS cluster harness ([#244](https://github.com/joshrotenberg/redis-tower/pull/244))
- TLS support for MultiplexedClusterClient ([#236](https://github.com/joshrotenberg/redis-tower/pull/236))
- MultiplexedClusterClient for redis-tower-cluster ([#235](https://github.com/joshrotenberg/redis-tower/pull/235))
- CLIENT SETINFO, credential rotation, cluster routing strategies ([#233](https://github.com/joshrotenberg/redis-tower/pull/233))
- final audit round -- cluster NAT, TLS flex, pool health, 87 new tests, CI ([#226](https://github.com/joshrotenberg/redis-tower/pull/226))
- implement Service<Cmd> for ClusterConnection and SentinelConnection
- shared command test macro for standalone/cluster matrix
- add redis-test-harness crate and run cluster integration tests
- add read preference, ClusterClient, and readonly routing
- add MOVED/ASK redirect handling to cluster connection
- add redis-tower-cluster crate with slot routing and topology
- [**breaking**] v2 rewrite -- workspace scaffold with core features
- add comprehensive TLS support for Redis connections

### Fixed

- structured reconnect/redirect/failover logs, non-idempotent retry guard, middleware unit tests (closes #303, #306, #340) ([#369](https://github.com/joshrotenberg/redis-tower/pull/369))
- cluster MOVED/ASK topology refresh, topology_mut, CROSSSLOT and redirect tests (closes #317, #327, #333) ([#367](https://github.com/joshrotenberg/redis-tower/pull/367))
- [**breaking**] honor Tower backpressure contract in poll_ready
- rewrite test harness to sync process management

### Other

- thread-safety docs, missing examples, #[deny(missing_docs)] (closes #337, #341, #343) ([#370](https://github.com/joshrotenberg/redis-tower/pull/370))
- replace test harness server lifecycle with redis-server-wrapper ([#248](https://github.com/joshrotenberg/redis-tower/pull/248))
- bump MSRV to 1.88, apply let-chain suggestions ([#247](https://github.com/joshrotenberg/redis-tower/pull/247))
- surface MultiplexedClusterClient, add benchmarks, fix flaky tests ([#243](https://github.com/joshrotenberg/redis-tower/pull/243))
- clean up README, move examples, add licenses, remove stale files ([#204](https://github.com/joshrotenberg/redis-tower/pull/204))
- add tower-resilience integration guide and example
- comprehensive documentation rewrite
- add crate metadata, module docs, and 10 runnable examples
- format
- add README with full API overview
- comprehensive cluster unit test coverage
- polish phase for v0.1.0 release
- initialize redis-tower experimental project skeleton
