# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/joshrotenberg/redis-tower/releases/tag/redis-tower-sentinel-v0.1.0) - 2026-06-05

### Added

- metrics error dimensions, OTel semconv spans, pool stats API, health_check (closes #293, #297, #307, #315) ([#366](https://github.com/joshrotenberg/redis-tower/pull/366))
- MultiplexedClient graceful shutdown (closes #311) ([#360](https://github.com/joshrotenberg/redis-tower/pull/360))
- RedisExecutor impls for ClusterConnection and SentinelConnection, ConnectionPool tests ([#259](https://github.com/joshrotenberg/redis-tower/pull/259)) ([#286](https://github.com/joshrotenberg/redis-tower/pull/286))
- MultiplexedSentinelClient for high-concurrency sentinel workloads ([#249](https://github.com/joshrotenberg/redis-tower/pull/249)) ([#277](https://github.com/joshrotenberg/redis-tower/pull/277))
- add error classification methods for retry policies
- implement Service<Cmd> for ClusterConnection and SentinelConnection
- add redis-tower-sentinel crate with master discovery and failover
- [**breaking**] v2 rewrite -- workspace scaffold with core features
- add comprehensive TLS support for Redis connections

### Fixed

- structured reconnect/redirect/failover logs, non-idempotent retry guard, middleware unit tests (closes #303, #306, #340) ([#369](https://github.com/joshrotenberg/redis-tower/pull/369))
- sentinel failover mid-command recovery, quorum and reconnect-after-failover tests (closes #325, #336) ([#371](https://github.com/joshrotenberg/redis-tower/pull/371))
- [**breaking**] honor Tower backpressure contract in poll_ready
- rewrite test harness to sync process management

### Other

- apply command_tests! macro to MultiplexedSentinelClient (closes #395) ([#401](https://github.com/joshrotenberg/redis-tower/pull/401))
- thread-safety docs, missing examples, #[deny(missing_docs)] (closes #337, #341, #343) ([#370](https://github.com/joshrotenberg/redis-tower/pull/370))
- sentinel failover simulation test ([#262](https://github.com/joshrotenberg/redis-tower/pull/262)) ([#287](https://github.com/joshrotenberg/redis-tower/pull/287))
- replace test harness server lifecycle with redis-server-wrapper ([#248](https://github.com/joshrotenberg/redis-tower/pull/248))
- bump MSRV to 1.88, apply let-chain suggestions ([#247](https://github.com/joshrotenberg/redis-tower/pull/247))
- surface MultiplexedClusterClient, add benchmarks, fix flaky tests ([#243](https://github.com/joshrotenberg/redis-tower/pull/243))
- clean up README, move examples, add licenses, remove stale files ([#204](https://github.com/joshrotenberg/redis-tower/pull/204))
- add tower-resilience integration guide and example
- comprehensive documentation rewrite
- fmt
- add crate metadata, module docs, and 10 runnable examples
- add README with full API overview
- polish phase for v0.1.0 release
- initialize redis-tower experimental project skeleton
