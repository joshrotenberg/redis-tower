# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/joshrotenberg/redis-tower/releases/tag/redis-tower-sync-v0.1.0) - 2026-06-05

### Added

- add redis-tower-sync crate for blocking API
- [**breaking**] v2 rewrite -- workspace scaffold with core features
- add comprehensive TLS support for Redis connections

### Other

- thread-safety docs, missing examples, #[deny(missing_docs)] (closes #337, #341, #343) ([#370](https://github.com/joshrotenberg/redis-tower/pull/370))
- MultiplexedClient connect_resp3 and SyncClient live-server integration tests (#264, #266, #267) ([#278](https://github.com/joshrotenberg/redis-tower/pull/278))
- surface MultiplexedClusterClient, add benchmarks, fix flaky tests ([#243](https://github.com/joshrotenberg/redis-tower/pull/243))
- clean up README, move examples, add licenses, remove stale files ([#204](https://github.com/joshrotenberg/redis-tower/pull/204))
- add tower-resilience integration guide and example
- comprehensive documentation rewrite
- add README with full API overview
- polish phase for v0.1.0 release
- initialize redis-tower experimental project skeleton
