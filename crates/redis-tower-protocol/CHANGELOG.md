# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1](https://github.com/joshrotenberg/redis-tower/compare/redis-tower-protocol-v0.1.0...redis-tower-protocol-v0.1.1) - 2026-06-11

### Added

- final audit round -- cluster NAT, TLS flex, pool health, 87 new tests, CI ([#226](https://github.com/joshrotenberg/redis-tower/pull/226))

### Fixed

- cluster MOVED/ASK topology refresh, topology_mut, CROSSSLOT and redirect tests (closes #317, #327, #333) ([#367](https://github.com/joshrotenberg/redis-tower/pull/367))

### Other

- fix alloc hotpaths, add MultiplexedClient/ConnectionPool benchmarks, redis-rs comparison (closes #301, #309, #320) ([#373](https://github.com/joshrotenberg/redis-tower/pull/373))
- thread-safety docs, missing examples, #[deny(missing_docs)] (closes #337, #341, #343) ([#370](https://github.com/joshrotenberg/redis-tower/pull/370))
- surface MultiplexedClusterClient, add benchmarks, fix flaky tests ([#243](https://github.com/joshrotenberg/redis-tower/pull/243))
