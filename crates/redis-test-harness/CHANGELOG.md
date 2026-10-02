# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2](https://github.com/joshrotenberg/redis-tower/compare/redis-tower-test-v0.1.1...redis-tower-test-v0.1.2) - 2026-10-01

### Added

- *(commands)* distinguish conditional SET outcomes ([#731](https://github.com/joshrotenberg/redis-tower/pull/731))
- *(conformance)* add versioned capability ledger ([#730](https://github.com/joshrotenberg/redis-tower/pull/730))

### Fixed

- *(core)* support authenticated Unix Redis URLs ([#719](https://github.com/joshrotenberg/redis-tower/pull/719))

### Other

- coordinate refreshing credentials across sessions ([#732](https://github.com/joshrotenberg/redis-tower/pull/732))
- add command guides and lifecycle contracts ([#728](https://github.com/joshrotenberg/redis-tower/pull/728))
- refresh release state, comparisons, and migration guidance ([#727](https://github.com/joshrotenberg/redis-tower/pull/727))

## [0.1.1](https://github.com/joshrotenberg/redis-tower/compare/redis-tower-test-v0.1.0...redis-tower-test-v0.1.1) - 2026-09-24

### Added

- support dedicated Cluster connections and binary Pub/Sub ([#693](https://github.com/joshrotenberg/redis-tower/pull/693))

## [0.1.0](https://github.com/joshrotenberg/redis-tower/releases/tag/redis-tower-test-v0.1.0) - 2026-08-26

### Added

- `MockConnection` for deterministic typed-command response parsing without a
  Redis server
- reusable command-contract test macros for standalone and clustered clients
- a managed three-master, three-replica Redis Cluster fixture with bounded
  startup, deterministic slot keys, resharding, promotion, and cleanup helpers
- a workspace port-range registry that catches overlapping live-server test
  fixtures
