# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/joshrotenberg/redis-tower/releases/tag/redis-tower-test-v0.1.0) - 2026-08-26

### Added

- `MockConnection` for deterministic typed-command response parsing without a
  Redis server
- reusable command-contract test macros for standalone and clustered clients
- a managed three-master, three-replica Redis Cluster fixture with bounded
  startup, deterministic slot keys, resharding, promotion, and cleanup helpers
- a workspace port-range registry that catches overlapping live-server test
  fixtures
