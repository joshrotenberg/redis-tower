# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/joshrotenberg/redis-tower/releases/tag/redis-tower-client-v0.1.0) - 2026-08-26

### Added

- `UniversalClient`, a cloneable executor over standalone, Redis Cluster, and
  Redis Sentinel multiplexed clients
- explicit constructors for each topology and URL-driven topology selection
  through `redis`, `redis+cluster`, and `redis+sentinel` schemes
- TLS variants and percent-decoded data-node credentials for every topology,
  plus independent Sentinel credentials and reconnect-safe configuration
