# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/joshrotenberg/redis-tower/releases/tag/redis-tower-modules-v0.1.0) - 2026-06-05

### Added

- RedisError::Json variant and From<serde_json::Error> impl (closes #308) ([#385](https://github.com/joshrotenberg/redis-tower/pull/385))
- TimeSeriesClient<C> with typed samples, labels, range queries (closes #344) ([#382](https://github.com/joshrotenberg/redis-tower/pull/382))
- BloomFilter, CuckooFilter, CountMinSketch, TopK, TDigest high-level clients (closes #348) ([#379](https://github.com/joshrotenberg/redis-tower/pull/379))
- SearchClient<C> with index lifecycle, typed search results, and suggest (closes #338) ([#378](https://github.com/joshrotenberg/redis-tower/pull/378))
- VectorSetClient<C> with KNN search, typed SimilarityResult, VADD/VREM/VSIM (closes #351) ([#380](https://github.com/joshrotenberg/redis-tower/pull/380))
- JsonClient<C> implementation with serde get/set/merge/arr/obj methods (closes #326) ([#381](https://github.com/joshrotenberg/redis-tower/pull/381))
- redis-tower-modules crate scaffold with feature-gated module client stubs (closes #312) ([#376](https://github.com/joshrotenberg/redis-tower/pull/376))
- [**breaking**] v2 rewrite -- workspace scaffold with core features
- add comprehensive TLS support for Redis connections

### Other

- #[ignore] integration tests for module clients (closes #394) ([#397](https://github.com/joshrotenberg/redis-tower/pull/397))
- surface MultiplexedClusterClient, add benchmarks, fix flaky tests ([#243](https://github.com/joshrotenberg/redis-tower/pull/243))
- clean up README, move examples, add licenses, remove stale files ([#204](https://github.com/joshrotenberg/redis-tower/pull/204))
- add tower-resilience integration guide and example
- comprehensive documentation rewrite
- add README with full API overview
- polish phase for v0.1.0 release
- initialize redis-tower experimental project skeleton
