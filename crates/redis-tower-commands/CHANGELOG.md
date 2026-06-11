# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/joshrotenberg/redis-tower/releases/tag/redis-tower-commands-v0.1.0) - 2026-06-05

### Added

- deprecated sorted-set range variants (ZREVRANGE, ZRANGEBYLEX, ZREVRANGEBYLEX, ZREVRANGEBYSCORE) (closes #310) ([#387](https://github.com/joshrotenberg/redis-tower/pull/387))
- XSETID command builder in streams.rs (closes #328) ([#386](https://github.com/joshrotenberg/redis-tower/pull/386))
- JsonClient<C> implementation with serde get/set/merge/arr/obj methods (closes #326) ([#381](https://github.com/joshrotenberg/redis-tower/pull/381))
- add bulk-insert constructors for HSet and ZAdd (closes #289, #323) ([#356](https://github.com/joshrotenberg/redis-tower/pull/356))
- CLIENT SETINFO, credential rotation, cluster routing strategies ([#233](https://github.com/joshrotenberg/redis-tower/pull/233))
- final audit round -- cluster NAT, TLS flex, pool health, 87 new tests, CI ([#226](https://github.com/joshrotenberg/redis-tower/pull/226))
- add LeastConnections dispatch strategy and fix CI issues
- add Redis Stack module commands (JSON, Search, probabilistic, TimeSeries)
- add auto-pipelining service and managed stream consumer
- add CLIENT, CONFIG, ACL, diagnostics, and PUBSUB commands
- add 29 commands for strings, lists, sets, sorted sets, keys, and server
- *(commands)* add cluster administration commands closes #143
- add blocking commands (BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX)
- add SCAN family commands (SCAN, SSCAN, HSCAN, ZSCAN)
- add Geo, HyperLogLog, and Bitmap command categories
- fill 44 missing commands across existing categories
- *(commands)* add Lua scripting and Functions commands closes #114
- *(commands)* add sharded pub/sub and hash field expiration commands closes #119
- *(commands)* add vector set commands (Redis 8+) closes #121
- add RESP3 push infrastructure and client-side caching
- add RESP3 protocol support and test matrix
- add MockConnection for unit-testing parse_response
- add remaining commands from #54
- add hash, list, set, and sorted set commands
- [**breaking**] v2 rewrite -- workspace scaffold with core features
- add comprehensive TLS support for Redis connections

### Fixed

- structured reconnect/redirect/failover logs, non-idempotent retry guard, middleware unit tests (closes #303, #306, #340) ([#369](https://github.com/joshrotenberg/redis-tower/pull/369))
- audit round 2 -- PubSub, RawCommand, errors, SCAN stream ([#225](https://github.com/joshrotenberg/redis-tower/pull/225))
- CI test failures for vector sets, RESP3 VInfo, and doc links
- resolve CI failures in test harness and formatting
- resolve rustdoc broken intra-doc link warnings

### Other

- proptest command-builder frame properties (closes #347) ([#388](https://github.com/joshrotenberg/redis-tower/pull/388))
- add #[must_use], # Errors, doc links, and constructor docs (closes #294, #319, #324, #330) ([#354](https://github.com/joshrotenberg/redis-tower/pull/354))
- OBJECT subcommand family integration tests ([#261](https://github.com/joshrotenberg/redis-tower/pull/261)) ([#275](https://github.com/joshrotenberg/redis-tower/pull/275))
- BLMOVE integration test ([#263](https://github.com/joshrotenberg/redis-tower/pull/263)) ([#273](https://github.com/joshrotenberg/redis-tower/pull/273))
- surface MultiplexedClusterClient, add benchmarks, fix flaky tests ([#243](https://github.com/joshrotenberg/redis-tower/pull/243))
- clean up README, move examples, add licenses, remove stale files ([#204](https://github.com/joshrotenberg/redis-tower/pull/204))
- add tower-resilience integration guide and example
- comprehensive documentation rewrite
- add feature gates for Redis Stack modules and 274 unit tests
- add crate metadata, module docs, and 10 runnable examples
- Merge pull request #167 from joshrotenberg/arsenale/143-cluster-administration-command
- Merge pull request #128 from joshrotenberg/automation/111-v2-streams-commands-xadd-xread-xreadgrou
- Merge pull request #123 from joshrotenberg/automation/121-v2-vector-sets-redis-8
- add README with full API overview
- polish phase for v0.1.0 release
- initialize redis-tower experimental project skeleton
