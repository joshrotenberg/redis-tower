# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned
- Connection pooling improvements
- RedisJSON module support
- RediSearch module support
- RedisTimeSeries module support
- Client-side caching
- RESP3 protocol support
- Pipeline builder enhancements
- Performance benchmarks vs redis-rs and fred

## [0.1.0] - 2025-10-24

### Added

#### Core Features
- **200 Redis commands** across all major categories (50% coverage)
- **Tower-native architecture** with `Service` trait implementation
- **Type-safe command API** with compile-time validation
- **Builder patterns** for complex commands with optional parameters
- **Zero-cost abstractions** via feature flags

#### Command Categories
- **Strings** (28 commands): GET, SET, INCR, APPEND, LCS, etc.
- **Hashes** (14 commands): HGET, HSET, HINCRBY, HRANDFIELD, etc.
- **Lists** (22 commands): LPUSH, RPOP, LRANGE, LMPOP, BLMOVE, etc.
- **Sets** (17 commands): SADD, SINTER, SUNION, SRANDMEMBER, etc.
- **Sorted Sets** (32 commands): ZADD, ZRANGE, ZMPOP, ZUNIONSTORE, etc.
- **Streams** (14 commands): XADD, XREAD, XREADGROUP, XACK, XPENDING, XCLAIM, XGROUP, etc.
- **Geospatial** (6 commands): GEOADD, GEOSEARCH, GEOSEARCHSTORE, etc.
- **HyperLogLog** (3 commands): PFADD, PFCOUNT, PFMERGE
- **Bitmap** (5 commands): SETBIT, GETBIT, BITCOUNT, BITOP, BITPOS
- **Keys** (17 commands): DEL, EXPIRE, DUMP, RESTORE, SCAN, etc.
- **Pub/Sub** (3 commands): PUBLISH, PUBSUB NUMSUB, PUBSUB NUMPAT
- **Scripting** (5 commands): EVAL, EVALSHA, SCRIPT LOAD, etc.
- **Server** (9 commands): INFO, DBSIZE, FLUSHDB, SAVE, etc.
- **Connection** (8 commands): AUTH, SELECT, CLIENT SETNAME, etc.
- **Transactions** (5 commands): MULTI, EXEC, DISCARD, WATCH, UNWATCH

#### Deployment Topology Support
- **Redis Cluster** support with automatic slot-based routing
- **Redis Sentinel** support for high availability
- **ReadOnly trait** for read-from-replica optimization
- Slot map caching and automatic redirection handling

#### Redis Modules
- **Bloom Filter** module (11 commands): BF.ADD, BF.EXISTS, BF.MADD, BF.RESERVE, etc.
- Feature-gated module support for RedisJSON, RediSearch (planned)

#### Developer Experience
- **Comprehensive documentation** with examples in every command
- **20+ examples** covering basic usage, cluster, sentinel, streams, geo, etc.
- **Builder pattern** for commands with optional parameters
- **ReadOnly trait** for cluster/sentinel read routing
- **Strong error types** with `thiserror`

#### Infrastructure
- **Feature flags** for optional functionality:
  - `cluster` - Redis Cluster support
  - `sentinel` - Redis Sentinel support
  - `deprecated` - Deprecated commands with migration guides
  - `bloom` - Bloom filter module
  - `modules` - Parent feature for all Redis modules
- **211 unit tests** with comprehensive coverage
- **Clippy clean** with strict linting
- **GitHub Actions** CI/CD (planned)

#### Documentation
- Comprehensive README with quick start and examples
- CONTRIBUTING.md with development guidelines
- COMMANDS_TRACKING.md tracking implementation progress
- CLAUDE.md with architectural decisions
- Inline documentation for all public APIs

### Notable Commands Added in Final Push

#### Redis 7.0+ Commands
- `LCS` - Longest common subsequence with IDX/LEN options
- `ZMPOP`, `BZMPOP` - Pop from multiple sorted sets
- `EXPIRETIME`, `PEXPIRETIME` - Get key expiration timestamps

#### Redis 6.2+ Commands
- `GEOSEARCHSTORE` - Search and store geospatial results
- `ZRANDMEMBER` - Random member from sorted set with count/scores
- `COPY` - Copy keys with replace option
- `ZDIFFSTORE` - Diff sorted sets and store result

#### Stream Consumer Groups
- `XREADGROUP` - Read from stream as consumer group
- `XACK` - Acknowledge processed messages
- `XPENDING` - Get pending messages info
- `XCLAIM` - Claim pending messages
- `XGROUP CREATE` - Create consumer group
- `XGROUP DESTROY` - Destroy consumer group

#### Key Management
- `DUMP` - Serialize key value
- `RESTORE` - Deserialize with TTL, REPLACE, ABSTTL, IDLETIME, FREQ options
- `TOUCH` - Update access time
- `UNLINK` - Async key deletion

#### Sorted Set Operations
- `ZUNIONSTORE` - Union with weights/aggregate
- `ZINTERSTORE` - Intersect with weights/aggregate
- `ZDIFFSTORE` - Difference operation

### Technical Improvements
- High-performance RESP parser with zero-copy parsing (~34-48ns/op, 4.8-8.0 GB/s)
- Tokio-based async I/O with framing support
- Connection pooling with configurable size and timeouts
- Pipeline support for batching commands
- Transaction support with MULTI/EXEC

### Breaking Changes
- None (initial release)

### Deprecated
- Commands available via `deprecated` feature flag:
  - `GETSET` - Use `SET` with `GET` option instead
  - `RPOPLPUSH` - Use `LMOVE` instead
  - `BRPOPLPUSH` - Use `BLMOVE` instead

### Migration Guide
N/A - Initial release

## Version History

- **0.1.0** (2025-10-24) - Initial release with 200 commands and full Tower integration
- More versions coming soon!

[Unreleased]: https://github.com/joshrotenberg/redis-tower/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/joshrotenberg/redis-tower/releases/tag/v0.1.0
