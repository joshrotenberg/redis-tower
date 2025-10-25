# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned for v0.2.0
- Connection pooling enhancements
- RedisJSON module support
- RediSearch module support
- RedisTimeSeries module support
- Client-side caching support
- Performance benchmarks vs redis-rs and fred
- Production deployment guides
- More middleware examples

## [0.1.0] - 2025-10-24

### Added

#### Core Features - Production Ready
- **328 Redis commands** - 100% core command coverage
- **530+ tests passing** - Comprehensive unit and integration test suite
- **100% type-safe API** - No stringly-typed commands, full compile-time validation
- **Tower-native architecture** with `Service` trait implementation
- **Builder patterns** for complex commands with optional parameters
- **Zero-cost abstractions** via feature flags
- **Structured response types** - SlowlogEntry, ModuleInfo, and more

#### Command Categories (328 Total)
- **Strings** (29): GET, SET, INCR, APPEND, GETEX, GETDEL, LCS, etc.
- **Hashes** (14): HGET, HSET, HINCRBY, HRANDFIELD, etc.
- **Lists** (22): LPUSH, RPOP, LRANGE, LMPOP, BLMOVE, etc.
- **Sets** (21): SADD, SINTER, SUNION, SINTERCARD, etc.
- **Sorted Sets** (44): ZADD, ZRANGE, ZMPOP, ZUNIONSTORE, ZINTERCARD, ZRANGESTORE, etc.
- **Streams** (15): XADD, XREAD, XREADGROUP, XACK, XPENDING, XCLAIM, XGROUP, etc.
- **Geospatial** (8): GEOADD, GEOSEARCH, GEOSEARCHSTORE, GEODIST, etc.
- **HyperLogLog** (3): PFADD, PFCOUNT, PFMERGE
- **Bitmap** (7): SETBIT, GETBIT, BITCOUNT, BITOP, BITFIELD, BITFIELD_RO, etc.
- **Keys** (27): DEL, EXPIRE, DUMP, RESTORE, SCAN, MIGRATE, SORT_RO, WAITAOF, etc.
- **Pub/Sub** (13): PUBLISH, SUBSCRIBE, PSUBSCRIBE, SSUBSCRIBE, PUBSUB commands, etc.
- **Scripting** (7): EVAL, EVALSHA, EVAL_RO, EVALSHA_RO, SCRIPT, etc.
- **Functions** (10): FCALL, FCALL_RO, FUNCTION LOAD/DELETE/FLUSH/LIST, etc.
- **ACL** (11): ACL SETUSER/GETUSER/DELUSER/LIST/CAT/WHOAMI, etc.
- **Server** (33): INFO, DBSIZE, FLUSHDB, CONFIG, SLOWLOG, MEMORY, DEBUG, etc.
- **Connection** (23): AUTH, SELECT, CLIENT, HELLO, RESET, etc.
- **Cluster** (27): CLUSTER INFO/NODES/SLOTS/SHARDS/ADDSLOTS/FAILOVER, etc.
- **Transactions** (5): MULTI, EXEC, DISCARD, WATCH, UNWATCH
- **Latency** (7): LATENCY DOCTOR/GRAPH/HISTOGRAM/HISTORY, etc.
- **Module** (4): MODULE LIST/LOAD/LOADEX/UNLOAD

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
- **530+ tests** with comprehensive unit and integration coverage
- **Clippy clean** with `-D warnings`
- **Integration tests** for pub/sub and transactions
- **CI/CD ready** with GitHub Actions support

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
