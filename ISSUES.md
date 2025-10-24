# Project Issues and Roadmap

This document tracks current issues, planned features, and project priorities for redis-tower.

**Last Updated**: 2025-10-24  
**Version**: 0.1.0

## Current Status

✅ **Ready for v0.1.0 Release**
- **208 commands implemented** (~52% coverage of Redis 7.x)
- **265 unit tests passing**
- **Transaction support**: Multi, Exec, Discard, Watch, Unwatch
- **Sort command**: Full support with BY, LIMIT, GET, ordering, ALPHA, STORE
- **Wait command**: Replication synchronization support
- Comprehensive documentation
- CI/CD infrastructure in place

### Commands by Category (208 total)
- ✅ **Strings**: 27 commands (GET, SET, INCR, APPEND, GETEX, GETDEL, LCS, etc.)
- ✅ **Hashes**: 14 commands (HGET, HSET, HINCRBY, HRANDFIELD, etc.)
- ✅ **Lists**: 22 commands (LPUSH, RPOP, LMOVE, BLMPOP, etc.)
- ✅ **Sets**: 21 commands (SADD, SINTER, SUNION, SDIFF, SINTERCARD, etc.)
- ✅ **Sorted Sets**: 36 commands (ZADD, ZRANGE, ZUNION, ZDIFF, ZMPOP, BZMPOP, etc.)
- ✅ **Streams**: 15 commands (XADD, XREAD, XREADGROUP, XCLAIM, etc.)
- ✅ **Transactions**: 5 commands (MULTI, EXEC, DISCARD, WATCH, UNWATCH)
- ✅ **Pub/Sub**: 3 commands (PUBLISH, PUBSUB NUMSUB, PUBSUB NUMPAT)
- ✅ **Scripting**: 5 commands (EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH)
- ✅ **Keys**: 22 commands (DEL, EXISTS, EXPIRE, TTL, TYPE, SORT, DUMP, RESTORE, OBJECT commands, etc.)
- ✅ **Server**: 10 commands (DBSIZE, FLUSHDB, FLUSHALL, INFO, TIME, SAVE, BGSAVE, LASTSAVE, WAIT, etc.)
- ✅ **Scan**: 4 commands (SCAN, HSCAN, SSCAN, ZSCAN)
- ✅ **Connection**: 8 commands (AUTH, SELECT, QUIT, READONLY, READWRITE, CLIENT GETNAME/SETNAME)
- ✅ **HyperLogLog**: 3 commands (PFADD, PFCOUNT, PFMERGE)
- ✅ **Geo**: 8 commands (GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH, etc.)
- ✅ **Bitmap**: 5 commands (SETBIT, GETBIT, BITCOUNT, BITOP, BITPOS)

---

## Priority Issues

### High Priority (v0.1.x patches)

#### Performance & Stability
- [ ] **Add connection pooling improvements** - Current implementation is basic, needs optimization for high-load scenarios
- [ ] **Implement connection health checks** - Detect and recover from dead connections
- [ ] **Add request timeout handling** - Ensure requests don't hang indefinitely
- [ ] **Memory leak investigation** - Profile for potential memory leaks in long-running connections

#### Documentation
- [ ] **Add real integration tests** - Current tests are mostly unit tests, need tests against real Redis
- [ ] **Add performance benchmarks** - Compare against redis-rs and fred
- [ ] **Document cluster failover scenarios** - How the client handles node failures
- [ ] **Add troubleshooting guide** - Common issues and solutions

### Medium Priority (v0.2.0)

#### Commands (~192 commands remaining to reach 400 total / 95% coverage)

**High-Value Missing Commands:**
- [ ] **Server/Admin**: CONFIG GET/SET/REWRITE, SLOWLOG, MONITOR, CLIENT LIST/KILL, MEMORY commands
- [ ] **Pub/Sub**: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB CHANNELS/SHARDCHANNELS/SHARDNUMSUB
- [ ] **Cluster**: CLUSTER INFO, CLUSTER NODES, CLUSTER SLOTS, CLUSTER KEYSLOT, CLUSTER ADDSLOTS, etc.
- [ ] **Streams**: XINFO STREAM/GROUPS/CONSUMERS, XSETID, XAUTOCLAIM
- [ ] **Geo**: GEORADIUS* (deprecated but still used), GEOSEARCHSTORE improvements
- [ ] **Keys**: SCAN variants (SSCAN, ZSCAN), MIGRATE, WAITAOF
- [ ] **Strings**: GETEX variants, LCS improvements
- [ ] **Lists**: Additional LPOS options
- [ ] **Sets**: Missing set operations
- [ ] **Sorted Sets**: ZRANGESTORE, additional ZRANGE variants

**Note**: We have ZDIFF, ZUNION, ZINTER, BLMPOP, LMOVE already implemented ✅

#### Features
- [ ] **Client-side caching** - Implement tracking and invalidation
- [ ] **RESP3 protocol support** - Upgrade from RESP2 to RESP3
- [ ] **Pipeline builder improvements** - Type-safe pipeline composition
- [ ] **Transaction builder enhancements** - Better ergonomics for MULTI/EXEC
- [ ] **Pub/Sub improvements** - Better patterns, multiplexing

#### Redis Modules
- [ ] **RedisJSON support** - JSON.GET, JSON.SET, etc.
- [ ] **RediSearch support** - FT.SEARCH, FT.CREATE, etc.
- [ ] **RedisTimeSeries support** - TS.ADD, TS.RANGE, etc.
- [ ] **RedisGraph support** - Graph query commands

### Low Priority (v0.3.0+)

#### Advanced Features
- [ ] **TLS support** - Encrypted connections
- [ ] **Unix socket support** - Local Redis connections
- [ ] **Sentinel improvements** - Better failover handling
- [ ] **Read-from-replica load balancing** - Distribute reads across replicas
- [ ] **Custom derive macros** - Generate Command implementations

#### Developer Experience
- [ ] **Command code generation** - Generate from Redis command docs
- [ ] **Better error messages** - More context in error types
- [ ] **Tracing integration** - Better observability
- [ ] **Metrics collection** - Track command latency, connection pool stats

---

## Known Limitations

### Architectural
1. **RESP2 only** - Currently only supports RESP2 protocol (RESP3 planned for v0.2.0)
2. **No pipelining optimization** - Pipelines work but aren't optimized for batching
3. **Basic connection pooling** - Simple round-robin, no advanced strategies
4. **No automatic retry logic** - Users must add retry middleware themselves

### Command Coverage
1. **Missing modules** - Only Bloom filter module implemented so far
2. **Missing server commands** - CONFIG, SLOWLOG, CLIENT commands incomplete
3. **Missing stream commands** - XAUTOCLAIM, XGROUP SETID, etc.
4. **No Redis Stack search** - RediSearch commands not implemented

### Testing
1. **No cluster integration tests** - Only unit tests for cluster logic
2. **No performance benchmarks** - Can't compare to redis-rs/fred yet
3. **No fuzzing** - Protocol parsing not fuzzed
4. **Limited error case testing** - Edge cases need more coverage

---

## Won't Fix / Out of Scope

### Intentional Limitations
- **Redis < 6.0 support** - Focus on modern Redis versions
- **Blocking API** - This is an async-only client
- **Low-level protocol access** - Users should use commands, not raw frames
- **Built-in connection encryption** - Use Tower middleware instead

---

## Community Requests

Track issues filed by community here once repository is public.

### Feature Requests
*(None yet - project not yet published)*

### Bug Reports
*(None yet - project not yet published)*

---

## Technical Debt

### Code Quality
- [ ] Reduce code duplication in command implementations
- [ ] Better test organization (separate unit/integration)
- [ ] Improve error type ergonomics
- [ ] Add more inline documentation examples

### Performance
- [ ] Profile hot paths and optimize
- [ ] Reduce allocations in frame parsing
- [ ] Optimize connection pool lock contention
- [ ] Benchmark against redis-rs and fred

### Documentation
- [ ] Add architecture diagrams
- [ ] Create command implementation guide video
- [ ] Document Tower middleware patterns
- [ ] Add more real-world examples

---

## Tracking Metrics

### Command Coverage
- Current: **208/400 (52%)**
- Target v0.2.0: 300/400 (75%)
- Target v1.0.0: 380/400 (95%)

### Recent Progress
- **+8 commands** since last update (201 → 208)
  - Transaction primitives: Multi, Exec, Discard, Watch, Unwatch
  - Sort command with full options
  - Wait command for replication
  - OBJECT commands (RefCount, Encoding, IdleTime, Freq)

### Test Coverage
- Current: **265 unit tests** (+54 since last update)
- Target: 70%+ code coverage
- Integration tests: **13 tests** for Sort/Wait (testcontainers-based)

### Performance
- Benchmarks: Not yet measured
- Target: Within 20% of redis-rs performance

### Documentation
- API docs: ✅ Complete for v0.1.0
- Examples: ✅ 20+ examples
- Guides: ⚠️ Need troubleshooting guide

---

## Release Planning

### v0.1.0 (Current)
- ✅ 200 commands
- ✅ Cluster and Sentinel support
- ✅ Bloom filter module
- ✅ Comprehensive documentation
- ✅ CI/CD with release-plz

### v0.1.1 (Patch - if needed)
- Bug fixes from community feedback
- Documentation improvements
- Performance tweaks

### v0.2.0 (Next minor - Q1 2025)
- RESP3 protocol support
- Additional 100 commands (75% coverage)
- Client-side caching
- RedisJSON module
- Performance benchmarks
- Integration tests

### v0.3.0 (Future)
- RediSearch module
- RedisTimeSeries module
- Advanced connection pooling
- TLS support
- Custom derive macros

### v1.0.0 (Stable)
- 95%+ command coverage
- Production-proven stability
- Comprehensive test suite
- Performance competitive with redis-rs
- Full documentation

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on:
- Reporting issues
- Requesting features
- Submitting pull requests
- Code standards

## Issue Labels

Use these labels when filing issues:

- `bug` - Something isn't working
- `enhancement` - New feature or request
- `documentation` - Documentation improvements
- `good first issue` - Good for newcomers
- `help wanted` - Extra attention needed
- `performance` - Performance improvements
- `question` - Further information requested
- `wontfix` - This will not be worked on
