# Redis-Tower Issue Tracking Structure

## Overview

We've established a comprehensive issue tracking system on GitHub to manage the redis-tower project roadmap and track all work items.

**View all issues**: https://github.com/joshrotenberg/redis-tower/issues

## Issue Structure

### Main Roadmap Issue

**#1 - 🗺️ Project Roadmap: redis-tower v0.1.0**
- Master tracking issue for the entire project
- Lists completed work and upcoming milestones
- References all umbrella issues
- Timeline: v0.1.0 (current) → v0.2.0 → v0.3.0 → v1.0.0

### Umbrella Issues (Major Work Areas)

These are meta-issues that track categories of work with sub-issues:

#### #2 - 📋 Commands: Complete Redis Command Coverage
**Status**: ~107 commands implemented, targeting 90%+ coverage  
**Priority**: High  
**Sub-issues**: #4, #5, #6, #7, and more to be created

Currently implemented:
- ✅ Strings (~15 commands)
- ✅ Hashes (~11 commands)
- ✅ Lists (~14 commands)
- ✅ Sets (~15 commands)
- ✅ Sorted Sets (~12 commands)
- ✅ Streams (~8 commands)
- ✅ Pub/Sub (3 commands)
- ✅ Scripting (5 commands)
- ✅ Scan (4 commands)
- ✅ Connection (6 commands)
- ✅ Sentinel (4 commands)

Still needed:
- HyperLogLog commands
- Geospatial commands
- Bitmap commands
- Additional key/server commands
- Additional cluster commands

#### #3 - 💾 Client-Side Caching: RESP3 Server-Assisted Caching
**Status**: Research complete, implementation not started  
**Priority**: Medium (post-v1.0 consideration)

Key challenges documented:
- RESP3 protocol requirement
- Tower Service trait incompatibility with push notifications
- Connection management complexity
- Cache implementation decisions

Three architectural approaches evaluated:
1. Separate tracking connection (redis-rs approach)
2. RESP3 multiplexed connection (fred approach)
3. Tower wrapper service with side channel

Comprehensive research on how other clients handle it:
- redis-rs: Experimental, sharded LRU, RESP3 only
- fred: Client tracking example, two-connection mode
- Jedis: Custom Cacheable interface, ~90% performance improvement
- lettuce: Reconnection issues, needs connection listeners
- ioredis: Not implemented (lacks RESP3)

#### #4 - Commands: Key Operations (DEL, EXISTS, EXPIRE, TTL, TYPE, etc.)
**Status**: Sub-issue of #2  
**Priority**: High  
**Labels**: good first issue

Essential key management commands:
- DEL, EXISTS, EXPIRE, EXPIREAT, TTL, PTTL
- PERSIST, TYPE, KEYS, RANDOMKEY
- RENAME, RENAMENX, MOVE, DUMP, RESTORE
- TOUCH, UNLINK

#### #5 - Commands: Server & Admin Operations (INFO, PING, DBSIZE, CONFIG, etc.)
**Status**: Sub-issue of #2  
**Priority**: High

Server monitoring and administration:
- INFO, PING, DBSIZE, TIME, LASTSAVE
- CLIENT LIST, CLIENT GETNAME, CLIENT SETNAME
- CONFIG GET/SET/REWRITE/RESETSTAT
- COMMAND, SAVE, BGSAVE, BGREWRITEAOF
- FLUSHDB, FLUSHALL, MEMORY commands
- SLOWLOG, MONITOR

#### #6 - Commands: HyperLogLog Operations (PFADD, PFCOUNT, PFMERGE)
**Status**: Sub-issue of #2  
**Priority**: Medium  
**Labels**: good first issue

Probabilistic cardinality estimation:
- PFADD - Add elements
- PFCOUNT - Get cardinality estimate
- PFMERGE - Merge multiple HyperLogLogs
- PFSELFTEST - Self-test

#### #7 - Commands: Geospatial Operations (GEOADD, GEODIST, GEORADIUS, etc.)
**Status**: Sub-issue of #2  
**Priority**: Medium

Location-based queries:
- GEOADD, GEODIST, GEOHASH, GEOPOS
- GEORADIUS (deprecated), GEORADIUSBYMEMBER (deprecated)
- GEOSEARCH (Redis 6.2+), GEOSEARCHSTORE

#### #8 - 📊 Testing: Comprehensive Test Coverage & Integration Tests
**Status**: 87 tests passing, need comprehensive coverage  
**Priority**: High

Test categories needed:
1. Unit tests (80%+ coverage target)
2. Integration tests (real Redis)
3. Cluster integration tests
4. Sentinel integration tests
5. Tower middleware tests
6. Connection pool tests
7. Failure scenario tests
8. Performance & load tests
9. Property-based testing
10. Documentation tests

Infrastructure needed:
- Docker Compose for Redis/Cluster/Sentinel
- Test utilities and helpers
- CI/CD integration
- Coverage reporting

#### #9 - 📚 Documentation: Complete User & API Documentation
**Status**: Basic docs exist, need comprehensive guide  
**Priority**: Medium

Documentation categories:
1. User documentation (getting started, user guide, config)
2. API documentation (rustdoc completeness)
3. Examples (basic, advanced, real-world)
4. Architecture documentation
5. Contributing guide
6. Migration guides (from redis-rs/fred)
7. Troubleshooting guide

Infrastructure:
- mdBook for user guide
- docs.rs for API reference
- GitHub Pages for hosted docs
- Documentation tests in CI

#### #10 - ⚡ Performance: Optimization & Benchmarking
**Status**: Beating fred by 12-35%, need redis-rs comparison  
**Priority**: Medium

Focus areas:
1. Expand benchmarks (add redis-rs)
2. Hot path optimization
3. Memory optimization
4. Concurrency optimization
5. Profiling & measurement
6. Performance testing (load, stress)
7. Optimization ideas (quick wins, architectural)

Current achievements:
- 12% faster than fred on GET/SET
- 35% faster than fred on mixed workload
- Zero-cost type safety demonstrated

Performance budget:
- GET/SET latency: < 100µs @ p99
- Pipeline (100 cmds): < 500µs @ p99
- Throughput: > 100k ops/sec
- Memory: < 10MB base
- Tower overhead: < 5µs per request

## Labels Used

### Area Labels
- `area: commands` - Redis command implementation
- `area: networking` - Network layer, codec, connections
- `area: testing` - Tests, integration tests, benchmarks
- `area: tower` - Tower Service integration and middleware
- `area: cluster` - Redis Cluster support
- `area: pubsub` - Pub/Sub functionality
- `area: client-caching` - Client-side caching features
- `area: transactions` - MULTI/EXEC transactions
- `area: performance` - Performance improvements

### Type Labels
- `type: feature` - New feature request
- `type: refactor` - Code refactoring
- `bug` - Something isn't working
- `documentation` - Improvements to docs
- `enhancement` - Improvement to existing feature

### Priority Labels
- `priority: high` - High priority (blocking)
- `priority: medium` - Medium priority
- `priority: low` - Low priority (nice to have)

### Status Labels
- `status: blocked` - Blocked by another issue
- `status: in-progress` - Currently being worked on

### Special Labels
- `good first issue` - Good for newcomers
- `help wanted` - Extra attention needed

## How to Use This System

### For Contributors

1. **Check the roadmap**: Start with issue #1 to see the big picture
2. **Pick an area**: Look at umbrella issues (#2-10) for your interest area
3. **Find a task**: Look for `good first issue` labels or sub-issues
4. **Reference the umbrella**: When creating PRs, reference the relevant umbrella issue

### For Maintainers

1. **Create sub-issues** for specific tasks under umbrella issues
2. **Update roadmap** (#1) as milestones are completed
3. **Close umbrellas** when all sub-issues are complete
4. **Track progress** using GitHub Projects or milestones

### Creating New Sub-Issues

When creating a sub-issue:
1. Reference the parent umbrella issue (e.g., "Part of #2")
2. Use appropriate area and priority labels
3. Include clear implementation guidance
4. Add examples and test requirements
5. Link to relevant Redis documentation

## Current Status Summary

### ✅ Completed (v0.1.0)
- Core infrastructure (codec, connection, commands)
- Tower middleware integration
- Cluster support with read-from-replica
- Sentinel support with automatic failover
- Type-safe pipelining and transactions
- ~107 commands implemented
- Benchmarks showing 12-35% performance advantage over fred

### 🚧 In Progress
- Complete command coverage (#2)
- Comprehensive testing (#8)

### 📋 Planned (v0.2.0+)
- Client-side caching (#3)
- Complete documentation (#9)
- Performance optimization (#10)
- Production hardening

## Timeline

- **v0.1.0** (Current): Core features, cluster, sentinel
- **v0.2.0** (Next): Complete commands, comprehensive testing
- **v0.3.0** (Future): Client-side caching, production hardening
- **v1.0.0** (Goal): Stable API, production-ready

### Module Support

#### #15 - 🔌 Modules: Redis Stack Module Support
**Status**: Planning  
**Priority**: Low (post-v1.0)  
**Sub-issues**: #11, #12, #13, #14

Feature-gated support for Redis Stack modules:
- RedisJSON: Native JSON documents with serde
- RediSearch: Full-text search and vector similarity
- RedisBloom: Probabilistic data structures
- RedisTimeSeries: Time-series with downsampling

All modules are optional dependencies:
- Zero overhead if not used
- Type-safe operations
- Tower middleware compatible
- Comprehensive documentation

#### #11 - Modules: RedisJSON Support
**Status**: Sub-issue of #15  
**Priority**: Medium  
**Feature Gate**: `json`

JSON document operations:
- JSON.SET, JSON.GET with serde integration
- JSONPath queries
- Array/object operations
- Numeric operations

#### #12 - Modules: RediSearch Support
**Status**: Sub-issue of #15  
**Priority**: Medium  
**Feature Gate**: `search`

Full-text search capabilities:
- FT.CREATE, FT.SEARCH
- Index schema builder
- Vector similarity search (AI/ML)
- Aggregations
- Auto-complete suggestions

#### #13 - Modules: RedisBloom Support
**Status**: Sub-issue of #15  
**Priority**: Low  
**Feature Gate**: `bloom`

Probabilistic data structures:
- Bloom Filter (membership testing)
- Cuckoo Filter (with deletion)
- Count-Min Sketch (frequency counting)
- Top-K (trending items)

#### #14 - Modules: RedisTimeSeries Support
**Status**: Sub-issue of #15  
**Priority**: Low  
**Feature Gate**: `timeseries`

Time-series data management:
- TS.ADD, TS.RANGE
- Automatic downsampling
- Built-in aggregations
- Retention policies
- Multi-metric queries

---

**Last Updated**: 2025-10-24  
**Total Issues**: 15 (1 roadmap + 6 umbrellas + 8 sub-issues)  
**Status**: Active development, community contributions welcome
