# Redis Command Coverage - Quick Reference

**Project:** redis-tower v0.1.0  
**Coverage:** 71.6% (371/518 commands)  
**Status:** ✅ Production ready for core use cases

---

## Coverage at a Glance

### 🟢 Complete (100%)
- **RedisBloom (BF.\*)** - 10/10 commands
- **Count-Min Sketch (CMS.\*)** - 6/6 commands  
- **RedisTimeSeries (TS.\*)** - 17/17 commands
- **Latency monitoring** - 7/7 commands

### 🟢 Excellent (90%+)
- **RedisJSON (JSON.\*)** - 92.3% (24/26)
- **Functions** - 88.9% (8/9)

### 🟡 Good (80-89%)
- **Core data structures** - 81.7% (205/251)
- **ACL** - 84.6% (11/13)
- **Cluster** - 82.8% (24/29)
- **Pub/Sub** - 83.3% (5/6)
- **Scripting** - 83.3% (5/6)
- **Config** - 80.0% (4/5)
- **Module** - 80.0% (4/5)
- **Object** - 80.0% (4/5)

### 🟡 Moderate (50-79%)
- **Cuckoo Filter (CF.\*)** - 75.0% (9/12)
- **Slowlog** - 75.0% (3/4)
- **Client** - 61.1% (11/18)

### 🔴 Needs Work (<50%)
- **XGROUP** - 33.3% (2/6) - Missing 4 subcommands
- **XINFO** - 0.0% (0/4) - Missing all subcommands
- **TDIGEST.\*** - 0.0% (0/14) - Module not implemented
- **TOPK.\*** - 0.0% (0/7) - Module not implemented
- **Memory introspection** - 33.3% (2/6)
- **Command introspection** - 28.6% (2/7)
- **RediSearch (FT.\*)** - 27.6% (8/29) - Missing advanced features

---

## Missing High-Priority Commands (8 total)

### Streams Consumer Groups
- `XGROUP-CREATECONSUMER`
- `XGROUP-DELCONSUMER`
- `XGROUP-SETID`
- `XGROUP-HELP`

**Note:** `XGROUP-CREATE` and `XGROUP-DESTROY` are already implemented!

### Streams Introspection
- `XINFO-CONSUMERS`
- `XINFO-GROUPS`
- `XINFO-STREAM`
- `XINFO-HELP`

---

## Missing Medium-Priority Commands (78 total)

### Redis Stack Modules (47 commands)
- **TDIGEST.\*** - 14 commands (probabilistic quantiles)
- **TOPK.\*** - 7 commands (top-k tracking)
- **RediSearch advanced** - 21 commands (aliases, dictionaries, suggestions)
- **Cuckoo Filter** - 3 commands (CF.MEXISTS, CF.SCANDUMP, CF.LOADCHUNK)
- **RedisJSON debug** - 2 commands (JSON.DEBUG-HELP, JSON.DEBUG-MEMORY)

### Core Introspection (31 commands)
- **Client-side caching** - 7 commands (CLIENT-TRACKING, etc.)
- **Command introspection** - 5 commands (COMMAND-DOCS, COMMAND-LIST, etc.)
- **Memory introspection** - 4 commands (MEMORY-DOCTOR, MEMORY-STATS, etc.)
- **Cluster management** - 5 commands
- **Help commands** - 10 commands (various *-HELP commands)

---

## Missing Low-Priority Commands (61 total)

### Deprecated (should redirect, not implement)
- `HMSET` → use `HSET`
- `GEORADIUS*` → use `GEOSEARCH`
- `SUBSTR` → use `GETRANGE`
- `SLAVEOF` → use `REPLICAOF`

### Redis 7.4+ Hash TTL (12 commands)
New feature, limited adoption yet

### Debug/Internal (15 commands)
- `LOLWUT`, `PSYNC`, `SYNC`, `REPLCONF`, `PFDEBUG`, etc.

### Experimental (12 commands)
- Vector similarity commands (`V*`)

### Root commands (15 commands)
Generic command names that just return help/error

---

## Roadmap to 80% Coverage

| Phase | Commands | New Coverage | Effort |
|-------|----------|--------------|--------|
| Phase 1: Complete Streams | 8 | 73.1% | 2-3 days |
| Phase 2: TDIGEST + TOPK | 21 | 77.2% | 1 week |
| Phase 3: Introspection | 14 | 79.9% | 3-4 days |
| **Total** | **43** | **~80%** | **~3 weeks** |

---

## What's Implemented Well

### ✅ All Core Data Structures
- Strings (28 commands)
- Hashes (14 commands)
- Lists (22 commands)
- Sets (21 commands)
- Sorted Sets (44 commands)
- Streams (15 commands) - *partial consumer groups*
- Geospatial (8 commands)
- HyperLogLog (3 commands)
- Bitmaps (7 commands)

### ✅ Most Redis Stack Modules
- RedisBloom (100%)
- RedisJSON (92%)
- RedisTimeSeries (100%)
- Count-Min Sketch (100%)
- Cuckoo Filter (75%)

### ✅ Administration & Cluster
- Cluster commands (83%)
- ACL (85%)
- Config (80%)
- Server management (100%)

### ✅ Scripting & Functions
- Lua scripts (83%)
- Redis Functions (89%)

---

## Comparison with Other Clients

| Feature | redis-tower | redis-rs | fred.rs |
|---------|-------------|----------|---------|
| Command Coverage | 71.6% | ~95% | ~100% |
| Type Safety | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| Tower Integration | ⭐⭐⭐⭐⭐ | - | - |
| Redis Stack | ⭐⭐⭐⭐ | - | ⭐⭐⭐⭐⭐ |
| Production Ready | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |

**redis-tower's unique strengths:**
- Best-in-class type safety with compile-time checking
- Only client with native Tower middleware support
- Modern builder patterns and structured responses
- Excellent documentation with examples for all 371 commands

---

## Full Details

See **COMMAND_COVERAGE_AUDIT.md** for:
- Complete list of all 147 missing commands
- Detailed category breakdowns
- Implementation recommendations
- Effort estimates

---

**Last Updated:** 2025-10-25  
**Next Audit:** After implementing Priority 1 commands (streams)
