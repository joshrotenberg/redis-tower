# Redis Commands Implementation Tracking

**Last Updated:** 2025-10-24  
**Total Redis Commands:** ~565 (from COMMAND LIST)  
**Implemented:** 189 (178 core + 11 module)  
**Coverage:** ~47% (178/400 core commands, 11 module commands behind feature flags)
**Deprecated Commands:** 3 (available with `deprecated` feature)

> **Note:** This is the single source of truth for command tracking. Historical analysis available in:
> - [docs/COMMAND_COVERAGE_REPORT.md](docs/COMMAND_COVERAGE_REPORT.md) - Comprehensive analysis vs fred/redis-rs
> - [docs/COMMANDS_LEGACY.md](docs/COMMANDS_LEGACY.md) - Original command list

---

## Implementation Status by Category

### ✅ Fully Implemented Categories

#### Strings (27 commands) ✅ EXPANDED
- [x] GET, SET, DEL, MGET, MSET
- [x] INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT
- [x] APPEND, STRLEN, GETRANGE, SETRANGE
- [x] GETEX, GETDEL
- [x] SETEX, PSETEX (set with expiration)
- [x] SETNX, MSETNX (set if not exists)
- [x] PING, ECHO
- [x] EXISTS, EXPIRE, TTL

#### Hashes (14 commands) ✅ EXPANDED
- [x] HGET, HSET, HDEL, HGETALL, HMGET
- [x] HEXISTS, HLEN, HKEYS, HVALS
- [x] HINCRBY, HINCRBYFLOAT, HSTRLEN
- [x] HSETNX (set if not exists)
- [x] HRANDFIELD (get random field)

#### Lists (22 commands) ✅ EXPANDED
- [x] LPUSH, RPUSH, LPOP, RPOP, LRANGE
- [x] LPUSHX, RPUSHX (push only if exists)
- [x] LLEN, LINDEX, LSET, LINSERT
- [x] LREM, LTRIM, LPOS
- [x] BLPOP, BRPOP (blocking pop)
- [x] LMOVE, BLMOVE (atomic move between lists - Redis 6.2+)
- [x] LMPOP, BLMPOP (pop from multiple lists - Redis 7.0+)

#### Sets (17 commands)
- [x] SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- [x] SINTER, SUNION, SDIFF
- [x] SINTERSTORE, SUNIONSTORE, SDIFFSTORE
- [x] SPOP, SRANDMEMBER, SMOVE
- [x] SMISMEMBER, SINTERCARD
- [x] SSCAN

#### Sorted Sets (28 commands) ✅ COMPLETE
- [x] ZADD, ZREM, ZCARD, ZSCORE
- [x] ZRANGE, ZREVRANGE, ZRANK, ZREVRANK
- [x] ZINCRBY, ZSCAN
- [x] ZPOPMIN, ZPOPMAX (pop lowest/highest scores)
- [x] BZPOPMIN, BZPOPMAX (blocking variants)
- [x] ZCOUNT (count members in range)
- [x] ZRANGEBYSCORE, ZREVRANGEBYSCORE (range by score)
- [x] ZRANGEBYLEX, ZREVRANGEBYLEX (range by lexicographic order)
- [x] ZLEXCOUNT (count in lexicographic range)
- [x] ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZREMRANGEBYRANK (remove by range)
- [x] ZMSCORE (get multiple member scores)
- [x] ZRANDMEMBER (get random member with count/scores - Redis 6.2+)
- [x] ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE (set operations with storage)

#### Streams (8 commands)
- [x] XADD, XREAD (with blocking), XLEN, XDEL
- [x] XTRIM, XRANGE, XREVRANGE

#### Pub/Sub (3 commands)
- [x] PUBLISH, PUBSUB NUMSUB, PUBSUB NUMPAT

#### Scripting (5 commands)
- [x] EVAL, EVALSHA
- [x] SCRIPT LOAD, SCRIPT EXISTS, SCRIPT FLUSH

#### Scan (2 commands)
- [x] SCAN, HSCAN
- Note: SSCAN, ZSCAN in respective modules

#### Connection (8 commands) ✅ EXPANDED
- [x] AUTH, AUTH (ACL)
- [x] READONLY, READWRITE
- [x] SELECT, QUIT
- [x] CLIENT GETNAME, CLIENT SETNAME

#### Sentinel (4 commands)
- [x] SENTINEL GET-MASTER-ADDR-BY-NAME
- [x] SENTINEL REPLICAS, SENTINEL SENTINELS
- [x] ROLE

#### HyperLogLog (3 commands) ✅ NEW
- [x] PFADD, PFCOUNT, PFMERGE

#### Bitmap (5 commands) ✅ NEW
- [x] SETBIT, GETBIT, BITCOUNT
- [x] BITOP, BITPOS

#### Keys (15 commands) ✅ EXPANDED
- [x] PERSIST, PEXPIRE, PTTL
- [x] EXPIREAT, PEXPIREAT
- [x] RENAME, RENAMENX
- [x] TYPE, KEYS
- [x] TOUCH (update access time)
- [x] UNLINK (async delete)
- [x] COPY (Redis 6.2+)
- [x] MOVE (move to different DB)
- [x] EXPIRETIME, PEXPIRETIME (Redis 7.0+)

#### Geospatial (5 commands) ✅ COMPLETED
- [x] GEOADD (add coordinates)
- [x] GEODIST (distance between members)
- [x] GEOHASH (geohash representation)
- [x] GEOPOS (get coordinates)
- [x] GEOSEARCH (modern radius/box queries with options)

#### Server/Admin (9 commands) ✅ EXPANDED
- [x] DBSIZE (count keys in database)
- [x] FLUSHDB, FLUSHALL (delete all keys - with ASYNC option)
- [x] RANDOMKEY (get random key)
- [x] TIME (server time as seconds + microseconds)
- [x] LASTSAVE (timestamp of last save)
- [x] SAVE, BGSAVE (persistence)
- [x] INFO (server information)

---

## ✅ Recently Completed

### Today's Session (2025-10-24) - 29 Commands Added! 🎉

#### Core Commands (23 commands)
- [x] **Advanced Strings (4):** SETEX, PSETEX, SETNX, MSETNX
- [x] **Advanced Lists (6):** LPUSHX, RPUSHX, LMOVE, BLMOVE, LMPOP, BLMPOP
- [x] **Sorted Sets (7):** ZREVRANGEBYSCORE, ZRANGEBYLEX, ZREVRANGEBYLEX, ZLEXCOUNT, ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZREMRANGEBYRANK
- [x] **Server/Admin (6):** DBSIZE, FLUSHDB, FLUSHALL, RANDOMKEY, TIME, LASTSAVE
- Brought string commands from 23 to 27
- Brought list commands from 16 to 22
- Brought sorted set commands from 17 to 24 (✅ COMPLETE)
- Added server/admin commands: 0 to 6

#### Module Support - RedisBloom (11 commands) 🌸 ✅ COMPLETE
- [x] **Feature-gated module pattern established**
- [x] BF.RESERVE, BF.ADD, BF.MADD
- [x] BF.EXISTS, BF.MEXISTS, BF.INFO
- [x] BF.INSERT (with builder pattern)
- [x] BF.CARD (cardinality)
- [x] BF.SCANDUMP, BF.LOADCHUNK (incremental save/restore)
- [x] BF.DEBUG (debug information)
- First complete module implementation with proper feature flags
- Can be enabled with `features = ["bloom"]`
- All 11 bloom filter commands implemented (100%)

**Total coverage:** 135 → 185 commands (34% → 46%)

#### Deprecated Commands (3 commands) - Feature-Gated 🔒
- [x] GETSET (use SET with GET option)
- [x] RPOPLPUSH (use LMOVE)
- [x] BRPOPLPUSH (use BLMOVE)
- Available with `features = ["deprecated"]` for backwards compatibility
- Each has clear migration guide in documentation

### Transactions (5 commands) - Previous Session
- [x] MULTI - Start transaction block
- [x] EXEC - Execute queued commands (returns Option<Vec<RedisValue>>)
- [x] DISCARD - Abort transaction
- [x] WATCH - Watch keys for conditional execution
- [x] UNWATCH - Clear watched keys
- Note: Both direct command usage and Transaction builder supported

---

## ⏳ High Priority (Production Use)
- [ ] BZPOPMIN, BZPOPMAX (blocking)
- [ ] ZRANGEBYSCORE, ZREVRANGEBYSCORE
- [ ] ZRANGEBYLEX, ZREVRANGEBYLEX
- [ ] ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZREMRANGEBYRANK
- [ ] ZCOUNT, ZLEXCOUNT
- [ ] ZMSCORE, ZRANDMEMBER
- [ ] ZUNION, ZINTER, ZDIFF (with stores)

### Server/Admin (15+ commands)
- [ ] INFO
- [ ] CONFIG GET, CONFIG SET
- [ ] CLIENT LIST, CLIENT KILL, CLIENT SETNAME
- [x] DBSIZE
- [x] FLUSHDB, FLUSHALL
- [ ] SAVE, BGSAVE
- [x] LASTSAVE
- [ ] SLOWLOG GET, SLOWLOG LEN, SLOWLOG RESET
- [ ] MEMORY STATS, MEMORY USAGE
- [x] TIME
- [x] RANDOMKEY

### Key Management (Additional - 10 commands)
- [x] TOUCH (update access time)
- [x] UNLINK (async delete)
- [x] COPY (Redis 6.2+)
- [x] MOVE (move to different DB)
- [x] EXPIRETIME, PEXPIRETIME (Redis 7.0+)
- [ ] DUMP, RESTORE
- [ ] MIGRATE
- [ ] WAIT, WAITAOF

---

## 🔮 Medium Priority

### Advanced Strings (10 commands)
- [ ] SETEX, PSETEX (can use SET with EX/PX)
- [ ] SETNX (can use SET with NX)
- [ ] MSETNX
- [ ] GETSET (deprecated, use SET with GET)
- [ ] LCS (longest common subsequence)
- [ ] SUBSTR (deprecated, use GETRANGE)

### Advanced Lists (6 commands)
- [ ] LMPOP, BLMPOP (Redis 7.0+)
- [ ] LMOVE, BLMOVE
- [ ] RPOPLPUSH, BRPOPLPUSH (deprecated)
- [ ] LPUSHX, RPUSHX

### Advanced Hashes (8 commands - Redis 7.4+)
- [ ] HEXPIRE, HPEXPIRE
- [ ] HEXPIREAT, HPEXPIREAT
- [ ] HEXPIRETIME, HPEXPIRETIME
- [ ] HTTL, HPTTL
- [ ] HPERSIST
- [ ] HSETNX
- [ ] HRANDFIELD

### Pub/Sub Extended (8 commands)
- [ ] SUBSCRIBE, UNSUBSCRIBE
- [ ] PSUBSCRIBE, PUNSUBSCRIBE
- [ ] SSUBSCRIBE, SUNSUBSCRIBE (sharded)
- [ ] SPUBLISH (sharded)
- [ ] PUBSUB CHANNELS, PUBSUB SHARDCHANNELS, PUBSUB SHARDNUMSUB

### Streams Extended (12 commands)
- [ ] XACK
- [ ] XGROUP CREATE, XGROUP DESTROY, XGROUP SETID
- [ ] XGROUP CREATECONSUMER, XGROUP DELCONSUMER
- [ ] XCLAIM, XAUTOCLAIM
- [ ] XPENDING
- [ ] XINFO STREAM, XINFO GROUPS, XINFO CONSUMERS
- [ ] XSETID

### Cluster (20+ commands)
- [ ] CLUSTER NODES, CLUSTER INFO
- [ ] CLUSTER SLOTS (partially implemented)
- [ ] CLUSTER MEET, CLUSTER FORGET
- [ ] CLUSTER ADDSLOTS, CLUSTER DELSLOTS
- [ ] CLUSTER SETSLOT
- [ ] CLUSTER REPLICATE, CLUSTER FAILOVER
- [ ] CLUSTER RESET
- [ ] Many more cluster management commands

### Scan Family (2 remaining)
- [ ] ZSCAN (in sorted_sets module, check if implemented)
- [ ] SSCAN (in sets module, check if implemented)

---

## 🔬 Low Priority (Specialized)

### Bitfields (2 commands)
- [ ] BITFIELD
- [ ] BITFIELD_RO

### ACL (11 commands)
- [ ] ACL LIST, ACL USERS, ACL GETUSER
- [ ] ACL SETUSER, ACL DELUSER
- [ ] ACL CAT, ACL WHOAMI
- [ ] ACL GENPASS
- [ ] ACL LOG
- [ ] ACL SAVE, ACL LOAD
- [ ] ACL DRYRUN

### Functions (9 commands - Redis 7.0+)
- [ ] FUNCTION LOAD, FUNCTION DELETE
- [ ] FUNCTION LIST, FUNCTION DUMP, FUNCTION RESTORE
- [ ] FUNCTION FLUSH, FUNCTION KILL
- [ ] FUNCTION STATS
- [ ] FCALL, FCALL_RO
- [ ] TFCALL, TFCALLASYNC (triggers)

### Generic Utilities (10 commands)
- [ ] TIME
- [ ] HELLO
- [ ] COMMAND (and subcommands)
- [ ] LATENCY (and subcommands)
- [ ] MEMORY (and subcommands)
- [ ] MODULE (and subcommands)
- [ ] OBJECT (and subcommands)
- [ ] DEBUG (and subcommands)
- [ ] MONITOR
- [ ] RESET

### Replication (6 commands)
- [ ] REPLICAOF, SLAVEOF
- [ ] ROLE (already implemented for Sentinel)
- [ ] PSYNC, SYNC
- [ ] REPLCONF
- [ ] FAILOVER

### Persistence (5 commands)
- [ ] SAVE, BGSAVE, LASTSAVE
- [ ] BGREWRITEAOF
- [ ] SHUTDOWN

### Other (5 commands)
- [ ] ASKING (cluster redirects)
- [ ] READONLY, READWRITE (already implemented)
- [ ] RESTORE-ASKING
- [ ] SWAPDB
- [ ] LOLWUT

---

## 🚫 Not Implementing (Modules)

### RedisJSON (30+ commands)
- JSON.SET, JSON.GET, JSON.DEL, JSON.MGET, JSON.MSET
- JSON.TYPE, JSON.NUMINCRBY, JSON.NUMMULTBY
- JSON.ARRAPPEND, JSON.ARRINDEX, JSON.ARRINSERT
- JSON.ARRLEN, JSON.ARRPOP, JSON.ARRTRIM
- JSON.OBJKEYS, JSON.OBJLEN
- JSON.STRAPPEND, JSON.STRLEN
- JSON.TOGGLE, JSON.CLEAR, JSON.DEBUG
- JSON.FORGET, JSON.RESP, JSON.MERGE, JSON.NUMPOWBY
- **Status:** Feature-gated module (issue #11)

### RediSearch (40+ commands)
- FT.CREATE, FT.SEARCH, FT.AGGREGATE
- FT.INFO, FT.EXPLAIN, FT.PROFILE
- FT.ALTER, FT.DROPINDEX, FT.DROP
- FT.ALIASADD, FT.ALIASUPDATE, FT.ALIASDEL
- FT.SUGADD, FT.SUGGET, FT.SUGDEL, FT.SUGLEN
- FT.SYNUPDATE, FT.SYNDUMP, FT.SYNADD
- FT.DICTADD, FT.DICTDEL, FT.DICTDUMP
- FT.CONFIG, FT.SPELLCHECK, FT.TAGVALS
- Many FT.DEBUG subcommands
- **Status:** Feature-gated module (issue #12)

### RedisTimeSeries (15+ commands)
- TS.CREATE, TS.ADD, TS.MADD, TS.INCRBY, TS.DECRBY
- TS.RANGE, TS.REVRANGE, TS.MRANGE, TS.MREVRANGE
- TS.GET, TS.MGET, TS.INFO
- TS.QUERYINDEX, TS.DEL, TS.ALTER
- TS.CREATERULE, TS.DELETERULE
- TIMESERIES.* cluster commands
- **Status:** Feature-gated module (issue #14)

### RedisBloom (25+ commands)
**Feature:** `bloom` - Enable with `features = ["bloom"]`

#### Bloom Filter (11/11 commands) ✅ COMPLETE
- [x] BF.RESERVE - Create filter with custom parameters
- [x] BF.ADD - Add single item
- [x] BF.MADD - Add multiple items
- [x] BF.EXISTS - Check single item
- [x] BF.MEXISTS - Check multiple items
- [x] BF.INFO - Get filter information
- [x] BF.INSERT - Add with options (CAPACITY, ERROR, NOCREATE, NONSCALING)
- [x] BF.CARD - Get cardinality (number of items)
- [x] BF.SCANDUMP - Dump filter for migration
- [x] BF.LOADCHUNK - Load dumped data
- [x] BF.DEBUG - Debug information

#### Cuckoo Filter (0/14 commands)
- [ ] CF.RESERVE, CF.ADD, CF.ADDNX, CF.INSERT, CF.INSERTNX
- [ ] CF.EXISTS, CF.MEXISTS, CF.DEL, CF.COUNT, CF.INFO
- [ ] CF.SCANDUMP, CF.LOADCHUNK, CF.DEBUG, CF.COMPACT

#### Count-Min Sketch (0/6 commands)
- [ ] CMS.INITBYDIM, CMS.INITBYPROB, CMS.INCRBY
- [ ] CMS.QUERY, CMS.MERGE, CMS.INFO

#### Top-K (0/7 commands)
- [ ] TOPK.RESERVE, TOPK.ADD, TOPK.INCRBY
- [ ] TOPK.QUERY, TOPK.COUNT, TOPK.LIST, TOPK.INFO

#### T-Digest (0/~10 commands)
- [ ] TDIGEST.* commands (quantile estimation)

**Status:** 6 commands implemented, feature-gated behind `bloom` feature

### RedisGears (10+ commands)
- REDISGEARS_2.* commands
- _RG_INTERNALS.* commands
- **Status:** Not planning to implement

---

## 📊 Statistics

### By Implementation Status
- ✅ Implemented: 120 commands
- 🚧 In Progress: 7 commands (geospatial)
- ⏳ High Priority: ~68 commands
- 🔮 Medium Priority: ~80 commands
- 🔬 Low Priority: ~60 commands
- 🚫 Modules: ~150+ commands (separate feature gates)
- **Total Core Redis:** ~400 commands (excluding modules)
- **Coverage:** 120/400 = **30% of core commands**

### By Complexity Level
- **Level 1** (Simple): 14 implemented / ~30 total
- **Level 2** (Multi-value): 17 implemented / ~50 total
- **Level 3** (Complex structures): 4 implemented / ~20 total
- **Level 4** (Stateful/blocking): 6 implemented / ~30 total
- **Level 5** (Cluster/scripts): 7 implemented / ~40 total

---

## 🎯 Next Steps

### Immediate (Today)
1. ✅ Complete key management commands (9 commands) - DONE
2. 🚧 Implement geospatial commands (7-8 commands) - IN PROGRESS
3. Target: 127 commands by end of today

### This Week
1. Advanced sorted sets (8-10 most common commands)
2. Transaction support (MULTI/EXEC/DISCARD)
3. Server commands (INFO, DBSIZE, CONFIG basics)
4. Target: 150+ commands

### Next Week
1. Remaining sorted set commands
2. Extended streams support (XGROUP, XACK)
3. Advanced list commands
4. Target: 180+ commands

### Goal: v0.1.0 Release
- **Target:** 200+ core commands (50% coverage)
- **Must have:**
  - All basic data types complete
  - Transaction support
  - Basic server/admin commands
  - Production-ready features (INFO, CONFIG, CLIENT)
- **Nice to have:**
  - Extended pub/sub
  - Advanced streams
  - Cluster commands beyond basics

---

## 📝 Notes

### Command Naming Conventions
- Command structs use PascalCase: `Get`, `HSet`, `PfAdd`
- Redis commands use uppercase: `GET`, `HSET`, `PFADD`
- Module re-exports use command names directly

### Testing Standards
- Every command must have frame generation test
- Every command must have response parsing test
- Complex commands need multiple test cases
- Current test count: 122 tests

### Documentation Standards
- Every command struct has doc comment
- Examples in doc comments where helpful
- Return value semantics documented
- Deprecation warnings where applicable

### GitHub Issue Tracking
- Issue #2: Commands Coverage (umbrella)
- Issue #4: Key Commands - COMPLETED
- Issue #5: Server Commands
- Issue #6: HyperLogLog Commands - COMPLETED
- Issue #7: Geospatial Commands - IN PROGRESS
- Issue #11-14: Module commands (future)
