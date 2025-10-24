# Redis-Tower Command Implementation Status

This document tracks the implementation status of Redis commands in redis-tower. Commands are organized by Redis category and marked with their implementation status.

Last Updated: 2025-10-23

## Summary Statistics

- **Total Commands Implemented**: 54 core commands + Cluster + Pub/Sub + Transactions
- **Coverage**: ~75% of commonly-used Redis commands
- **Production Ready**: Strings, Hashes, Lists, Sets, Streams, Scripting, Transactions, Cluster (foundation), Pub/Sub

## Legend

- ✅ Fully implemented and tested
- 🚧 Partially implemented
- ❌ Not implemented
- 🎯 High priority for next implementation

## Strings (14 commands)

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| GET | ✅ | Simple | Returns `Option<Bytes>` |
| SET | ✅ | Simple | Supports NX/XX/EX/PX/EXAT/PXAT/KEEPTTL |
| DEL | ✅ | Simple | Multi-key support |
| INCR | ✅ | Simple | Returns `i64` |
| DECR | ✅ | Simple | Returns `i64` |
| MGET | ✅ | Multi-Value | Returns `Vec<Option<Bytes>>` |
| MSET | ✅ | Multi-Value | Atomic multi-set |
| APPEND | ❌ | Simple | 🎯 |
| GETRANGE | ❌ | Simple | 🎯 |
| SETRANGE | ❌ | Simple | |
| STRLEN | ❌ | Simple | 🎯 |
| GETSET | ❌ | Simple | Deprecated (use SET with GET) |
| INCRBY | ❌ | Simple | 🎯 |
| DECRBY | ❌ | Simple | 🎯 |
| INCRBYFLOAT | ❌ | Simple | |
| SETEX | ❌ | Simple | Use SET with EX instead |
| SETNX | ❌ | Simple | Use SET with NX instead |
| MSETNX | ❌ | Multi-Value | |
| GETEX | ❌ | Simple | |
| GETDEL | ❌ | Simple | |
| LCS | ❌ | Complex | Longest common subsequence |

## Hashes (7 commands)

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| HGET | ✅ | Simple | Returns `Option<Bytes>` |
| HSET | ✅ | Multi-Value | Multi-field support |
| HGETALL | ✅ | Multi-Value | Returns `HashMap<String, Bytes>` |
| HDEL | ✅ | Multi-Value | Multi-field support |
| HSCAN | ✅ | Complex | Cursor-based iteration |
| HEXISTS | ❌ | Simple | 🎯 |
| HLEN | ❌ | Simple | 🎯 |
| HKEYS | ❌ | Simple | 🎯 |
| HVALS | ❌ | Simple | 🎯 |
| HMGET | ❌ | Multi-Value | 🎯 |
| HMSET | ❌ | Multi-Value | Use HSET instead (deprecated) |
| HINCRBY | ❌ | Simple | |
| HINCRBYFLOAT | ❌ | Simple | |
| HSTRLEN | ❌ | Simple | |
| HSETNX | ❌ | Simple | |
| HRANDFIELD | ❌ | Simple | |

## Lists (7 commands)

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| LPUSH | ✅ | Multi-Value | Multi-element support |
| RPUSH | ✅ | Multi-Value | Multi-element support |
| LPOP | ✅ | Simple | Returns `Option<Bytes>` |
| RPOP | ✅ | Simple | Returns `Option<Bytes>` |
| LRANGE | ✅ | Multi-Value | Returns `Vec<Bytes>` |
| BLPOP | ✅ | Stateful | Blocking with timeout |
| BRPOP | ✅ | Stateful | Blocking with timeout |
| LLEN | ❌ | Simple | 🎯 |
| LINDEX | ❌ | Simple | 🎯 |
| LSET | ❌ | Simple | |
| LINSERT | ❌ | Multi-Value | |
| LREM | ❌ | Multi-Value | |
| LTRIM | ❌ | Simple | |
| LPOS | ❌ | Complex | |
| RPOPLPUSH | ❌ | Simple | Use LMOVE instead |
| LMOVE | ❌ | Simple | |
| BLMOVE | ❌ | Stateful | |
| LMPOP | ❌ | Multi-Value | |
| BLMPOP | ❌ | Stateful | |

## Sets (9 commands)

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| SADD | ✅ | Multi-Value | Multi-member support |
| SREM | ✅ | Multi-Value | Multi-member support |
| SMEMBERS | ✅ | Multi-Value | Returns `Vec<Bytes>` |
| SISMEMBER | ✅ | Simple | Returns `bool` |
| SCARD | ✅ | Simple | Returns `i64` |
| SINTER | ✅ | Multi-Value | Returns `Vec<Bytes>` |
| SUNION | ✅ | Multi-Value | Returns `Vec<Bytes>` |
| SDIFF | ✅ | Multi-Value | Returns `Vec<Bytes>` |
| SSCAN | ✅ | Complex | Cursor-based iteration |
| SPOP | ❌ | Simple | 🎯 |
| SRANDMEMBER | ❌ | Simple | 🎯 |
| SMOVE | ❌ | Simple | |
| SINTERSTORE | ❌ | Multi-Value | |
| SUNIONSTORE | ❌ | Multi-Value | |
| SDIFFSTORE | ❌ | Multi-Value | |
| SMISMEMBER | ❌ | Multi-Value | |
| SINTERCARD | ❌ | Multi-Value | |

## Sorted Sets (2 commands)

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| ZADD | ✅ | Multi-Value | Multi-member support with NX/XX/GT/LT/CH/INCR |
| ZSCAN | ✅ | Complex | Cursor-based iteration |
| ZCARD | ❌ | Simple | 🎯 High priority |
| ZCOUNT | ❌ | Simple | 🎯 High priority |
| ZINCRBY | ❌ | Simple | 🎯 High priority |
| ZRANGE | ❌ | Complex | 🎯 High priority - supports BYSCORE/BYLEX/REV/LIMIT/WITHSCORES |
| ZREVRANGE | ❌ | Multi-Value | Use ZRANGE with REV instead |
| ZRANGEBYSCORE | ❌ | Complex | Use ZRANGE BYSCORE instead |
| ZREVRANGEBYSCORE | ❌ | Complex | Use ZRANGE BYSCORE REV instead |
| ZRANK | ❌ | Simple | 🎯 High priority |
| ZREVRANK | ❌ | Simple | 🎯 High priority |
| ZSCORE | ❌ | Simple | 🎯 High priority |
| ZREM | ❌ | Multi-Value | 🎯 High priority |
| ZREMRANGEBYRANK | ❌ | Simple | |
| ZREMRANGEBYSCORE | ❌ | Simple | |
| ZREMRANGEBYLEX | ❌ | Simple | |
| ZPOPMIN | ❌ | Simple | |
| ZPOPMAX | ❌ | Simple | |
| BZPOPMIN | ❌ | Stateful | |
| BZPOPMAX | ❌ | Stateful | |
| ZINTER | ❌ | Complex | |
| ZUNION | ❌ | Complex | |
| ZDIFF | ❌ | Complex | |
| ZINTERSTORE | ❌ | Complex | |
| ZUNIONSTORE | ❌ | Complex | |
| ZDIFFSTORE | ❌ | Complex | |
| ZRANGESTORE | ❌ | Complex | |
| ZMSCORE | ❌ | Multi-Value | |
| ZLEXCOUNT | ❌ | Simple | |
| ZRANDMEMBER | ❌ | Simple | |
| ZMPOP | ❌ | Multi-Value | |
| BZMPOP | ❌ | Stateful | |

## Streams (7 commands)

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| XADD | ✅ | Stateful | Auto-generated IDs, MAXLEN/MINID trimming |
| XREAD | ✅ | Stateful | Non-blocking and blocking with BLOCK, COUNT support |
| XLEN | ✅ | Simple | Get stream length |
| XDEL | ✅ | Multi-Value | Delete entries by ID |
| XTRIM | ✅ | Complex | Trim by MAXLEN or MINID, exact or approximate |
| XRANGE | ✅ | Multi-Value | Range query with COUNT support |
| XREVRANGE | ✅ | Multi-Value | Reverse range query with COUNT support |
| XREADGROUP | ❌ | Complex | Consumer groups |
| XACK | ❌ | Multi-Value | Consumer groups |
| XGROUP CREATE | ❌ | Simple | Consumer groups |
| XGROUP DESTROY | ❌ | Simple | Consumer groups |
| XGROUP SETID | ❌ | Simple | Consumer groups |
| XGROUP DELCONSUMER | ❌ | Simple | Consumer groups |
| XPENDING | ❌ | Complex | Consumer groups |
| XCLAIM | ❌ | Complex | Consumer groups |
| XAUTOCLAIM | ❌ | Complex | Consumer groups |
| XINFO STREAM | ❌ | Complex | Stream metadata |
| XINFO GROUPS | ❌ | Complex | Consumer group metadata |
| XINFO CONSUMERS | ❌ | Complex | Consumer metadata |

## Pub/Sub (7 commands)

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| PUBLISH | ✅ | Simple | Returns number of subscribers |
| SUBSCRIBE | ✅ | Stateful | Dedicated PubSubConnection |
| UNSUBSCRIBE | ✅ | Stateful | Dedicated PubSubConnection |
| PSUBSCRIBE | ✅ | Stateful | Pattern subscriptions |
| PUNSUBSCRIBE | ✅ | Stateful | Pattern unsubscribe |
| PUBSUB CHANNELS | ✅ | Multi-Value | List active channels |
| PUBSUB NUMPAT | ✅ | Simple | Count pattern subscriptions |
| PUBSUB NUMSUB | ❌ | Multi-Value | Count subscribers per channel |
| PUBSUB SHARDCHANNELS | ❌ | Multi-Value | Sharded pub/sub (Redis 7.0+) |
| PUBSUB SHARDNUMSUB | ❌ | Multi-Value | Sharded pub/sub (Redis 7.0+) |
| SSUBSCRIBE | ❌ | Stateful | Sharded pub/sub (Redis 7.0+) |
| SUNSUBSCRIBE | ❌ | Stateful | Sharded pub/sub (Redis 7.0+) |
| SPUBLISH | ❌ | Simple | Sharded pub/sub (Redis 7.0+) |

## Transactions (3 commands)

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| MULTI | ✅ | Stateful | Transaction builder pattern |
| EXEC | ✅ | Stateful | Type-safe execution |
| DISCARD | ✅ | Stateful | Abort transaction |
| WATCH | ✅ | Stateful | Optimistic locking |
| UNWATCH | ✅ | Stateful | Clear watches |

## Scripting (5 commands)

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| EVAL | ✅ | Complex | Dynamic return types with RedisValue |
| EVALSHA | ✅ | Complex | SHA1 script caching |
| SCRIPT LOAD | ✅ | Simple | Pre-load scripts |
| SCRIPT EXISTS | ✅ | Multi-Value | Check script cache |
| SCRIPT FLUSH | ✅ | Simple | Clear script cache |
| SCRIPT KILL | ❌ | Simple | |
| SCRIPT DEBUG | ❌ | Simple | Debugging mode |
| EVALSHA_RO | ❌ | Complex | Read-only variant (Redis 7.0+) |
| EVAL_RO | ❌ | Complex | Read-only variant (Redis 7.0+) |
| FCALL | ❌ | Complex | Redis Functions (Redis 7.0+) |
| FCALL_RO | ❌ | Complex | Redis Functions (Redis 7.0+) |
| FUNCTION LOAD | ❌ | Simple | Redis Functions (Redis 7.0+) |
| FUNCTION DELETE | ❌ | Simple | Redis Functions (Redis 7.0+) |
| FUNCTION LIST | ❌ | Complex | Redis Functions (Redis 7.0+) |
| FUNCTION FLUSH | ❌ | Simple | Redis Functions (Redis 7.0+) |
| FUNCTION KILL | ❌ | Simple | Redis Functions (Redis 7.0+) |

## Cluster (4 commands + routing)

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| CLUSTER SLOTS | ✅ | Complex | Slot map for routing |
| CLUSTER NODES | ✅ | Complex | Node information |
| CLUSTER INFO | ✅ | Complex | Cluster state |
| ASKING | ✅ | Simple | ASK redirect handling |
| SlotMap | ✅ | Infrastructure | CRC16 slot calculation |
| KeyExtractor | ✅ | Infrastructure | 50+ commands support routing |
| ClusterClient | ✅ | Infrastructure | Automatic routing with MOVED/ASK redirects |
| CLUSTER ADDSLOTS | ❌ | Multi-Value | Admin command |
| CLUSTER DELSLOTS | ❌ | Multi-Value | Admin command |
| CLUSTER MEET | ❌ | Simple | Admin command |
| CLUSTER FORGET | ❌ | Simple | Admin command |
| CLUSTER REPLICATE | ❌ | Simple | Admin command |
| CLUSTER SAVECONFIG | ❌ | Simple | Admin command |
| CLUSTER SETSLOT | ❌ | Simple | Admin command |
| CLUSTER FAILOVER | ❌ | Simple | Admin command |
| CLUSTER RESET | ❌ | Simple | Admin command |
| CLUSTER COUNTKEYSINSLOT | ❌ | Simple | |
| CLUSTER GETKEYSINSLOT | ❌ | Multi-Value | |

## Keys (4 commands)

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| SCAN | ✅ | Complex | Cursor-based key iteration |
| EXISTS | ✅ | Simple | Multi-key support |
| TTL | ✅ | Simple | Returns seconds or -1/-2 |
| EXPIRE | ✅ | Simple | Set expiration in seconds |
| PTTL | ❌ | Simple | Millisecond precision |
| EXPIREAT | ❌ | Simple | Unix timestamp |
| PEXPIRE | ❌ | Simple | Millisecond precision |
| PEXPIREAT | ❌ | Simple | Unix timestamp milliseconds |
| PERSIST | ❌ | Simple | Remove expiration |
| KEYS | ❌ | Multi-Value | Use SCAN instead (blocking) |
| RANDOMKEY | ❌ | Simple | |
| RENAME | ❌ | Simple | |
| RENAMENX | ❌ | Simple | |
| TYPE | ❌ | Simple | 🎯 |
| DUMP | ❌ | Simple | Serialization |
| RESTORE | ❌ | Complex | Deserialization |
| MIGRATE | ❌ | Complex | Key migration |
| MOVE | ❌ | Simple | Database selection |
| OBJECT REFCOUNT | ❌ | Simple | Debugging |
| OBJECT ENCODING | ❌ | Simple | Debugging |
| OBJECT IDLETIME | ❌ | Simple | Debugging |
| OBJECT FREQ | ❌ | Simple | Debugging |
| OBJECT HELP | ❌ | Simple | Debugging |
| TOUCH | ❌ | Multi-Value | Update access time |
| UNLINK | ❌ | Multi-Value | Async DEL |
| WAIT | ❌ | Simple | Replication sync |
| WAITAOF | ❌ | Simple | AOF sync (Redis 7.2+) |
| COPY | ❌ | Simple | Copy key (Redis 6.2+) |
| EXPIRETIME | ❌ | Simple | Unix timestamp when key expires (Redis 7.0+) |
| PEXPIRETIME | ❌ | Simple | Millisecond precision (Redis 7.0+) |

## Server/Connection (2 commands)

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| PING | ✅ | Simple | Returns "PONG" or custom message |
| ECHO | ✅ | Simple | Returns echoed bytes |
| SELECT | ❌ | Simple | Database selection |
| QUIT | ❌ | Simple | Connection close |
| AUTH | ❌ | Simple | 🎯 Authentication |
| CLIENT SETNAME | ❌ | Simple | |
| CLIENT GETNAME | ❌ | Simple | |
| CLIENT LIST | ❌ | Complex | |
| CLIENT KILL | ❌ | Complex | |
| CLIENT PAUSE | ❌ | Simple | |
| CLIENT REPLY | ❌ | Simple | |
| CLIENT UNBLOCK | ❌ | Simple | |
| CLIENT ID | ❌ | Simple | |
| CLIENT INFO | ❌ | Complex | |
| HELLO | ❌ | Complex | RESP3 handshake |
| COMMAND | ❌ | Complex | Command metadata |
| COMMAND COUNT | ❌ | Simple | |
| COMMAND GETKEYS | ❌ | Multi-Value | |
| COMMAND INFO | ❌ | Complex | |
| INFO | ❌ | Complex | Server info |
| CONFIG GET | ❌ | Multi-Value | |
| CONFIG SET | ❌ | Simple | |
| CONFIG REWRITE | ❌ | Simple | |
| CONFIG RESETSTAT | ❌ | Simple | |
| DBSIZE | ❌ | Simple | |
| FLUSHDB | ❌ | Simple | |
| FLUSHALL | ❌ | Simple | |
| SAVE | ❌ | Simple | |
| BGSAVE | ❌ | Simple | |
| BGREWRITEAOF | ❌ | Simple | |
| LASTSAVE | ❌ | Simple | |
| SHUTDOWN | ❌ | Simple | |
| TIME | ❌ | Multi-Value | |
| ROLE | ❌ | Complex | |
| REPLICAOF | ❌ | Simple | |
| SLAVEOF | ❌ | Simple | Use REPLICAOF |
| MONITOR | ❌ | Stateful | |
| DEBUG OBJECT | ❌ | Complex | |
| DEBUG SEGFAULT | ❌ | Simple | |
| SLOWLOG GET | ❌ | Complex | |
| SLOWLOG LEN | ❌ | Simple | |
| SLOWLOG RESET | ❌ | Simple | |
| MEMORY DOCTOR | ❌ | Complex | |
| MEMORY STATS | ❌ | Complex | |
| MEMORY USAGE | ❌ | Simple | |
| MEMORY PURGE | ❌ | Simple | |
| MODULE LOAD | ❌ | Simple | |
| MODULE UNLOAD | ❌ | Simple | |
| MODULE LIST | ❌ | Complex | |

## HyperLogLog

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| PFADD | ❌ | Multi-Value | 🎯 Probabilistic counting |
| PFCOUNT | ❌ | Multi-Value | 🎯 |
| PFMERGE | ❌ | Multi-Value | 🎯 |
| PFDEBUG | ❌ | Complex | Debugging |
| PFSELFTEST | ❌ | Simple | Testing |

## Geospatial

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| GEOADD | ❌ | Complex | Add coordinates |
| GEOPOS | ❌ | Multi-Value | Get coordinates |
| GEODIST | ❌ | Simple | Distance calculation |
| GEORADIUS | ❌ | Complex | Deprecated - use GEOSEARCH |
| GEORADIUSBYMEMBER | ❌ | Complex | Deprecated - use GEOSEARCH |
| GEOHASH | ❌ | Multi-Value | |
| GEOSEARCH | ❌ | Complex | 🎯 Modern geospatial queries |
| GEOSEARCHSTORE | ❌ | Complex | |

## Bitmaps

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| SETBIT | ❌ | Simple | |
| GETBIT | ❌ | Simple | |
| BITCOUNT | ❌ | Simple | |
| BITPOS | ❌ | Simple | |
| BITOP | ❌ | Multi-Value | AND/OR/XOR/NOT |
| BITFIELD | ❌ | Complex | |
| BITFIELD_RO | ❌ | Complex | Read-only (Redis 6.2+) |

## JSON (RedisJSON Module)

| Command | Status | Type | Notes |
|---------|--------|------|-------|
| JSON.SET | ❌ | Complex | Requires RedisJSON module |
| JSON.GET | ❌ | Complex | |
| JSON.DEL | ❌ | Simple | |
| JSON.MGET | ❌ | Multi-Value | |
| JSON.TYPE | ❌ | Simple | |
| JSON.NUMINCRBY | ❌ | Simple | |
| JSON.NUMMULTBY | ❌ | Simple | |
| JSON.STRAPPEND | ❌ | Simple | |
| JSON.STRLEN | ❌ | Simple | |
| JSON.ARRAPPEND | ❌ | Multi-Value | |
| JSON.ARRINDEX | ❌ | Simple | |
| JSON.ARRINSERT | ❌ | Multi-Value | |
| JSON.ARRLEN | ❌ | Simple | |
| JSON.ARRPOP | ❌ | Simple | |
| JSON.ARRTRIM | ❌ | Simple | |
| JSON.OBJKEYS | ❌ | Multi-Value | |
| JSON.OBJLEN | ❌ | Simple | |

## Strategic Next Steps

Based on this analysis, here are recommended priorities:

### Immediate (High ROI):
1. **Streams completion** (XLEN, XDEL, XTRIM, XRANGE, XREVRANGE) - finish what we started
2. **Sorted Sets core** (ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZRANK, ZSCORE, ZREM) - essential for leaderboards/rankings
3. **String operations** (APPEND, STRLEN, INCRBY, DECRBY, GETRANGE) - common use cases
4. **Hash operations** (HEXISTS, HLEN, HKEYS, HVALS, HMGET) - complete the hash API

### Medium Priority:
5. **List operations** (LLEN, LINDEX) - round out list support
6. **Key operations** (TYPE, AUTH) - essential utilities
7. **HyperLogLog** (PFADD, PFCOUNT, PFMERGE) - unique counting use case
8. **Set operations** (SPOP, SRANDMEMBER) - random selection

### Future:
9. **Geospatial** - specialized use case but powerful
10. **Consumer Groups** - advanced streams features
11. **RedisJSON** - requires module, but very popular

### Infrastructure:
- Connection pooling with Tower Balance
- Pipeline support
- Client-side caching (RESP3 push notifications)
- Full cluster routing (MOVED/ASK retry logic)
- Sentinel support
- TLS/SSL support

## Testing Coverage

### Integration Tests (179 total):
- ✅ 57 unit tests (command encoding/parsing)
- ✅ 114 integration tests (9 categories)
  - Basic commands (strings, hashes, lists, sets)
  - Complex commands (SCAN iteration)
  - Level 4 commands (blocking, streams)
  - Sets operations (union, intersect, diff, scan)
  - Scripting (EVAL, EVALSHA, caching)
  - Transactions (MULTI/EXEC, WATCH, optimistic locking)
  - Cluster (8 tests with Docker cluster)
  - Pub/Sub (8 tests with patterns, binary data, timeouts)
  - Streams (14 tests: XADD, XLEN, XRANGE, XREVRANGE, XDEL, XTRIM)
- ✅ 8 doctests (examples in documentation)

### Examples:
- ✅ `basic.rs` - Simple commands
- ✅ `essential.rs` - Essential commands (PING, ECHO, EXISTS, TTL, EXPIRE, MSET)
- ✅ `sets.rs` - Set operations
- ✅ `transactions.rs` - Transaction patterns
- ✅ `commands.rs` - All data structures
- ✅ `complex_commands.rs` - SCAN iteration
- ✅ `level4_commands.rs` - Blocking & Streams
- ✅ `scripting.rs` - Lua scripts
- ✅ `resilient.rs` - Tower middleware

## Command Complexity Classification

### Level 1 (Simple) - Fixed args, single response
Examples: GET, SET, INCR, PING, HGET, LPUSH, SADD, EXISTS, TTL

### Level 2 (Multi-Value) - Arrays, variable args
Examples: MGET, MSET, HGETALL, LRANGE, SMEMBERS, SINTER

### Level 3 (Complex Structures) - Custom types, builders
Examples: SCAN, HSCAN, SSCAN, CLUSTER SLOTS, ZRANGE

### Level 4 (Stateful/Modal) - Blocking, streams, transactions
Examples: BLPOP, BRPOP, XADD, XREAD, WATCH, SUBSCRIBE

### Level 5 (Advanced) - Scripts, cluster, modules
Examples: EVAL, EVALSHA, CLUSTER commands, JSON commands

## Performance Targets

- Single command: < 1ms p99 latency
- Pipeline (10 commands): < 2ms p99 latency
- Throughput: > 100k ops/sec on localhost
- Memory: < 10MB overhead per 10k connections
- Zero-copy parsing via resp-parser integration

## Architecture Strengths

1. **Type Safety**: Every command knows its response type at compile time
2. **Zero-Copy**: resp-parser integration avoids allocations
3. **Tower Integration**: Composable middleware (timeout, retry, circuit breaker)
4. **RESP3 Support**: Full protocol support including Push types
5. **Cluster Ready**: CRC16 slots, MOVED/ASK redirects, key extraction
6. **Pub/Sub**: Dedicated connection with async message streaming
7. **Transactions**: Type-safe MULTI/EXEC builder with optimistic locking
8. **Scripting**: Dynamic return types with SHA1 caching
