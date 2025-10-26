# Redis-Tower Command Coverage Report

**Generated**: 2025-10-26

**Total Redis Commands**: 370

**Implemented**: 311 (84%)

**Missing**: 59 (15%)

## Summary

The redis-tower project has implemented **311 out of 370 Redis commands** (84% coverage). This analysis is based on the official Redis commands.json from redis-doc.

## Coverage by Category

| Category | Implemented | Missing | Total | Coverage |
|----------|-------------|---------|-------|----------|
| ACL | 11 | 3 | 14 | 78% |
| Bitmap | 5 | 2 | 7 | 71% |
| Cluster | 24 | 5 | 29 | 82% |
| Connection/Client | 23 | 4 | 27 | 85% |
| Functions | 9 | 3 | 12 | 75% |
| Generic | 2 | 0 | 2 | 100% |
| Geospatial | 6 | 4 | 10 | 60% |
| Hash | 15 | 1 | 16 | 93% |
| HyperLogLog | 3 | 2 | 5 | 60% |
| Keys | 24 | 1 | 25 | 96% |
| Latency | 6 | 2 | 8 | 75% |
| List | 20 | 0 | 20 | 100% |
| Module | 4 | 2 | 6 | 66% |
| Other | 13 | 6 | 19 | 68% |
| Pub/Sub | 13 | 2 | 15 | 86% |
| Replication | 1 | 0 | 1 | 100% |
| Scripting | 7 | 4 | 11 | 63% |
| Server | 24 | 9 | 33 | 72% |
| Set | 22 | 2 | 24 | 91% |
| Sorted Set | 35 | 0 | 35 | 100% |
| Streams | 19 | 6 | 25 | 76% |
| String | 20 | 1 | 21 | 95% |
| Transactions | 5 | 0 | 5 | 100% |

## Missing Commands by Category

### ACL (3 missing)

- `ACL` -> Expected struct: `Acl`
- `ACL DRYRUN` -> Expected struct: `AclDryrun`
- `ACL HELP` -> Expected struct: `AclHelp`

### Bitmap (2 missing)

- `BITFIELD_RO` -> Expected struct: `Bitfield_ro`
- `BITOP` -> Expected struct: `Bitop`

### Cluster (5 missing)

- `CLUSTER` -> Expected struct: `Cluster`
- `CLUSTER COUNT-FAILURE-REPORTS` -> Expected struct: `ClusterCount-failure-reports`
- `CLUSTER HELP` -> Expected struct: `ClusterHelp`
- `CLUSTER SET-CONFIG-EPOCH` -> Expected struct: `ClusterSet-config-epoch`
- `CLUSTER SLAVES` -> Expected struct: `ClusterSlaves`

### Connection/Client (4 missing)

- `CLIENT` -> Expected struct: `Client`
- `CLIENT HELP` -> Expected struct: `ClientHelp`
- `CLIENT NO-EVICT` -> Expected struct: `ClientNo-evict`
- `CLIENT NO-TOUCH` -> Expected struct: `ClientNo-touch`

### Functions (3 missing)

- `FCALL_RO` -> Expected struct: `Fcall_ro`
- `FUNCTION` -> Expected struct: `Function`
- `FUNCTION HELP` -> Expected struct: `FunctionHelp`

### Geospatial (4 missing)

- `GEORADIUS` -> Expected struct: `Georadius`
- `GEORADIUSBYMEMBER` -> Expected struct: `Georadiusbymember`
- `GEORADIUSBYMEMBER_RO` -> Expected struct: `Georadiusbymember_ro`
- `GEORADIUS_RO` -> Expected struct: `Georadius_ro`

### Hash (1 missing)

- `HMSET` -> Expected struct: `Hmset`

### HyperLogLog (2 missing)

- `PFDEBUG` -> Expected struct: `Pfdebug`
- `PFSELFTEST` -> Expected struct: `Pfselftest`

### Keys (1 missing)

- `OBJECT` -> Expected struct: `Object`

### Latency (2 missing)

- `LATENCY` -> Expected struct: `Latency`
- `LATENCY LATEST` -> Expected struct: `LatencyLatest`

### Module (2 missing)

- `MODULE` -> Expected struct: `Module`
- `MODULE HELP` -> Expected struct: `ModuleHelp`

### Other (6 missing)

- `LOLWUT` -> Expected struct: `Lolwut`
- `MEMORY` -> Expected struct: `Memory`
- `MEMORY HELP` -> Expected struct: `MemoryHelp`
- `MEMORY MALLOC-STATS` -> Expected struct: `MemoryMalloc-stats`
- `OBJECT HELP` -> Expected struct: `ObjectHelp`
- `REPLCONF` -> Expected struct: `Replconf`

### Pub/Sub (2 missing)

- `PUBSUB` -> Expected struct: `Pubsub`
- `PUBSUB HELP` -> Expected struct: `PubsubHelp`

### Scripting (4 missing)

- `EVALSHA_RO` -> Expected struct: `Evalsha_ro`
- `EVAL_RO` -> Expected struct: `Eval_ro`
- `SCRIPT` -> Expected struct: `Script`
- `SCRIPT HELP` -> Expected struct: `ScriptHelp`

### Server (9 missing)

- `COMMAND` -> Expected struct: `Command`
- `COMMAND HELP` -> Expected struct: `CommandHelp`
- `CONFIG` -> Expected struct: `Config`
- `CONFIG HELP` -> Expected struct: `ConfigHelp`
- `PSYNC` -> Expected struct: `Psync`
- `SLAVEOF` -> Expected struct: `Slaveof`
- `SLOWLOG` -> Expected struct: `Slowlog`
- `SWAPDB` -> Expected struct: `Swapdb`
- `SYNC` -> Expected struct: `Sync`

### Set (2 missing)

- `SLOWLOG HELP` -> Expected struct: `SlowlogHelp`
- `SORT_RO` -> Expected struct: `Sort_ro`

### Streams (6 missing)

- `XAUTOCLAIM` -> Expected struct: `Xautoclaim`
- `XGROUP` -> Expected struct: `Xgroup`
- `XGROUP HELP` -> Expected struct: `XgroupHelp`
- `XINFO` -> Expected struct: `Xinfo`
- `XINFO HELP` -> Expected struct: `XinfoHelp`
- `XSETID` -> Expected struct: `Xsetid`

### String (1 missing)

- `SUBSTR` -> Expected struct: `Substr`

## Implemented Commands by Category

### ACL (11 implemented)

- `ACL CAT` (struct: `AclCat`)
- `ACL DELUSER` (struct: `AclDelUser`)
- `ACL GENPASS` (struct: `AclGenPass`)
- `ACL GETUSER` (struct: `AclGetUser`)
- `ACL LIST` (struct: `AclList`)
- `ACL LOAD` (struct: `AclLoad`)
- `ACL LOG` (struct: `AclLog`)
- `ACL SAVE` (struct: `AclSave`)
- `ACL SETUSER` (struct: `AclSetUser`)
- `ACL USERS` (struct: `AclUsers`)
- `ACL WHOAMI` (struct: `AclWhoAmI`)

### Bitmap (5 implemented)

- `BITCOUNT` (struct: `BitCount`)
- `BITFIELD` (struct: `Bitfield`)
- `BITPOS` (struct: `BitPos`)
- `GETBIT` (struct: `GetBit`)
- `SETBIT` (struct: `SetBit`)

### Cluster (24 implemented)

- `CLUSTER ADDSLOTS` (struct: `ClusterAddSlots`)
- `CLUSTER ADDSLOTSRANGE` (struct: `ClusterAddSlotsRange`)
- `CLUSTER BUMPEPOCH` (struct: `ClusterBumpEpoch`)
- `CLUSTER COUNTKEYSINSLOT` (struct: `ClusterCountKeysInSlot`)
- `CLUSTER DELSLOTS` (struct: `ClusterDelSlots`)
- `CLUSTER DELSLOTSRANGE` (struct: `ClusterDelSlotsRange`)
- `CLUSTER FAILOVER` (struct: `ClusterFailover`)
- `CLUSTER FLUSHSLOTS` (struct: `ClusterFlushSlots`)
- `CLUSTER FORGET` (struct: `ClusterForget`)
- `CLUSTER GETKEYSINSLOT` (struct: `ClusterGetKeysInSlot`)
- `CLUSTER INFO` (struct: `ClusterInfo`)
- `CLUSTER KEYSLOT` (struct: `ClusterKeySlot`)
- `CLUSTER LINKS` (struct: `ClusterLinks`)
- `CLUSTER MEET` (struct: `ClusterMeet`)
- `CLUSTER MYID` (struct: `ClusterMyId`)
- `CLUSTER MYSHARDID` (struct: `ClusterMyShardId`)
- `CLUSTER NODES` (struct: `ClusterNodes`)
- `CLUSTER REPLICAS` (struct: `ClusterReplicas`)
- `CLUSTER REPLICATE` (struct: `ClusterReplicate`)
- `CLUSTER RESET` (struct: `ClusterReset`)
- `CLUSTER SAVECONFIG` (struct: `ClusterSaveConfig`)
- `CLUSTER SETSLOT` (struct: `ClusterSetSlot`)
- `CLUSTER SHARDS` (struct: `ClusterShards`)
- `CLUSTER SLOTS` (struct: `ClusterSlots`)

### Connection/Client (23 implemented)

- `ASKING` (struct: `Asking`)
- `AUTH` (struct: `Auth`)
- `CLIENT CACHING` (struct: `ClientCaching`)
- `CLIENT GETNAME` (struct: `ClientGetName`)
- `CLIENT GETREDIR` (struct: `ClientGetRedir`)
- `CLIENT ID` (struct: `ClientId`)
- `CLIENT INFO` (struct: `ClientInfo`)
- `CLIENT KILL` (struct: `ClientKill`)
- `CLIENT LIST` (struct: `ClientList`)
- `CLIENT PAUSE` (struct: `ClientPause`)
- `CLIENT REPLY` (struct: `ClientReply`)
- `CLIENT SETINFO` (struct: `ClientSetInfo`)
- `CLIENT SETNAME` (struct: `ClientSetName`)
- `CLIENT TRACKING` (struct: `ClientTracking`)
- `CLIENT TRACKINGINFO` (struct: `ClientTrackingInfo`)
- `CLIENT UNBLOCK` (struct: `ClientUnblock`)
- `CLIENT UNPAUSE` (struct: `ClientUnpause`)
- `HELLO` (struct: `Hello`)
- `QUIT` (struct: `Quit`)
- `READONLY` (struct: `ReadOnly`)
- `READWRITE` (struct: `ReadWrite`)
- `RESET` (struct: `Reset`)
- `SELECT` (struct: `Select`)

### Functions (9 implemented)

- `FCALL` (struct: `FCall`)
- `FUNCTION DELETE` (struct: `FunctionDelete`)
- `FUNCTION DUMP` (struct: `FunctionDump`)
- `FUNCTION FLUSH` (struct: `FunctionFlush`)
- `FUNCTION KILL` (struct: `FunctionKill`)
- `FUNCTION LIST` (struct: `FunctionList`)
- `FUNCTION LOAD` (struct: `FunctionLoad`)
- `FUNCTION RESTORE` (struct: `FunctionRestore`)
- `FUNCTION STATS` (struct: `FunctionStats`)

### Generic (2 implemented)

- `ECHO` (struct: `Echo`)
- `PING` (struct: `Ping`)

### Geospatial (6 implemented)

- `GEOADD` (struct: `GeoAdd`)
- `GEODIST` (struct: `GeoDist`)
- `GEOHASH` (struct: `GeoHash`)
- `GEOPOS` (struct: `GeoPos`)
- `GEOSEARCH` (struct: `GeoSearch`)
- `GEOSEARCHSTORE` (struct: `GeoSearchStore`)

### Hash (15 implemented)

- `HDEL` (struct: `HDel`)
- `HEXISTS` (struct: `HExists`)
- `HGET` (struct: `HGet`)
- `HGETALL` (struct: `HGetAll`)
- `HINCRBY` (struct: `HIncrBy`)
- `HINCRBYFLOAT` (struct: `HIncrByFloat`)
- `HKEYS` (struct: `HKeys`)
- `HLEN` (struct: `HLen`)
- `HMGET` (struct: `HMGet`)
- `HRANDFIELD` (struct: `HRandField`)
- `HSCAN` (struct: `HScan`)
- `HSET` (struct: `HSet`)
- `HSETNX` (struct: `HSetNx`)
- `HSTRLEN` (struct: `HStrLen`)
- `HVALS` (struct: `HVals`)

### HyperLogLog (3 implemented)

- `PFADD` (struct: `PfAdd`)
- `PFCOUNT` (struct: `PfCount`)
- `PFMERGE` (struct: `PfMerge`)

### Keys (24 implemented)

- `COPY` (struct: `Copy`)
- `DEL` (struct: `Del`)
- `DUMP` (struct: `Dump`)
- `EXISTS` (struct: `Exists`)
- `EXPIRE` (struct: `Expire`)
- `EXPIREAT` (struct: `ExpireAt`)
- `EXPIRETIME` (struct: `ExpireTime`)
- `KEYS` (struct: `Keys`)
- `MIGRATE` (struct: `Migrate`)
- `MOVE` (struct: `Move`)
- `PERSIST` (struct: `Persist`)
- `PEXPIRE` (struct: `PExpire`)
- `PEXPIREAT` (struct: `PExpireAt`)
- `PEXPIRETIME` (struct: `PExpireTime`)
- `PTTL` (struct: `PTtl`)
- `RANDOMKEY` (struct: `RandomKey`)
- `RENAME` (struct: `Rename`)
- `RENAMENX` (struct: `RenameNx`)
- `RESTORE` (struct: `Restore`)
- `RESTORE-ASKING` (struct: `RestoreAsking`)
- `TOUCH` (struct: `Touch`)
- `TTL` (struct: `Ttl`)
- `TYPE` (struct: `Type`)
- `UNLINK` (struct: `Unlink`)

### Latency (6 implemented)

- `LATENCY DOCTOR` (struct: `LatencyDoctor`)
- `LATENCY GRAPH` (struct: `LatencyGraph`)
- `LATENCY HELP` (struct: `LatencyHelp`)
- `LATENCY HISTOGRAM` (struct: `LatencyHistogram`)
- `LATENCY HISTORY` (struct: `LatencyHistory`)
- `LATENCY RESET` (struct: `LatencyReset`)

### List (20 implemented)

- `BLMOVE` (struct: `BLMove`)
- `BLMPOP` (struct: `BLMPop`)
- `BLPOP` (struct: `BLPop`)
- `BRPOP` (struct: `BRPop`)
- `BRPOPLPUSH` (struct: `BRPopLPush`)
- `LASTSAVE` (struct: `LastSave`)
- `LCS` (struct: `Lcs`)
- `LINDEX` (struct: `LIndex`)
- `LINSERT` (struct: `LInsert`)
- `LLEN` (struct: `LLen`)
- `LMOVE` (struct: `LMove`)
- `LMPOP` (struct: `LMPop`)
- `LPOP` (struct: `LPop`)
- `LPOS` (struct: `LPos`)
- `LPUSH` (struct: `LPush`)
- `LPUSHX` (struct: `LPushX`)
- `LRANGE` (struct: `LRange`)
- `LREM` (struct: `LRem`)
- `LSET` (struct: `LSet`)
- `LTRIM` (struct: `LTrim`)

### Module (4 implemented)

- `MODULE LIST` (struct: `ModuleList`)
- `MODULE LOAD` (struct: `ModuleLoad`)
- `MODULE LOADEX` (struct: `ModuleLoadEx`)
- `MODULE UNLOAD` (struct: `ModuleUnload`)

### Other (13 implemented)

- `DEBUG` (struct: `Debug`)
- `MEMORY DOCTOR` (struct: `MemoryDoctor`)
- `MEMORY PURGE` (struct: `MemoryPurge`)
- `MEMORY STATS` (struct: `MemoryStats`)
- `MEMORY USAGE` (struct: `MemoryUsage`)
- `OBJECT ENCODING` (struct: `ObjectEncoding`)
- `OBJECT FREQ` (struct: `ObjectFreq`)
- `OBJECT IDLETIME` (struct: `ObjectIdleTime`)
- `OBJECT REFCOUNT` (struct: `ObjectRefCount`)
- `RPOP` (struct: `RPop`)
- `RPOPLPUSH` (struct: `RPopLPush`)
- `RPUSH` (struct: `RPush`)
- `RPUSHX` (struct: `RPushX`)

### Pub/Sub (13 implemented)

- `PSUBSCRIBE` (struct: `Psubscribe`)
- `PUBLISH` (struct: `Publish`)
- `PUBSUB CHANNELS` (struct: `PubsubChannels`)
- `PUBSUB NUMPAT` (struct: `PubsubNumpat`)
- `PUBSUB NUMSUB` (struct: `PubsubNumsub`)
- `PUBSUB SHARDCHANNELS` (struct: `PubsubShardchannels`)
- `PUBSUB SHARDNUMSUB` (struct: `PubsubShardnumsub`)
- `PUNSUBSCRIBE` (struct: `Punsubscribe`)
- `SPUBLISH` (struct: `Spublish`)
- `SSUBSCRIBE` (struct: `Ssubscribe`)
- `SUBSCRIBE` (struct: `Subscribe`)
- `SUNSUBSCRIBE` (struct: `Sunsubscribe`)
- `UNSUBSCRIBE` (struct: `Unsubscribe`)

### Replication (1 implemented)

- `FAILOVER` (struct: `Failover`)

### Scripting (7 implemented)

- `EVAL` (struct: `Eval`)
- `EVALSHA` (struct: `EvalSha`)
- `SCRIPT DEBUG` (struct: `ScriptDebug`)
- `SCRIPT EXISTS` (struct: `ScriptExists`)
- `SCRIPT FLUSH` (struct: `ScriptFlush`)
- `SCRIPT KILL` (struct: `ScriptKill`)
- `SCRIPT LOAD` (struct: `ScriptLoad`)

### Server (24 implemented)

- `BGREWRITEAOF` (struct: `BgRewriteAof`)
- `BGSAVE` (struct: `BgSave`)
- `COMMAND COUNT` (struct: `CommandCount`)
- `COMMAND DOCS` (struct: `CommandDocs`)
- `COMMAND GETKEYS` (struct: `CommandGetKeys`)
- `COMMAND GETKEYSANDFLAGS` (struct: `CommandGetKeysAndFlags`)
- `COMMAND INFO` (struct: `CommandInfo`)
- `COMMAND LIST` (struct: `CommandList`)
- `CONFIG GET` (struct: `ConfigGet`)
- `CONFIG RESETSTAT` (struct: `ConfigResetStat`)
- `CONFIG REWRITE` (struct: `ConfigRewrite`)
- `CONFIG SET` (struct: `ConfigSet`)
- `DBSIZE` (struct: `DbSize`)
- `FLUSHALL` (struct: `FlushAll`)
- `FLUSHDB` (struct: `FlushDb`)
- `INFO` (struct: `Info`)
- `MONITOR` (struct: `Monitor`)
- `REPLICAOF` (struct: `ReplicaOf`)
- `ROLE` (struct: `Role`)
- `SAVE` (struct: `Save`)
- `SHUTDOWN` (struct: `Shutdown`)
- `TIME` (struct: `Time`)
- `WAIT` (struct: `Wait`)
- `WAITAOF` (struct: `WaitAof`)

### Set (22 implemented)

- `SADD` (struct: `Sadd`)
- `SCAN` (struct: `Scan`)
- `SCARD` (struct: `Scard`)
- `SDIFF` (struct: `Sdiff`)
- `SDIFFSTORE` (struct: `SDiffStore`)
- `SINTER` (struct: `Sinter`)
- `SINTERCARD` (struct: `SInterCard`)
- `SINTERSTORE` (struct: `SInterStore`)
- `SISMEMBER` (struct: `Sismember`)
- `SLOWLOG GET` (struct: `SlowlogGet`)
- `SLOWLOG LEN` (struct: `SlowlogLen`)
- `SLOWLOG RESET` (struct: `SlowlogReset`)
- `SMEMBERS` (struct: `Smembers`)
- `SMISMEMBER` (struct: `SMIsMember`)
- `SMOVE` (struct: `Smove`)
- `SORT` (struct: `Sort`)
- `SPOP` (struct: `Spop`)
- `SRANDMEMBER` (struct: `Srandmember`)
- `SREM` (struct: `Srem`)
- `SSCAN` (struct: `Sscan`)
- `SUNION` (struct: `Sunion`)
- `SUNIONSTORE` (struct: `SUnionStore`)

### Sorted Set (35 implemented)

- `BZMPOP` (struct: `BZMPop`)
- `BZPOPMAX` (struct: `BZPopMax`)
- `BZPOPMIN` (struct: `BZPopMin`)
- `ZADD` (struct: `Zadd`)
- `ZCARD` (struct: `Zcard`)
- `ZCOUNT` (struct: `ZCount`)
- `ZDIFF` (struct: `Zdiff`)
- `ZDIFFSTORE` (struct: `ZDiffStore`)
- `ZINCRBY` (struct: `Zincrby`)
- `ZINTER` (struct: `Zinter`)
- `ZINTERCARD` (struct: `ZInterCard`)
- `ZINTERSTORE` (struct: `ZInterStore`)
- `ZLEXCOUNT` (struct: `ZLexCount`)
- `ZMPOP` (struct: `ZMPop`)
- `ZMSCORE` (struct: `ZMScore`)
- `ZPOPMAX` (struct: `ZPopMax`)
- `ZPOPMIN` (struct: `ZPopMin`)
- `ZRANDMEMBER` (struct: `Zrandmember`)
- `ZRANGE` (struct: `Zrange`)
- `ZRANGEBYLEX` (struct: `ZRangeByLex`)
- `ZRANGEBYSCORE` (struct: `ZRangeByScore`)
- `ZRANGESTORE` (struct: `ZRangeStore`)
- `ZRANK` (struct: `Zrank`)
- `ZREM` (struct: `Zrem`)
- `ZREMRANGEBYLEX` (struct: `ZRemRangeByLex`)
- `ZREMRANGEBYRANK` (struct: `ZRemRangeByRank`)
- `ZREMRANGEBYSCORE` (struct: `ZRemRangeByScore`)
- `ZREVRANGE` (struct: `Zrevrange`)
- `ZREVRANGEBYLEX` (struct: `ZRevRangeByLex`)
- `ZREVRANGEBYSCORE` (struct: `ZRevRangeByScore`)
- `ZREVRANK` (struct: `Zrevrank`)
- `ZSCAN` (struct: `Zscan`)
- `ZSCORE` (struct: `Zscore`)
- `ZUNION` (struct: `Zunion`)
- `ZUNIONSTORE` (struct: `ZUnionStore`)

### Streams (19 implemented)

- `XACK` (struct: `XAck`)
- `XADD` (struct: `XAdd`)
- `XCLAIM` (struct: `XClaim`)
- `XDEL` (struct: `XDel`)
- `XGROUP CREATE` (struct: `XGroupCreate`)
- `XGROUP CREATECONSUMER` (struct: `XGroupCreateConsumer`)
- `XGROUP DELCONSUMER` (struct: `XGroupDelConsumer`)
- `XGROUP DESTROY` (struct: `XGroupDestroy`)
- `XGROUP SETID` (struct: `XGroupSetId`)
- `XINFO CONSUMERS` (struct: `XInfoConsumers`)
- `XINFO GROUPS` (struct: `XInfoGroups`)
- `XINFO STREAM` (struct: `XInfoStream`)
- `XLEN` (struct: `XLen`)
- `XPENDING` (struct: `XPending`)
- `XRANGE` (struct: `XRange`)
- `XREAD` (struct: `XRead`)
- `XREADGROUP` (struct: `XReadGroup`)
- `XREVRANGE` (struct: `XRevRange`)
- `XTRIM` (struct: `XTrim`)

### String (20 implemented)

- `APPEND` (struct: `Append`)
- `DECR` (struct: `Decr`)
- `DECRBY` (struct: `DecrBy`)
- `GET` (struct: `Get`)
- `GETDEL` (struct: `GetDel`)
- `GETEX` (struct: `GetEx`)
- `GETRANGE` (struct: `GetRange`)
- `GETSET` (struct: `GetSet`)
- `INCR` (struct: `Incr`)
- `INCRBY` (struct: `IncrBy`)
- `INCRBYFLOAT` (struct: `IncrByFloat`)
- `MGET` (struct: `MGet`)
- `MSET` (struct: `Mset`)
- `MSETNX` (struct: `Msetnx`)
- `PSETEX` (struct: `Psetex`)
- `SET` (struct: `Set`)
- `SETEX` (struct: `Setex`)
- `SETNX` (struct: `Setnx`)
- `SETRANGE` (struct: `SetRange`)
- `STRLEN` (struct: `StrLen`)

### Transactions (5 implemented)

- `DISCARD` (struct: `Discard`)
- `EXEC` (struct: `Exec`)
- `MULTI` (struct: `Multi`)
- `UNWATCH` (struct: `Unwatch`)
- `WATCH` (struct: `Watch`)

## Mapping Rules

Redis command names are converted to Rust struct names using these rules:

1. Space-separated commands: `ACL CAT` -> `AclCat`
2. Hyphen-separated commands: `COMMAND-DOCS` -> `CommandDocs`
3. Module commands with dots: `JSON.SET` -> `JsonSet`, `FT.SEARCH` -> `FtSearch`
4. Simple commands: `GET` -> `Get`
5. Case-insensitive matching is used to account for naming variations

## Notes

- Some commands may be intentionally excluded (deprecated, enterprise-only, etc.)
- Redis Stack modules (JSON, Search, TimeSeries, Graph, Bloom) are optional features
- Some struct names may differ from the expected mapping due to naming conventions
- This report compares against Redis's official commands.json schema
