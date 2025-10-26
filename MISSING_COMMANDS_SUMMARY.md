# Missing Commands Summary

**Quick Reference**: 311/370 commands implemented (84% coverage)

## Priority Commands to Implement

### High Priority (Core Functionality)
1. **OBJECT** - Inspect internal Redis object encoding (Keys category)
2. **BITOP** - Bitwise operations between strings (Bitmap category)
3. **HMSET** - Set multiple hash fields (deprecated but still used)

### Medium Priority (Advanced Features)
4. **XAUTOCLAIM** - Automatic stream message claiming (Streams)
5. **XINFO HELP** - Stream info help text
6. **XGROUP HELP** - Stream group help text
7. **GEORADIUS** - Query geo radius (deprecated in favor of GEOSEARCH)
8. **GEORADIUSBYMEMBER** - Query geo radius by member (deprecated)

### Low Priority (Help Commands & Diagnostics)
- Various `*_HELP` commands (14 total): ACL HELP, CLIENT HELP, CLUSTER HELP, etc.
- Base commands without subcommands: ACL, CLIENT, CLUSTER, COMMAND, CONFIG, etc.
- Debug/diagnostic: PFDEBUG, PFSELFTEST, LOLWUT, REPLCONF
- Deprecated: SLAVEOF, PSYNC, SYNC, GEORADIUS*, EVAL_RO, EVALSHA_RO

## Commands by Implementation Difficulty

### Easy (Straightforward implementations)
- `OBJECT` - Simple key inspection command
- `BITOP` - Binary operations on strings
- `HMSET` - Already have HSET, this is batch version
- `SORT_RO` - Read-only variant of existing SORT
- `SWAPDB` - Database swapping command

### Medium (Require new patterns)
- `XAUTOCLAIM` - Streams auto-claiming logic
- `FCALL_RO` - Read-only function calls
- `MEMORY MALLOC-STATS` - Memory statistics parsing
- `LATENCY LATEST` - Latency tracking results

### Hard (Complex or deprecated)
- `GEORADIUS*` commands - Deprecated, complex geo logic
- `PSYNC`, `SYNC` - Replication internals (low value for client)
- `REPLCONF` - Replication configuration (internal use)

## Categories with 100% Coverage ✓

These categories are **complete**:
- **Generic** (2/2): PING, ECHO
- **List** (20/20): All list commands
- **Sorted Set** (35/35): All sorted set commands
- **Transactions** (5/5): MULTI, EXEC, DISCARD, WATCH, UNWATCH
- **Replication** (1/1): FAILOVER

## Categories Nearly Complete (>90%)

- **Keys** (24/25 - 96%): Missing only OBJECT
- **String** (20/21 - 95%): Missing only LCS (already implemented!)
- **Hash** (15/16 - 93%): Missing only HMSET
- **Set** (22/24 - 91%): Missing SLOWLOG HELP, SORT_RO

## Deprecated Commands (Can Skip)

These commands are deprecated in Redis and can be skipped:
- `SLAVEOF` (use REPLICAOF)
- `GEORADIUS` (use GEOSEARCH)
- `GEORADIUSBYMEMBER` (use GEOSEARCH)
- `GEORADIUS_RO` (use GEOSEARCH)
- `GEORADIUSBYMEMBER_RO` (use GEOSEARCH)
- `HMSET` (use HSET)
- `EVAL_RO` / `EVALSHA_RO` (Redis 7.0+, rarely used)

## Help Commands (Low Priority)

Base commands and help subcommands (15 total):
- `ACL`, `ACL HELP`, `ACL DRYRUN`
- `CLIENT`, `CLIENT HELP`
- `CLUSTER`, `CLUSTER HELP`
- `COMMAND`, `COMMAND HELP`
- `CONFIG`, `CONFIG HELP`
- `FUNCTION`, `FUNCTION HELP`
- `LATENCY`
- `MEMORY`, `MEMORY HELP`
- `MODULE`, `MODULE HELP`
- `PUBSUB`, `PUBSUB HELP`
- `SCRIPT`, `SCRIPT HELP`
- `SLOWLOG`, `SLOWLOG HELP`
- `XGROUP`, `XGROUP HELP`
- `XINFO`

## Recommendations

### For v0.2.0 (High Value)
1. Implement `OBJECT` command (most useful missing command)
2. Implement `BITOP` for bitmap operations
3. Add `XAUTOCLAIM` for complete streams support

### For v0.3.0 (Completeness)
4. Add help commands for better CLI compatibility
5. Consider deprecated commands if users request them
6. Add internal/diagnostic commands (PSYNC, SYNC, REPLCONF) only if needed

### Can Skip
- Debug commands: PFDEBUG, PFSELFTEST, LOLWUT
- Deprecated geo commands (already have GEOSEARCH)
- Read-only eval variants (EVAL_RO, EVALSHA_RO)
- Most base commands without subcommands

## Conclusion

With **84% coverage**, redis-tower has excellent command support. The remaining 59 commands are:
- 15 help/base commands (low priority)
- 11 deprecated commands (can skip)
- 8 internal/diagnostic commands (low value)
- **25 genuine missing commands** worth implementing

Focus on the 25 genuine commands for near-complete coverage!
