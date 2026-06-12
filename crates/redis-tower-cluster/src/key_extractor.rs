//! Extract the routing key from a command frame.
//!
//! For cluster routing we need the command's first key so we can compute its
//! hash slot. Most commands follow `COMMAND key [args...]`, so the key is at
//! `argv[1]` -- but a meaningful minority do not, and blindly hashing
//! `argv[1]` routes them to the wrong node and eats a guaranteed MOVED
//! round-trip (often on the hottest commands: Lua scripts, stream consumers,
//! multi-key set ops). [`extract_key`] uses a per-command table, modelled on
//! the Redis command key specs, for the families where the first key is not
//! `argv[1]`:
//!
//! | Family | Commands | First key |
//! |---|---|---|
//! | script / function | `EVAL[_RO]`, `EVALSHA[_RO]`, `FCALL[_RO]` | after `numkeys` at `argv[2]` |
//! | numkeys-first | `LMPOP`, `ZMPOP`, `SINTERCARD`, `ZUNION`, `ZINTER`, `ZDIFF` | after `numkeys` at `argv[1]` |
//! | blocking numkeys | `BLMPOP`, `BZMPOP` | after `numkeys` at `argv[2]` |
//! | streams | `XREAD`, `XREADGROUP` | first token after `STREAMS` |
//! | subcommand + key | `OBJECT <sub> key`, `MEMORY USAGE key` | `argv[2]` |
//! | op + dest | `BITOP <op> dest src...` | `argv[2]` |
//!
//! When `numkeys` is `0` (a keyless script, e.g. `EVAL "..." 0`) there is no
//! key, so routing falls back to the default node.

use redis_tower_core::Frame;

/// Extract the first key from a command frame.
///
/// Returns `None` for keyless commands (`PING`, `FLUSHDB`, ...), for
/// numkeys-style commands invoked with zero keys, and for malformed frames --
/// in every case the caller routes to the default node. See the
/// [module docs](self) for the per-command key table.
pub fn extract_key(frame: &Frame) -> Option<&[u8]> {
    let items = match frame {
        Frame::Array(Some(items)) if !items.is_empty() => items.as_slice(),
        _ => return None,
    };

    // Get the command name.
    let cmd_name = match &items[0] {
        Frame::BulkString(Some(b)) => b.as_ref(),
        _ => return None,
    };

    let upper: Vec<u8> = cmd_name.iter().map(|b| b.to_ascii_uppercase()).collect();
    match upper.as_slice() {
        // Keyless commands route to the default node.
        //
        // MULTI/EXEC/DISCARD are keyless and route to the default node, but the
        // commands queued between them route by their own keys -- so a
        // transaction driven with raw command builders scatters across nodes
        // and does NOT execute atomically on a cluster. Atomic cluster
        // transactions require all keys in one slot plus a slot-pinned executor
        // (not yet implemented). Drive a transaction against a single-node
        // client for the owning slot, not the cluster client.
        b"PING" | b"ECHO" | b"AUTH" | b"SELECT" | b"FLUSHDB" | b"FLUSHALL" | b"DBSIZE"
        | b"INFO" | b"CONFIG" | b"CLUSTER" | b"CLIENT" | b"COMMAND" | b"TIME" | b"MULTI"
        | b"EXEC" | b"DISCARD" => None,

        // Script / function: `CMD body numkeys key...` -- key follows numkeys
        // at argv[2]. argv[1] is the script text / SHA / function name.
        b"EVAL" | b"EVALSHA" | b"EVAL_RO" | b"EVALSHA_RO" | b"FCALL" | b"FCALL_RO" => {
            key_after_numkeys(items, 2)
        }

        // numkeys-first: `CMD numkeys key...` -- key follows numkeys at argv[1].
        b"LMPOP" | b"ZMPOP" | b"SINTERCARD" | b"ZDIFF" | b"ZINTER" | b"ZUNION" => {
            key_after_numkeys(items, 1)
        }

        // Blocking numkeys: `CMD timeout numkeys key...` -- numkeys at argv[2].
        b"BLMPOP" | b"BZMPOP" => key_after_numkeys(items, 2),

        // Streams: keys follow the STREAMS token, after optional COUNT/BLOCK.
        b"XREAD" | b"XREADGROUP" => key_after_token(items, b"STREAMS"),

        // Subcommand then key: `CMD SUB key` -- key at argv[2]. OBJECT HELP and
        // MEMORY DOCTOR/STATS/... have no key.
        b"OBJECT" => as_key(items.get(2)?),
        b"MEMORY" => {
            if matches_token(items.get(1), b"USAGE") {
                as_key(items.get(2)?)
            } else {
                None
            }
        }

        // BITOP op dest src...: argv[1] is the operation (AND/OR/XOR/NOT), so
        // the first key is the destination at argv[2].
        b"BITOP" => as_key(items.get(2)?),

        // Default: the key is at argv[1].
        _ => as_key(items.get(1)?),
    }
}

/// Interpret a frame as a binary-safe key (a bulk string).
fn as_key(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(Some(b)) => Some(b.as_ref()),
        _ => None,
    }
}

/// Parse a frame as an integer argument (`numkeys`, encoded as a bulk string
/// in a request, though an `Integer` frame is accepted defensively).
fn parse_int(frame: &Frame) -> Option<i64> {
    match frame {
        Frame::BulkString(Some(b)) => std::str::from_utf8(b).ok()?.trim().parse().ok(),
        Frame::Integer(n) => Some(*n),
        _ => None,
    }
}

/// True when `frame` is a bulk string equal to `token`, case-insensitively.
fn matches_token(frame: Option<&Frame>, token: &[u8]) -> bool {
    matches!(frame, Some(Frame::BulkString(Some(b))) if b.eq_ignore_ascii_case(token))
}

/// First key for a `... numkeys key [key ...]` command, where `numkeys` is at
/// `numkeys_idx`. Returns `None` when `numkeys` is `< 1`, unparseable, or the
/// key slot is absent.
fn key_after_numkeys(items: &[Frame], numkeys_idx: usize) -> Option<&[u8]> {
    if parse_int(items.get(numkeys_idx)?)? < 1 {
        return None;
    }
    as_key(items.get(numkeys_idx + 1)?)
}

/// First key appearing immediately after `token` (e.g. `STREAMS`) in the
/// argument list.
fn key_after_token<'a>(items: &'a [Frame], token: &[u8]) -> Option<&'a [u8]> {
    let pos = items.iter().position(|f| matches_token(Some(f), token))?;
    as_key(items.get(pos + 1)?)
}

/// Returns true if the command is read-only, and so safe to route to a replica
/// under [`ReadPreference::Replica`](crate::ReadPreference).
///
/// Routing happens on the serialized frame -- the auto-pipeline batches frames,
/// not typed commands -- so this matches the command name rather than a
/// `Command` trait flag. The name is uppercased into a stack buffer to avoid a
/// heap allocation on every replica-routed command.
///
/// Coverage follows the Redis command `readonly` flag across the core types and
/// the common Redis Stack reads. Commands that can mutate -- even conditionally,
/// like `GETEX` (may change a TTL), `GEORADIUS`/`SORT` (have a `STORE` option),
/// or `XREADGROUP` (advances a consumer group) -- are treated as writes and
/// routed to the master; their dedicated `_RO` variants are read-only.
pub fn is_readonly_command(frame: &Frame) -> bool {
    let items = match frame {
        Frame::Array(Some(items)) if !items.is_empty() => items,
        _ => return false,
    };

    let cmd_name = match &items[0] {
        Frame::BulkString(Some(b)) => b.as_ref(),
        _ => return false,
    };

    // Uppercase into a stack buffer. No read-only command name is longer than
    // this, so anything that overflows it cannot be read-only.
    let mut buf = [0u8; 24];
    if cmd_name.len() > buf.len() {
        return false;
    }
    for (i, b) in cmd_name.iter().enumerate() {
        buf[i] = b.to_ascii_uppercase();
    }

    matches!(
        &buf[..cmd_name.len()],
        // strings / bitmaps
        b"GET" | b"GETRANGE" | b"SUBSTR" | b"MGET" | b"STRLEN" | b"LCS"
        | b"GETBIT" | b"BITCOUNT" | b"BITPOS" | b"BITFIELD_RO"
        // generic keyspace
        | b"EXISTS" | b"TYPE" | b"TTL" | b"PTTL" | b"EXPIRETIME" | b"PEXPIRETIME"
        | b"DUMP" | b"OBJECT" | b"MEMORY" | b"SORT_RO"
        // hashes
        | b"HGET" | b"HGETALL" | b"HKEYS" | b"HVALS" | b"HLEN" | b"HEXISTS"
        | b"HMGET" | b"HSTRLEN" | b"HRANDFIELD" | b"HSCAN"
        // lists
        | b"LRANGE" | b"LLEN" | b"LINDEX" | b"LPOS"
        // sets
        | b"SMEMBERS" | b"SISMEMBER" | b"SMISMEMBER" | b"SCARD" | b"SINTER"
        | b"SINTERCARD" | b"SUNION" | b"SDIFF" | b"SRANDMEMBER" | b"SSCAN"
        // sorted sets
        | b"ZRANGE" | b"ZRANGEBYSCORE" | b"ZRANGEBYLEX" | b"ZREVRANGE"
        | b"ZREVRANGEBYSCORE" | b"ZREVRANGEBYLEX" | b"ZSCORE" | b"ZMSCORE"
        | b"ZCARD" | b"ZRANK" | b"ZREVRANK" | b"ZCOUNT" | b"ZLEXCOUNT"
        | b"ZRANDMEMBER" | b"ZSCAN" | b"ZDIFF" | b"ZINTER" | b"ZUNION"
        | b"ZINTERCARD"
        // streams (XREADGROUP mutates a consumer group -- excluded)
        | b"XLEN" | b"XRANGE" | b"XREVRANGE" | b"XREAD" | b"XINFO" | b"XPENDING"
        // geo (read-only; STORE-capable GEORADIUS routes to master)
        | b"GEOPOS" | b"GEODIST" | b"GEOHASH" | b"GEOSEARCH"
        | b"GEORADIUS_RO" | b"GEORADIUSBYMEMBER_RO"
        // hyperloglog (PFADD/PFMERGE mutate -- excluded)
        | b"PFCOUNT"
        // scripting (read-only variants only)
        | b"EVAL_RO" | b"EVALSHA_RO" | b"FCALL_RO"
        // server
        | b"DBSIZE" | b"PING" | b"ECHO" | b"INFO"
        // Redis Stack: JSON
        | b"JSON.GET" | b"JSON.MGET" | b"JSON.TYPE" | b"JSON.STRLEN"
        | b"JSON.ARRLEN" | b"JSON.ARRINDEX" | b"JSON.OBJLEN" | b"JSON.OBJKEYS"
        | b"JSON.RESP"
        // Redis Stack: Search
        | b"FT.SEARCH" | b"FT.AGGREGATE" | b"FT.INFO" | b"FT.EXPLAIN"
        // Redis Stack: TimeSeries
        | b"TS.GET" | b"TS.MGET" | b"TS.RANGE" | b"TS.REVRANGE" | b"TS.MRANGE"
        | b"TS.MREVRANGE" | b"TS.INFO" | b"TS.QUERYINDEX"
        // Redis Stack: probabilistic
        | b"BF.EXISTS" | b"BF.MEXISTS" | b"BF.INFO" | b"BF.CARD"
        | b"CF.EXISTS" | b"CF.COUNT" | b"CF.INFO"
        | b"CMS.QUERY" | b"CMS.INFO"
        | b"TOPK.QUERY" | b"TOPK.COUNT" | b"TOPK.LIST" | b"TOPK.INFO"
        | b"TDIGEST.MIN" | b"TDIGEST.MAX" | b"TDIGEST.QUANTILE"
        | b"TDIGEST.CDF" | b"TDIGEST.RANK" | b"TDIGEST.INFO"
        // Redis Stack: vector sets
        | b"VSIM" | b"VCARD" | b"VDIM" | b"VEMB" | b"VGETATTR" | b"VLINKS"
        | b"VINFO"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_protocol::helpers::{array, bulk};

    #[test]
    fn extract_key_from_get() {
        let frame = array(vec![bulk("GET"), bulk("mykey")]);
        assert_eq!(extract_key(&frame), Some(b"mykey".as_slice()));
    }

    #[test]
    fn extract_key_from_set() {
        let frame = array(vec![bulk("SET"), bulk("mykey"), bulk("value")]);
        assert_eq!(extract_key(&frame), Some(b"mykey".as_slice()));
    }

    #[test]
    fn no_key_for_ping() {
        let frame = array(vec![bulk("PING")]);
        assert_eq!(extract_key(&frame), None);
    }

    #[test]
    fn no_key_for_flushdb() {
        let frame = array(vec![bulk("FLUSHDB")]);
        assert_eq!(extract_key(&frame), None);
    }

    #[test]
    fn extract_key_case_insensitive() {
        let frame = array(vec![bulk("ping")]);
        assert_eq!(extract_key(&frame), None);
    }

    #[test]
    fn readonly_commands() {
        assert!(is_readonly_command(&array(vec![bulk("GET"), bulk("k")])));
        assert!(is_readonly_command(&array(vec![
            bulk("HGETALL"),
            bulk("k")
        ])));
        assert!(is_readonly_command(&array(vec![
            bulk("LRANGE"),
            bulk("k"),
            bulk("0"),
            bulk("-1")
        ])));
        assert!(is_readonly_command(&array(vec![
            bulk("ZRANGE"),
            bulk("k"),
            bulk("0"),
            bulk("-1")
        ])));
    }

    #[test]
    fn write_commands_not_readonly() {
        assert!(!is_readonly_command(&array(vec![
            bulk("SET"),
            bulk("k"),
            bulk("v")
        ])));
        assert!(!is_readonly_command(&array(vec![bulk("DEL"), bulk("k")])));
        assert!(!is_readonly_command(&array(vec![bulk("INCR"), bulk("k")])));
        assert!(!is_readonly_command(&array(vec![
            bulk("LPUSH"),
            bulk("k"),
            bulk("v")
        ])));
    }

    #[test]
    fn readonly_case_insensitive() {
        assert!(is_readonly_command(&array(vec![bulk("get"), bulk("k")])));
        assert!(is_readonly_command(&array(vec![bulk("Get"), bulk("k")])));
    }

    #[test]
    fn empty_frame_not_readonly() {
        assert!(!is_readonly_command(&Frame::Array(Some(vec![]))));
        assert!(!is_readonly_command(&Frame::Null));
    }

    #[test]
    fn expanded_readonly_coverage_engages_replicas() {
        // The reads that were previously missing -- replicas sat idle for these.
        for cmd in [
            "GETBIT",
            "BITCOUNT",
            "BITPOS",
            "SMISMEMBER",
            "SINTERCARD",
            "ZMSCORE",
            "ZRANDMEMBER",
            "ZDIFF",
            "ZUNION",
            "XLEN",
            "XRANGE",
            "XREAD",
            "XINFO",
            "GEOPOS",
            "GEODIST",
            "GEOSEARCH",
            "PFCOUNT",
            "OBJECT",
            "HRANDFIELD",
            "LPOS",
            "DUMP",
            "EXPIRETIME",
        ] {
            assert!(
                is_readonly_command(&array(vec![bulk(cmd), bulk("k")])),
                "{cmd} should be read-only"
            );
        }
    }

    #[test]
    fn readonly_ro_variants_engage_replicas_but_base_does_not() {
        // The base commands can mutate (STORE / TTL / consumer-group), so they
        // route to the master; the dedicated _RO variants are read-only.
        for ro in [
            "EVAL_RO",
            "EVALSHA_RO",
            "FCALL_RO",
            "GEORADIUS_RO",
            "GEORADIUSBYMEMBER_RO",
            "BITFIELD_RO",
            "SORT_RO",
        ] {
            assert!(
                is_readonly_command(&array(vec![bulk(ro), bulk("k")])),
                "{ro} should be read-only"
            );
        }
        for write in [
            "GEORADIUS",
            "BITFIELD",
            "SORT",
            "XREADGROUP",
            "GETEX",
            "PFADD",
        ] {
            assert!(
                !is_readonly_command(&array(vec![bulk(write), bulk("k")])),
                "{write} can mutate and must route to the master"
            );
        }
    }

    #[test]
    fn readonly_covers_redis_stack_reads() {
        for cmd in [
            "JSON.GET",
            "JSON.TYPE",
            "FT.SEARCH",
            "FT.AGGREGATE",
            "TS.RANGE",
            "TS.GET",
            "BF.EXISTS",
            "CF.COUNT",
            "CMS.QUERY",
            "TOPK.LIST",
            "VSIM",
        ] {
            assert!(
                is_readonly_command(&array(vec![bulk(cmd), bulk("k")])),
                "{cmd} should be read-only"
            );
        }
        // Stack writes still route to the master.
        for cmd in ["JSON.SET", "TS.ADD", "BF.ADD", "FT.CREATE"] {
            assert!(
                !is_readonly_command(&array(vec![bulk(cmd), bulk("k")])),
                "{cmd} should route to the master"
            );
        }
    }

    #[test]
    fn overlong_command_name_is_not_readonly() {
        // Longer than the stack buffer -- must return false, not panic.
        let long = "X".repeat(64);
        assert!(!is_readonly_command(&array(vec![bulk(long), bulk("k")])));
        // The longest real read-only name still fits and matches.
        assert!(is_readonly_command(&array(vec![
            bulk("GEORADIUSBYMEMBER_RO"),
            bulk("k")
        ])));
    }

    #[test]
    fn extract_key_from_hset() {
        let frame = array(vec![bulk("HSET"), bulk("hash"), bulk("field"), bulk("val")]);
        assert_eq!(extract_key(&frame), Some(b"hash".as_slice()));
    }

    #[test]
    fn extract_key_from_lpush() {
        let frame = array(vec![bulk("LPUSH"), bulk("list"), bulk("item")]);
        assert_eq!(extract_key(&frame), Some(b"list".as_slice()));
    }

    #[test]
    fn extract_key_from_zadd() {
        let frame = array(vec![
            bulk("ZADD"),
            bulk("zset"),
            bulk("1.0"),
            bulk("member"),
        ]);
        assert_eq!(extract_key(&frame), Some(b"zset".as_slice()));
    }

    #[test]
    fn no_key_for_cluster_commands() {
        assert_eq!(
            extract_key(&array(vec![bulk("CLUSTER"), bulk("SLOTS")])),
            None
        );
        assert_eq!(
            extract_key(&array(vec![bulk("CLUSTER"), bulk("INFO")])),
            None
        );
    }

    #[test]
    fn no_key_for_multi_exec() {
        assert_eq!(extract_key(&array(vec![bulk("MULTI")])), None);
        assert_eq!(extract_key(&array(vec![bulk("EXEC")])), None);
        assert_eq!(extract_key(&array(vec![bulk("DISCARD")])), None);
    }

    #[test]
    fn no_key_for_auth_select() {
        assert_eq!(
            extract_key(&array(vec![bulk("AUTH"), bulk("password")])),
            None
        );
        assert_eq!(extract_key(&array(vec![bulk("SELECT"), bulk("0")])), None);
    }

    #[test]
    fn null_frame_returns_none() {
        assert_eq!(extract_key(&Frame::Null), None);
    }

    #[test]
    fn single_element_array_no_key() {
        // Only command name, no key argument.
        assert_eq!(extract_key(&array(vec![bulk("RANDOMKEY")])), None);
    }

    // --- script / function: key follows numkeys at argv[2] ---

    #[test]
    fn eval_hashes_the_key_not_the_script() {
        // EVAL script numkeys key [key ...] arg [arg ...]
        let frame = array(vec![
            bulk("EVAL"),
            bulk("return redis.call('GET', KEYS[1])"),
            bulk("1"),
            bulk("mykey"),
            bulk("extra-arg"),
        ]);
        // The old heuristic hashed argv[1] (the script text); the key is argv[3].
        assert_eq!(extract_key(&frame), Some(b"mykey".as_slice()));
    }

    #[test]
    fn evalsha_first_of_multiple_keys() {
        let frame = array(vec![
            bulk("EVALSHA"),
            bulk("abc123"),
            bulk("2"),
            bulk("k1"),
            bulk("k2"),
        ]);
        assert_eq!(extract_key(&frame), Some(b"k1".as_slice()));
    }

    #[test]
    fn eval_with_zero_keys_has_no_key() {
        // A keyless script routes to the default node.
        let frame = array(vec![bulk("EVAL"), bulk("return 1"), bulk("0")]);
        assert_eq!(extract_key(&frame), None);
    }

    #[test]
    fn fcall_and_ro_variants() {
        assert_eq!(
            extract_key(&array(vec![
                bulk("FCALL"),
                bulk("myfunc"),
                bulk("1"),
                bulk("fkey"),
            ])),
            Some(b"fkey".as_slice())
        );
        assert_eq!(
            extract_key(&array(vec![
                bulk("EVAL_RO"),
                bulk("return 1"),
                bulk("1"),
                bulk("rokey"),
            ])),
            Some(b"rokey".as_slice())
        );
        assert_eq!(
            extract_key(&array(vec![
                bulk("FCALL_RO"),
                bulk("f"),
                bulk("1"),
                bulk("frokey"),
            ])),
            Some(b"frokey".as_slice())
        );
    }

    // --- numkeys-first: key follows numkeys at argv[1] ---

    #[test]
    fn lmpop_zmpop_key_after_numkeys() {
        assert_eq!(
            extract_key(&array(vec![
                bulk("LMPOP"),
                bulk("2"),
                bulk("list1"),
                bulk("list2"),
                bulk("LEFT"),
            ])),
            Some(b"list1".as_slice())
        );
        assert_eq!(
            extract_key(&array(vec![
                bulk("ZMPOP"),
                bulk("1"),
                bulk("zset"),
                bulk("MIN"),
            ])),
            Some(b"zset".as_slice())
        );
    }

    #[test]
    fn sintercard_and_zsetops_key_after_numkeys() {
        for cmd in ["SINTERCARD", "ZUNION", "ZINTER", "ZDIFF"] {
            let frame = array(vec![bulk(cmd), bulk("2"), bulk("a"), bulk("b")]);
            assert_eq!(
                extract_key(&frame),
                Some(b"a".as_slice()),
                "{cmd} should route by its first key"
            );
        }
    }

    #[test]
    fn blocking_numkeys_key_at_argv3() {
        // BLMPOP timeout numkeys key [key ...] <LEFT|RIGHT>
        assert_eq!(
            extract_key(&array(vec![
                bulk("BLMPOP"),
                bulk("0"),
                bulk("2"),
                bulk("l1"),
                bulk("l2"),
                bulk("LEFT"),
            ])),
            Some(b"l1".as_slice())
        );
        assert_eq!(
            extract_key(&array(vec![
                bulk("BZMPOP"),
                bulk("1.5"),
                bulk("1"),
                bulk("z"),
                bulk("MAX"),
            ])),
            Some(b"z".as_slice())
        );
    }

    #[test]
    fn numkeys_unparseable_or_zero_is_none() {
        // numkeys "0" => no keys.
        assert_eq!(
            extract_key(&array(vec![bulk("SINTERCARD"), bulk("0")])),
            None
        );
        // garbage numkeys => can't determine the key, route to default.
        assert_eq!(
            extract_key(&array(vec![bulk("LMPOP"), bulk("notanint"), bulk("k")])),
            None
        );
    }

    // --- streams: key follows the STREAMS token ---

    #[test]
    fn xread_key_after_streams_token() {
        // XREAD COUNT 2 STREAMS s1 s2 0 0
        let frame = array(vec![
            bulk("XREAD"),
            bulk("COUNT"),
            bulk("2"),
            bulk("STREAMS"),
            bulk("s1"),
            bulk("s2"),
            bulk("0"),
            bulk("0"),
        ]);
        assert_eq!(extract_key(&frame), Some(b"s1".as_slice()));
    }

    #[test]
    fn xread_with_block_and_lowercase_streams() {
        let frame = array(vec![
            bulk("XREAD"),
            bulk("BLOCK"),
            bulk("100"),
            bulk("streams"),
            bulk("mystream"),
            bulk("$"),
        ]);
        assert_eq!(extract_key(&frame), Some(b"mystream".as_slice()));
    }

    #[test]
    fn xreadgroup_key_after_streams() {
        let frame = array(vec![
            bulk("XREADGROUP"),
            bulk("GROUP"),
            bulk("g"),
            bulk("c"),
            bulk("COUNT"),
            bulk("1"),
            bulk("STREAMS"),
            bulk("stream"),
            bulk(">"),
        ]);
        assert_eq!(extract_key(&frame), Some(b"stream".as_slice()));
    }

    // --- subcommand + key, and op + dest ---

    #[test]
    fn object_routes_by_key_not_subcommand() {
        assert_eq!(
            extract_key(&array(vec![
                bulk("OBJECT"),
                bulk("ENCODING"),
                bulk("mykey"),
            ])),
            Some(b"mykey".as_slice())
        );
        // OBJECT HELP has no key.
        assert_eq!(
            extract_key(&array(vec![bulk("OBJECT"), bulk("HELP")])),
            None
        );
    }

    #[test]
    fn memory_usage_has_key_other_subcommands_do_not() {
        assert_eq!(
            extract_key(&array(vec![bulk("MEMORY"), bulk("USAGE"), bulk("mykey"),])),
            Some(b"mykey".as_slice())
        );
        assert_eq!(
            extract_key(&array(vec![bulk("MEMORY"), bulk("DOCTOR")])),
            None
        );
        assert_eq!(
            extract_key(&array(vec![bulk("MEMORY"), bulk("STATS")])),
            None
        );
    }

    #[test]
    fn bitop_routes_by_destination_not_operation() {
        // BITOP AND dest src1 src2 -- argv[1] is the operation, argv[2] the dest.
        let frame = array(vec![
            bulk("BITOP"),
            bulk("AND"),
            bulk("dest"),
            bulk("src1"),
            bulk("src2"),
        ]);
        assert_eq!(extract_key(&frame), Some(b"dest".as_slice()));
    }

    #[test]
    fn integer_numkeys_frame_is_accepted() {
        // Requests encode numkeys as a bulk string, but accept an Integer too.
        let frame = Frame::Array(Some(vec![
            bulk("LMPOP"),
            Frame::Integer(1),
            bulk("list"),
            bulk("LEFT"),
        ]));
        assert_eq!(extract_key(&frame), Some(b"list".as_slice()));
    }
}
