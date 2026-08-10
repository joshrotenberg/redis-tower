//! Shared read-preference and replica-routing primitives.
//!
//! [`ReadPreference`] and [`ReadRoutingStrategy`] are used by both
//! `redis-tower-cluster` (routing per hash slot) and `redis-tower-sentinel`
//! (routing across a monitored master's replica set). They live here, in a
//! crate both siblings already depend on, so neither pulls in the other --
//! `redis-tower-cluster` re-exports these types under their original paths
//! for source compatibility.
//!
//! [`NodeAddr`] is likewise shared: cluster topology and sentinel replica
//! discovery both resolve to a `host:port` pair, and [`ReadRoutingStrategy`]
//! selects among a slice of them regardless of which crate is asking.
//!
//! [`is_readonly_command`] classifies a serialized command frame as
//! read-only or not, the other input routing needs beyond an address: given
//! a [`ReadPreference`] other than [`ReadPreference::Master`], only commands
//! this function accepts are safe to send to a replica.
//!
//! Read preferences have the same availability semantics in every client that
//! supports replica routing. [`ReadPreference::Replica`] is strict: an eligible
//! read fails when no usable replica is available, and is never silently sent
//! to the master. [`ReadPreference::PreferReplica`] makes the same selection
//! attempt but falls back to the master. Writes are unaffected and always use
//! the master.

use redis_tower_core::Frame;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Address of a Redis node, identified by host and port.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeAddr {
    /// Hostname or IP address.
    pub host: String,
    /// TCP port.
    pub port: u16,
}

impl NodeAddr {
    /// Format as `"host:port"`.
    pub fn addr_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Parse a `"host:port"` string into a [`NodeAddr`].
    ///
    /// Splits on the last `:` and preserves the host portion verbatim, so both
    /// hostnames and bracketed IPv6 addresses round-trip. Returns `None` if the
    /// host is empty, there is no `:`, or the port is not a valid `u16`.
    pub fn parse(addr: &str) -> Option<Self> {
        let (host, port) = addr.rsplit_once(':')?;
        if host.is_empty() {
            return None;
        }
        let port = port.parse().ok()?;
        Some(Self {
            host: host.to_string(),
            port,
        })
    }
}

impl std::fmt::Display for NodeAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// Read routing preference for read-only commands.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReadPreference {
    /// Always read from the master (default).
    #[default]
    Master,
    /// Route read-only commands strictly to a replica.
    ///
    /// If no usable replica is available, return an error rather than falling
    /// back to the master.
    Replica,
    /// Prefer a replica, but fall back to the master when none is usable.
    PreferReplica,
}

/// Strategy for selecting which replica to read from.
///
/// Implement this trait to provide custom replica selection logic.
/// Built-in implementations include [`RoundRobinRouting`], [`RandomRouting`],
/// and [`FirstReplicaRouting`].
///
/// `slot` is a Redis Cluster hash slot when the caller is cluster-aware, or
/// `0` when the caller has no slot concept (Sentinel monitors one shard, not
/// 16384 of them). Implementations that only care about the replica list --
/// every built-in strategy -- can ignore it.
pub trait ReadRoutingStrategy: Send + Sync + 'static {
    /// Select a replica address for the given slot.
    ///
    /// `replicas` is the list of available replica addresses for the slot.
    /// Return the selected address, or `None` when no replica should be used.
    /// The caller's [`ReadPreference`] determines whether that becomes an
    /// error or a fallback to the master.
    fn select_replica<'a>(&self, slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr>;
}

/// Round-robin across replicas (default).
///
/// Distributes reads evenly across all available replicas for a slot
/// by cycling through them in order.
pub struct RoundRobinRouting {
    counter: AtomicUsize,
}

impl RoundRobinRouting {
    /// Create a new round-robin routing strategy.
    pub fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobinRouting {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadRoutingStrategy for RoundRobinRouting {
    fn select_replica<'a>(&self, _slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr> {
        if replicas.is_empty() {
            return None;
        }
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) % replicas.len();
        Some(&replicas[idx])
    }
}

/// Pseudo-random replica selection.
///
/// Uses an atomic counter with a time-based seed to approximate random
/// distribution without requiring an external RNG dependency.
pub struct RandomRouting {
    counter: AtomicUsize,
}

impl RandomRouting {
    /// Create a new random routing strategy.
    pub fn new() -> Self {
        // Seed from the current time for a pseudo-random starting point.
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as usize)
            .unwrap_or(0);
        Self {
            counter: AtomicUsize::new(seed),
        }
    }
}

impl Default for RandomRouting {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadRoutingStrategy for RandomRouting {
    fn select_replica<'a>(&self, _slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr> {
        if replicas.is_empty() {
            return None;
        }
        // Mix the counter value to spread selections across replicas.
        let val = self.counter.fetch_add(7919, Ordering::Relaxed);
        let idx = val % replicas.len();
        Some(&replicas[idx])
    }
}

/// Always pick the first replica.
///
/// Useful for testing or when replicas are ordered by preference
/// (e.g., closest datacenter first).
pub struct FirstReplicaRouting;

impl ReadRoutingStrategy for FirstReplicaRouting {
    fn select_replica<'a>(&self, _slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr> {
        replicas.first()
    }
}

/// Returns true if the command is read-only, and so safe to route to a
/// replica under [`ReadPreference::Replica`] or [`ReadPreference::PreferReplica`].
///
/// Routing happens on the serialized frame -- callers batch frames, not
/// typed commands, ahead of this check -- so this matches the command name
/// rather than a `Command` trait flag. The name is uppercased into a stack
/// buffer to avoid a heap allocation on every replica-routed command.
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
        b"GET" | b"GETRANGE" | b"SUBSTR" | b"MGET" | b"STRLEN" | b"LCS" | b"DIGEST"
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
        // arrays (Redis 8.8; mutation commands are excluded)
        | b"ARCOUNT" | b"ARGET" | b"ARGETRANGE" | b"ARGREP" | b"ARINFO"
        | b"ARLASTITEMS" | b"ARLEN" | b"ARMGET" | b"ARNEXT" | b"AROP" | b"ARSCAN"
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
        | b"FT.SEARCH" | b"FT.AGGREGATE" | b"FT.INFO" | b"FT.PROFILE"
        | b"FT.EXPLAIN" | b"FT.EXPLAINCLI" | b"FT.HYBRID" | b"FT.TAGVALS"
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
        | b"VINFO" | b"VISMEMBER" | b"VRANGE"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_addr_display() {
        let addr = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 7000,
        };
        assert_eq!(addr.to_string(), "127.0.0.1:7000");
        assert_eq!(addr.addr_string(), "127.0.0.1:7000");
    }

    #[test]
    fn node_addr_equality() {
        let a = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 7000,
        };
        let b = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 7000,
        };
        let c = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 7001,
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn node_addr_parse_roundtrips() {
        let addr = NodeAddr::parse("127.0.0.1:6380").unwrap();
        assert_eq!(addr.host, "127.0.0.1");
        assert_eq!(addr.port, 6380);

        let ipv6 = NodeAddr::parse("[::1]:6380").unwrap();
        assert_eq!(ipv6.addr_string(), "[::1]:6380");
    }

    #[test]
    fn node_addr_parse_rejects_missing_port() {
        assert!(NodeAddr::parse("127.0.0.1").is_none());
        assert!(NodeAddr::parse(":6379").is_none());
    }

    #[test]
    fn node_addr_parse_rejects_non_numeric_port() {
        assert!(NodeAddr::parse("127.0.0.1:notaport").is_none());
    }

    #[test]
    fn read_preference_default_is_master() {
        assert_eq!(ReadPreference::default(), ReadPreference::Master);
    }

    #[test]
    fn read_preference_variants() {
        assert_ne!(ReadPreference::Master, ReadPreference::Replica);
        assert_ne!(ReadPreference::Replica, ReadPreference::PreferReplica);
        assert_ne!(ReadPreference::Master, ReadPreference::PreferReplica);
    }

    // -- ReadRoutingStrategy tests --

    fn make_replicas() -> Vec<NodeAddr> {
        vec![
            NodeAddr {
                host: "10.0.0.1".to_string(),
                port: 7001,
            },
            NodeAddr {
                host: "10.0.0.2".to_string(),
                port: 7002,
            },
            NodeAddr {
                host: "10.0.0.3".to_string(),
                port: 7003,
            },
        ]
    }

    #[test]
    fn round_robin_distributes_across_replicas() {
        let strategy = RoundRobinRouting::new();
        let replicas = make_replicas();

        let first = strategy.select_replica(0, &replicas).unwrap();
        let second = strategy.select_replica(0, &replicas).unwrap();
        let third = strategy.select_replica(0, &replicas).unwrap();
        let fourth = strategy.select_replica(0, &replicas).unwrap();

        assert_eq!(first.port, 7001);
        assert_eq!(second.port, 7002);
        assert_eq!(third.port, 7003);
        // Wraps around.
        assert_eq!(fourth.port, 7001);
    }

    #[test]
    fn round_robin_returns_none_for_empty_replicas() {
        let strategy = RoundRobinRouting::new();
        assert!(strategy.select_replica(0, &[]).is_none());
    }

    #[test]
    fn random_routing_returns_valid_replica() {
        let strategy = RandomRouting::new();
        let replicas = make_replicas();

        // Call many times and verify all results are valid replicas.
        for _ in 0..100 {
            let selected = strategy.select_replica(0, &replicas).unwrap();
            assert!(
                replicas.contains(selected),
                "selected replica not in list: {selected:?}"
            );
        }
    }

    #[test]
    fn random_routing_returns_none_for_empty_replicas() {
        let strategy = RandomRouting::new();
        assert!(strategy.select_replica(0, &[]).is_none());
    }

    #[test]
    fn first_replica_always_returns_first() {
        let strategy = FirstReplicaRouting;
        let replicas = make_replicas();

        for _ in 0..10 {
            let selected = strategy.select_replica(0, &replicas).unwrap();
            assert_eq!(selected.port, 7001);
            assert_eq!(selected.host, "10.0.0.1");
        }
    }

    #[test]
    fn first_replica_returns_none_for_empty_replicas() {
        let strategy = FirstReplicaRouting;
        assert!(strategy.select_replica(0, &[]).is_none());
    }

    #[test]
    fn is_readonly_command_accepts_reads_and_rejects_writes() {
        use redis_tower_protocol::helpers::{array, bulk};

        assert!(is_readonly_command(&array(vec![bulk("GET"), bulk("key")])));
        assert!(is_readonly_command(&array(vec![
            bulk("HGETALL"),
            bulk("h")
        ])));
        assert!(!is_readonly_command(&array(vec![
            bulk("SET"),
            bulk("key"),
            bulk("val")
        ])));
        assert!(!is_readonly_command(&array(vec![bulk("GETEX"), bulk("k")])));
        assert!(!is_readonly_command(&Frame::Array(None)));
    }

    #[test]
    fn is_readonly_command_is_case_insensitive() {
        use redis_tower_protocol::helpers::{array, bulk};
        assert!(is_readonly_command(&array(vec![bulk("get"), bulk("k")])));
    }
}
