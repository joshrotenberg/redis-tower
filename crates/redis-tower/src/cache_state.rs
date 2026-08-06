//! Shared state and helpers for client-side caching.
//!
//! Both [`CachedClient`](crate::caching::CachedClient) and the Tower
//! [`CacheService`](crate::cache_layer::CacheService) use this so their
//! cache-key derivation and invalidation can never diverge.
//!
//! Cache keys are the **full** command argument vector, length-prefixed and
//! binary-safe -- so `HGET h f1` and `HGET h f2` are distinct entries rather
//! than colliding on `HGET:h`. A reverse index maps each Redis key to the cache
//! entries that depend on it, so a single invalidation evicts every variant in
//! O(1) lookups instead of an O(n) suffix scan that over-evicts.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use redis_tower_core::Frame;

use crate::metrics_layer::{CacheEvent, MetricsRecorder};

/// Default maximum number of cached entries (memory bound).
pub(crate) const DEFAULT_MAX_ENTRIES: usize = 10_000;
/// Default per-entry client-side freshness deadline. A cached entry older than
/// this is treated as a miss even if no invalidation arrived -- a safety
/// backstop against missed invalidations (Redis CSC guidance).
pub(crate) const DEFAULT_TTL: Duration = Duration::from_secs(30);
/// Maximum number of per-key invalidation epochs retained when the cache itself
/// is configured as unbounded. Epoch pruning advances the global generation,
/// so dropping bookkeeping can never make an old in-flight read current again.
const DEFAULT_MAX_EPOCHS: usize = DEFAULT_MAX_ENTRIES;

/// The cacheable read commands. The key these read is always argument 1.
///
/// `TTL` is intentionally excluded: its reply is a countdown that is wrong the
/// instant it is cached.
const CACHEABLE: &[&[u8]] = &[
    b"GET",
    b"HGET",
    b"HGETALL",
    b"LRANGE",
    b"SMEMBERS",
    b"ZRANGE",
    b"TYPE",
];

/// Append `arg` to `buf` with a 4-byte length prefix, so a concatenation of
/// arguments is unambiguous regardless of the bytes inside any argument.
fn push_arg(buf: &mut Vec<u8>, arg: &[u8]) {
    buf.extend_from_slice(&(arg.len() as u32).to_le_bytes());
    buf.extend_from_slice(arg);
}

/// For a cacheable read command frame, return `(cache_key, redis_key)`:
///
/// - `cache_key` identifies the exact command + arguments (binary-safe).
/// - `redis_key` is the Redis key the command reads, used for the reverse
///   index so invalidation of that key evicts this entry.
///
/// Returns `None` for non-cacheable commands or malformed frames.
#[doc(hidden)]
pub fn extract_cache_entry(frame: &Frame) -> Option<(Vec<u8>, Vec<u8>)> {
    let items = match frame {
        Frame::Array(Some(items)) if items.len() >= 2 => items,
        _ => return None,
    };

    let cmd_name = match &items[0] {
        Frame::BulkString(Some(b)) => b.as_ref(),
        _ => return None,
    };
    let upper: Vec<u8> = cmd_name.iter().map(|b| b.to_ascii_uppercase()).collect();
    if !CACHEABLE.contains(&upper.as_slice()) {
        return None;
    }

    let redis_key = match &items[1] {
        Frame::BulkString(Some(b)) => b.to_vec(),
        _ => return None,
    };

    // Cache key = uppercased command name + every argument, length-prefixed.
    let mut cache_key = Vec::new();
    push_arg(&mut cache_key, &upper);
    for item in &items[1..] {
        match item {
            Frame::BulkString(Some(b)) => push_arg(&mut cache_key, b.as_ref()),
            // A non-bulk argument can't be keyed safely; don't cache.
            _ => return None,
        }
    }

    Some((cache_key, redis_key))
}

/// Parse a server invalidation push message into the affected key bytes.
///
/// Returns `Some(vec![])` for a flush-everything invalidation (null payload),
/// `Some(keys)` for specific keys, or `None` if the frame is not an
/// `invalidate` message. Keys are raw bytes (binary-safe).
#[doc(hidden)]
pub fn parse_invalidation(frame: &Frame) -> Option<Vec<Vec<u8>>> {
    let items = match frame {
        Frame::Push(items) if !items.is_empty() => items,
        Frame::Array(Some(items)) if !items.is_empty() => items,
        _ => return None,
    };

    match &items[0] {
        Frame::BulkString(Some(b)) | Frame::SimpleString(b) if b.as_ref() == b"invalidate" => {}
        _ => return None,
    }

    match items.get(1) {
        Some(Frame::Array(Some(keys))) => {
            let mut result = Vec::new();
            for key in keys {
                let bytes = match key {
                    Frame::BulkString(Some(b)) | Frame::SimpleString(b) => b,
                    // The message is recognizably an invalidation but its key
                    // payload is malformed. Flush instead of risking a stale
                    // entry by applying only a partial key list.
                    _ => return Some(Vec::new()),
                };
                result.push(bytes.to_vec());
            }
            Some(result)
        }
        Some(Frame::Null | Frame::Array(None)) => Some(Vec::new()),
        // Once the frame is recognizably an invalidation, any missing or
        // malformed payload must flush rather than be ignored.
        _ => Some(Vec::new()),
    }
}

/// Commands known not to mutate Redis keyspace. Unknown commands are treated
/// as mutations by [`command_invalidation`] so new Redis/module commands remain
/// correct until explicitly classified.
const READ_ONLY_COMMANDS: &[&[u8]] = &[
    b"BITCOUNT",
    b"BITFIELD_RO",
    b"BITPOS",
    b"COMMAND",
    b"DBSIZE",
    b"DUMP",
    b"ECHO",
    b"EXISTS",
    b"EXPIRETIME",
    b"FCALL_RO",
    b"FT.AGGREGATE",
    b"FT.EXPLAIN",
    b"FT.EXPLAINCLI",
    b"FT.INFO",
    b"FT.PROFILE",
    b"FT.SEARCH",
    b"GEOHASH",
    b"GEOPOS",
    b"GEODIST",
    b"GEOSEARCH",
    b"GET",
    b"GETBIT",
    b"GETRANGE",
    b"HEXISTS",
    b"HGET",
    b"HGETALL",
    b"HKEYS",
    b"HLEN",
    b"HMGET",
    b"HRANDFIELD",
    b"HSCAN",
    b"HSTRLEN",
    b"HVALS",
    b"INFO",
    b"KEYS",
    b"LATENCY",
    b"LINDEX",
    b"LLEN",
    b"LPOS",
    b"LRANGE",
    b"MEMORY",
    b"MGET",
    b"OBJECT",
    b"PEXPIRETIME",
    b"PFCOUNT",
    b"PING",
    b"PTTL",
    b"PUBSUB",
    b"PUBLISH",
    b"RANDOMKEY",
    b"ROLE",
    b"SCAN",
    b"SCARD",
    b"SDIFF",
    b"SINTER",
    b"SINTERCARD",
    b"SISMEMBER",
    b"SMEMBERS",
    b"SMISMEMBER",
    b"SORT_RO",
    b"SRANDMEMBER",
    b"SSCAN",
    b"STRLEN",
    b"SUNION",
    b"TIME",
    b"TTL",
    b"TYPE",
    b"WAIT",
    b"WAITAOF",
    b"XINFO",
    b"XLEN",
    b"XPENDING",
    b"XRANGE",
    b"XREAD",
    b"XREVRANGE",
    b"ZCARD",
    b"ZCOUNT",
    b"ZDIFF",
    b"ZINTER",
    b"ZINTERCARD",
    b"ZLEXCOUNT",
    b"ZMSCORE",
    b"ZRANDMEMBER",
    b"ZRANGE",
    b"ZRANGEBYLEX",
    b"ZRANGEBYSCORE",
    b"ZRANK",
    b"ZREVRANGE",
    b"ZREVRANGEBYLEX",
    b"ZREVRANGEBYSCORE",
    b"ZREVRANK",
    b"ZSCAN",
    b"ZSCORE",
    b"ZUNION",
    b"JSON.ARRINDEX",
    b"JSON.ARRLEN",
    b"JSON.DEBUG",
    b"JSON.GET",
    b"JSON.MGET",
    b"JSON.OBJKEYS",
    b"JSON.OBJLEN",
    b"JSON.RESP",
    b"JSON.STRLEN",
    b"JSON.TYPE",
];

/// Mutations whose first argument is the only Redis key they change.
const FIRST_KEY_MUTATIONS: &[&[u8]] = &[
    b"APPEND",
    b"BITFIELD",
    b"DECR",
    b"DECRBY",
    b"EXPIRE",
    b"EXPIREAT",
    b"GETDEL",
    b"GETEX",
    b"GETSET",
    b"GEOADD",
    b"HDEL",
    b"HINCRBY",
    b"HINCRBYFLOAT",
    b"HMSET",
    b"HSET",
    b"HSETNX",
    b"INCR",
    b"INCRBY",
    b"INCRBYFLOAT",
    b"LINSERT",
    b"LPOP",
    b"LPUSH",
    b"LPUSHX",
    b"LREM",
    b"LSET",
    b"LTRIM",
    b"PERSIST",
    b"PEXPIRE",
    b"PEXPIREAT",
    b"PFADD",
    b"PSETEX",
    b"RESTORE",
    b"RPOP",
    b"RPUSH",
    b"RPUSHX",
    b"SADD",
    b"SREM",
    b"SET",
    b"SETBIT",
    b"SETEX",
    b"SETNX",
    b"SETRANGE",
    b"SPOP",
    b"XACK",
    b"XADD",
    b"XAUTOCLAIM",
    b"XCLAIM",
    b"XDEL",
    b"XSETID",
    b"XTRIM",
    b"ZADD",
    b"ZINCRBY",
    b"ZPOPMAX",
    b"ZPOPMIN",
    b"ZREM",
    b"ZREMRANGEBYLEX",
    b"ZREMRANGEBYRANK",
    b"ZREMRANGEBYSCORE",
    b"JSON.ARRAPPEND",
    b"JSON.ARRINSERT",
    b"JSON.ARRPOP",
    b"JSON.ARRTRIM",
    b"JSON.CLEAR",
    b"JSON.DEL",
    b"JSON.FORGET",
    b"JSON.MERGE",
    b"JSON.NUMINCRBY",
    b"JSON.NUMMULTBY",
    b"JSON.SET",
    b"JSON.STRAPPEND",
    b"JSON.TOGGLE",
];

enum CommandInvalidation {
    None,
    Keys(Vec<Vec<u8>>),
    Clear,
}

fn bulk_arg(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(Some(value)) => Some(value.as_ref()),
        _ => None,
    }
}

/// Return the connection-local command reserved by managed cached clients.
///
/// These commands can disable tracking, change RESP protocol/reply semantics,
/// or close/reset the data connection. Cached clients reject them before
/// dispatch so callers cannot silently invalidate the owned lifecycle.
#[doc(hidden)]
pub fn managed_cache_state_command(frame: &Frame) -> Option<&'static str> {
    let items = match frame {
        Frame::Array(Some(items)) if !items.is_empty() => items,
        _ => return None,
    };
    let command = bulk_arg(&items[0])?;

    if command.eq_ignore_ascii_case(b"RESET") {
        return Some("RESET");
    }
    if command.eq_ignore_ascii_case(b"HELLO") {
        return Some("HELLO");
    }
    if command.eq_ignore_ascii_case(b"QUIT") {
        return Some("QUIT");
    }
    if !command.eq_ignore_ascii_case(b"CLIENT") {
        return None;
    }

    let subcommand = items.get(1).and_then(bulk_arg)?;
    if subcommand.eq_ignore_ascii_case(b"TRACKING") {
        Some("CLIENT TRACKING")
    } else if subcommand.eq_ignore_ascii_case(b"CACHING") {
        Some("CLIENT CACHING")
    } else if subcommand.eq_ignore_ascii_case(b"REPLY") {
        Some("CLIENT REPLY")
    } else {
        None
    }
}

fn command_invalidation(frame: &Frame) -> CommandInvalidation {
    let items = match frame {
        Frame::Array(Some(items)) if !items.is_empty() => items,
        _ => return CommandInvalidation::Clear,
    };
    let Some(command) = bulk_arg(&items[0]) else {
        return CommandInvalidation::Clear;
    };
    let command: Vec<u8> = command.iter().map(u8::to_ascii_uppercase).collect();

    if READ_ONLY_COMMANDS.contains(&command.as_slice()) {
        return CommandInvalidation::None;
    }

    if FIRST_KEY_MUTATIONS.contains(&command.as_slice()) {
        return items
            .get(1)
            .and_then(bulk_arg)
            .map(|key| CommandInvalidation::Keys(vec![key.to_vec()]))
            .unwrap_or(CommandInvalidation::Clear);
    }

    match command.as_slice() {
        // XGROUP's first argument is the subcommand; its stream key is the
        // second argument. In particular, CREATE ... MKSTREAM can change a
        // cached TYPE response from `none` to `stream`.
        b"XGROUP" => items
            .get(2)
            .and_then(bulk_arg)
            .map(|key| CommandInvalidation::Keys(vec![key.to_vec()]))
            .unwrap_or(CommandInvalidation::Clear),

        // Every remaining argument is a key.
        b"DEL" | b"UNLINK" => collect_keys(&items[1..]),

        // Alternating key/value pairs.
        b"MSET" | b"MSETNX" => {
            if items.len() < 3 || items.len().is_multiple_of(2) {
                return CommandInvalidation::Clear;
            }
            collect_keys(items[1..].iter().step_by(2))
        }

        // Both endpoints can change.
        b"RENAME" | b"RENAMENX" | b"LMOVE" | b"BLMOVE" | b"RPOPLPUSH" | b"BRPOPLPUSH"
        | b"SMOVE" => collect_keys(items.iter().skip(1).take(2)),

        // Only the destination changes.
        b"COPY" => collect_keys(items.iter().skip(2).take(1)),
        b"BITOP" => collect_keys(items.iter().skip(2).take(1)),
        b"GEOSEARCHSTORE" | b"PFMERGE" | b"SDIFFSTORE" | b"SINTERSTORE" | b"SUNIONSTORE"
        | b"ZDIFFSTORE" | b"ZINTERSTORE" | b"ZUNIONSTORE" => {
            collect_keys(items.iter().skip(1).take(1))
        }

        // Blocking pops use every argument except the trailing timeout as a
        // key. More complex numkeys-based variants conservatively clear below.
        b"BLPOP" | b"BRPOP" | b"BZPOPMAX" | b"BZPOPMIN" if items.len() >= 3 => {
            collect_keys(items.iter().skip(1).take(items.len() - 2))
        }

        // Database switches and global mutations invalidate every local entry.
        b"FLUSHALL" | b"FLUSHDB" | b"SELECT" | b"SWAPDB" => CommandInvalidation::Clear,

        // Unknown commands may be module writes or future Redis mutations.
        _ => CommandInvalidation::Clear,
    }
}

/// Whether `frame` can change cached keyspace state.
///
/// Unknown and malformed commands return `true` so callers fail safely when
/// Redis or a module adds a mutation this crate does not yet classify.
#[doc(hidden)]
pub fn command_may_mutate(frame: &Frame) -> bool {
    !matches!(command_invalidation(frame), CommandInvalidation::None)
}

fn collect_keys<'a>(items: impl IntoIterator<Item = &'a Frame>) -> CommandInvalidation {
    let keys: Option<Vec<Vec<u8>>> = items
        .into_iter()
        .map(|item| bulk_arg(item).map(<[u8]>::to_vec))
        .collect();
    match keys {
        Some(keys) if !keys.is_empty() => CommandInvalidation::Keys(keys),
        _ => CommandInvalidation::Clear,
    }
}

/// A cached response plus the metadata needed to bound and expire it.
struct Entry {
    /// The Redis key this entry depends on (for reverse-index cleanup).
    redis_key: Vec<u8>,
    /// The optional cluster partition and the generation under which this
    /// entry was populated. Standalone caches leave this unset.
    partition: Option<PartitionEpoch>,
    frame: Frame,
    /// When the entry was stored, for the per-entry client TTL.
    stored_at: Instant,
}

/// Aggregate client-side cache counters.
///
/// The snapshot contains no Redis keys or command arguments, so it is safe to
/// expose through diagnostics without leaking data or creating unbounded label
/// cardinality.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStatistics {
    /// Responses served from the local cache.
    pub hits: u64,
    /// Cacheable requests that required a Redis roundtrip.
    pub misses: u64,
    /// Key or full-cache invalidation operations observed locally.
    pub invalidations: u64,
    /// Cached entries removed by invalidation, expiry, or capacity bounds.
    pub evictions: u64,
}

/// A point-in-time invalidation token captured before a cacheable request is
/// sent to Redis. The response may only be inserted if every value still
/// matches.
///
/// This type is public only so sibling workspace crates can integrate with the
/// shared cache implementation. Its fields intentionally remain opaque.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheEpoch {
    generation: u64,
    key_epoch: u64,
    partition: Option<PartitionEpoch>,
}

/// A partition identifier paired with its current ownership generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionEpoch {
    partition: u16,
    generation: u64,
}

/// The client-side cache: response entries keyed by the full command, plus a
/// reverse index from Redis key to the cache entries that depend on it.
///
/// This is an opaque handle. Construct one with [`CacheState::default`] to
/// share a cache between a [`CacheService`](crate::cache_layer::CacheService)
/// and its invalidation task; the cache is managed internally by those types.
pub struct CacheState {
    /// `cache_key -> Entry`.
    entries: HashMap<Vec<u8>, Entry>,
    /// `redis_key -> {cache_key}`.
    index: HashMap<Vec<u8>, HashSet<Vec<u8>>>,
    /// `partition -> {cache_key}` for targeted topology invalidation.
    partition_index: HashMap<u16, HashSet<Vec<u8>>>,
    /// When `false`, the tracking connection is unhealthy: reads pass through to
    /// the server and nothing is cached, so stale data can never be served.
    enabled: bool,
    /// Maximum number of cached entries (`0` = unbounded).
    max_size: usize,
    /// Per-entry freshness deadline (`None` = no client TTL).
    ttl: Option<Duration>,
    /// Advances for full-cache invalidations and whenever per-key epoch
    /// bookkeeping is pruned.
    generation: u64,
    /// Latest invalidation epoch for each Redis key.
    key_epochs: HashMap<Vec<u8>, u64>,
    /// Latest ownership/invalidation generation for each partition.
    partition_generations: HashMap<u16, u64>,
    /// Monotonic source for per-key epochs within a generation.
    next_key_epoch: u64,
    /// Bound for `key_epochs`; never zero.
    max_key_epochs: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    invalidations: AtomicU64,
    evictions: AtomicU64,
    recorder: Option<Arc<dyn MetricsRecorder>>,
}

impl Default for CacheState {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ENTRIES, Some(DEFAULT_TTL))
    }
}

impl CacheState {
    /// Create a cache bounded to `max_size` entries (`0` = unbounded) with an
    /// optional per-entry client TTL.
    pub(crate) fn new(max_size: usize, ttl: Option<Duration>) -> Self {
        Self::new_with_recorder(max_size, ttl, None)
    }

    /// Create a cache and optionally forward aggregate events to a metrics
    /// recorder. The in-memory counters are always enabled.
    pub(crate) fn new_with_recorder(
        max_size: usize,
        ttl: Option<Duration>,
        recorder: Option<Arc<dyn MetricsRecorder>>,
    ) -> Self {
        Self {
            entries: HashMap::new(),
            index: HashMap::new(),
            partition_index: HashMap::new(),
            enabled: true,
            max_size,
            ttl,
            generation: 0,
            key_epochs: HashMap::new(),
            partition_generations: HashMap::new(),
            next_key_epoch: 0,
            max_key_epochs: if max_size == 0 {
                DEFAULT_MAX_EPOCHS
            } else {
                max_size
            }
            .max(1),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            recorder,
        }
    }

    /// Look up a cached response by its full-command cache key. Returns `None`
    /// when caching is disabled (passthrough) or when the entry is older than
    /// the client TTL, so the caller fetches fresh from the server.
    pub(crate) fn get(&self, cache_key: &[u8]) -> Option<&Frame> {
        self.get_inner(cache_key, None)
    }

    /// Look up a response scoped to `partition` (a Redis Cluster slot).
    ///
    /// Entries from a previous partition generation are treated as misses.
    /// This method is public only for the workspace cluster integration.
    #[doc(hidden)]
    pub fn get_in_partition(&self, cache_key: &[u8], partition: u16) -> Option<&Frame> {
        self.get_inner(cache_key, Some(partition))
    }

    fn get_inner(&self, cache_key: &[u8], partition: Option<u16>) -> Option<&Frame> {
        if !self.enabled {
            self.record_event(CacheEvent::Miss, 1);
            return None;
        }
        let Some(entry) = self.entries.get(cache_key) else {
            self.record_event(CacheEvent::Miss, 1);
            return None;
        };
        let expected_partition = partition.map(|partition| self.partition_epoch(partition));
        if entry.partition != expected_partition {
            self.record_event(CacheEvent::Miss, 1);
            return None;
        }
        if let Some(ttl) = self.ttl
            && entry.stored_at.elapsed() >= ttl
        {
            // Expired: don't serve it. The entry is left in place and will be
            // overwritten by the re-fetch's insert (removing it would need
            // &mut, and the Tower read path only holds a read lock).
            self.record_event(CacheEvent::Miss, 1);
            return None;
        }
        self.record_event(CacheEvent::Hit, 1);
        Some(&entry.frame)
    }

    /// Capture the key and full-cache generations before dispatching a cache
    /// miss to Redis.
    ///
    /// Pass this token to [`insert_if_current`](Self::insert_if_current). If a
    /// server invalidation or local write races the request, the token no
    /// longer matches and the stale response is discarded. Returns `None`
    /// while caching is disabled so a read begun without invalidation tracking
    /// can never populate the cache after tracking recovers.
    pub(crate) fn snapshot_epoch(&self, redis_key: &[u8]) -> Option<CacheEpoch> {
        self.snapshot_epoch_inner(redis_key, None)
    }

    /// Capture an invalidation token for a key routed through `partition`.
    ///
    /// A later key invalidation, partition invalidation/ownership change, full
    /// clear, or suspension makes the token stale. This method is public only
    /// for the workspace cluster integration.
    #[doc(hidden)]
    pub fn snapshot_epoch_in_partition(
        &self,
        redis_key: &[u8],
        partition: u16,
    ) -> Option<CacheEpoch> {
        self.snapshot_epoch_inner(redis_key, Some(partition))
    }

    fn snapshot_epoch_inner(&self, redis_key: &[u8], partition: Option<u16>) -> Option<CacheEpoch> {
        self.enabled.then(|| CacheEpoch {
            generation: self.generation,
            key_epoch: self.key_epochs.get(redis_key).copied().unwrap_or(0),
            partition: partition.map(|partition| self.partition_epoch(partition)),
        })
    }

    /// Store `frame` under `cache_key`, recording the reverse-index link to
    /// `redis_key`. If the cache is bounded and full of *new* keys, one
    /// arbitrary existing entry is evicted first.
    #[cfg(test)]
    pub(crate) fn insert(&mut self, cache_key: Vec<u8>, redis_key: Vec<u8>, frame: Frame) -> bool {
        let Some(epoch) = self.snapshot_epoch(&redis_key) else {
            return false;
        };
        self.insert_if_current(cache_key, redis_key, frame, epoch)
    }

    /// Store a response only if its invalidation epoch is still current.
    ///
    /// Returns `false` when caching is disabled or an invalidation raced the
    /// Redis request. A rejected response is never visible through the cache.
    pub(crate) fn insert_if_current(
        &mut self,
        cache_key: Vec<u8>,
        redis_key: Vec<u8>,
        frame: Frame,
        epoch: CacheEpoch,
    ) -> bool {
        self.insert_if_current_inner(cache_key, redis_key, frame, None, epoch)
    }

    /// Store a response only if its key, global, and partition generations are
    /// still current.
    ///
    /// This is the cluster-scoped counterpart to the standalone insertion
    /// path. It is public only for the workspace cluster integration.
    #[doc(hidden)]
    pub fn insert_if_current_in_partition(
        &mut self,
        cache_key: Vec<u8>,
        redis_key: Vec<u8>,
        frame: Frame,
        partition: u16,
        epoch: CacheEpoch,
    ) -> bool {
        self.insert_if_current_inner(cache_key, redis_key, frame, Some(partition), epoch)
    }

    fn insert_if_current_inner(
        &mut self,
        cache_key: Vec<u8>,
        redis_key: Vec<u8>,
        frame: Frame,
        partition: Option<u16>,
        epoch: CacheEpoch,
    ) -> bool {
        // Don't populate the cache while tracking is unhealthy.
        if !self.enabled || self.snapshot_epoch_inner(&redis_key, partition) != Some(epoch) {
            return false;
        }

        // Expired entries remain observable as misses on the read-only lookup
        // path. Remove and count one when their replacement arrives.
        let replacing_expired = self
            .entries
            .get(&cache_key)
            .is_some_and(|entry| self.ttl.is_some_and(|ttl| entry.stored_at.elapsed() >= ttl));
        if replacing_expired && self.remove_entry(&cache_key) {
            self.record_event(CacheEvent::Eviction, 1);
        }

        if self.max_size > 0
            && !self.entries.contains_key(&cache_key)
            && self.entries.len() >= self.max_size
            && let Some(victim) = self.entries.keys().next().cloned()
            && self.remove_entry(&victim)
        {
            self.record_event(CacheEvent::Eviction, 1);
        }

        // An exact-key overwrite should normally point at the same Redis key,
        // but keep the reverse index correct even for manually constructed
        // state in tests or downstream integrations.
        if self.entries.contains_key(&cache_key) {
            self.remove_entry(&cache_key);
        }
        self.index
            .entry(redis_key.clone())
            .or_default()
            .insert(cache_key.clone());
        if let Some(partition) = partition {
            self.partition_index
                .entry(partition)
                .or_default()
                .insert(cache_key.clone());
        }
        self.entries.insert(
            cache_key,
            Entry {
                redis_key,
                partition: epoch.partition,
                frame,
                stored_at: Instant::now(),
            },
        );
        true
    }

    /// Evict every cache entry that depends on `redis_key`.
    #[doc(hidden)]
    pub fn invalidate(&mut self, redis_key: &[u8]) {
        self.advance_key_epoch(redis_key);
        self.record_event(CacheEvent::Invalidation, 1);
        if let Some(cache_keys) = self.index.remove(redis_key) {
            let mut count = 0;
            for ck in cache_keys {
                count += u64::from(self.remove_entry(&ck));
            }
            self.record_event(CacheEvent::Eviction, count);
        }
    }

    /// Evict entries routed through `partition` and advance its generation.
    ///
    /// Advancing even when the partition currently has no entries rejects any
    /// earlier in-flight response. Repeating this for each observed ownership
    /// transition also protects an A -> B -> A cycle: a response issued during
    /// either prior ownership generation cannot become current again.
    #[doc(hidden)]
    pub fn invalidate_partition(&mut self, partition: u16) {
        self.advance_partition_generation(partition);
        self.record_event(CacheEvent::Invalidation, 1);
        if let Some(cache_keys) = self.partition_index.remove(&partition) {
            let mut count = 0;
            for cache_key in cache_keys {
                count += u64::from(self.remove_entry(&cache_key));
            }
            self.record_event(CacheEvent::Eviction, count);
        }
    }

    /// Drop all cached entries and index links.
    #[doc(hidden)]
    pub fn clear(&mut self) {
        self.advance_generation();
        self.key_epochs.clear();
        self.partition_generations.clear();
        self.next_key_epoch = 0;
        self.record_event(CacheEvent::Invalidation, 1);
        let evicted = self.entries.len() as u64;
        self.entries.clear();
        self.index.clear();
        self.partition_index.clear();
        self.record_event(CacheEvent::Eviction, evicted);
    }

    /// Synchronously invalidate cache state affected by `request`.
    ///
    /// Common single- and multi-key mutations invalidate only their affected
    /// keys. Known read-only commands preserve the cache. Unknown or malformed
    /// commands conservatively clear it, which is safe for module commands and
    /// future Redis mutations the client does not yet recognize.
    ///
    /// Cached services call this both before dispatch and after a successful
    /// write. Advancing the epoch twice is intentional: the first call removes
    /// values that predate the write, while the second rejects reads that raced
    /// the write window (including when `NOLOOP` suppresses server pushes).
    #[doc(hidden)]
    pub fn invalidate_for_command(&mut self, request: &Frame) {
        match command_invalidation(request) {
            CommandInvalidation::None => {}
            CommandInvalidation::Keys(keys) => {
                for key in keys {
                    self.invalidate(&key);
                }
            }
            CommandInvalidation::Clear => self.clear(),
        }
    }

    /// Disable caching because the tracking connection was lost: clears all
    /// entries and makes every read pass through to the server until
    /// [`enable`](Self::enable) is called. This is what prevents serving stale
    /// data after invalidations stop arriving.
    #[doc(hidden)]
    pub fn disable(&mut self) {
        self.enabled = false;
        self.clear();
    }

    /// Re-enable caching after the tracking connection is restored.
    #[doc(hidden)]
    pub fn enable(&mut self) {
        self.resume();
    }

    /// Temporarily prevent cache hits and new fills without evicting entries.
    ///
    /// The global generation advances when the cache first becomes suspended,
    /// so responses dispatched before suspension are rejected even if they
    /// arrive after [`resume`](Self::resume). Callers may invalidate only the
    /// affected partitions before resuming, preserving unrelated entries.
    #[doc(hidden)]
    pub fn suspend(&mut self) {
        if self.enabled {
            self.enabled = false;
            self.advance_generation();
        }
    }

    /// Resume cache hits and fills after a temporary suspension.
    #[doc(hidden)]
    pub fn resume(&mut self) {
        self.enabled = true;
    }

    /// Whether caching is currently active (the tracking connection is healthy).
    /// A `false` here means reads are passing through to the server.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Number of cached response entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache holds no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return a lock-free snapshot of the aggregate cache counters.
    pub fn statistics(&self) -> CacheStatistics {
        CacheStatistics {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// Remove a single entry and its reverse-index link.
    fn remove_entry(&mut self, cache_key: &[u8]) -> bool {
        let Some(entry) = self.entries.remove(cache_key) else {
            return false;
        };
        if let Some(set) = self.index.get_mut(&entry.redis_key) {
            set.remove(cache_key);
            if set.is_empty() {
                self.index.remove(&entry.redis_key);
            }
        }
        if let Some(partition) = entry.partition.map(|partition| partition.partition)
            && let Some(set) = self.partition_index.get_mut(&partition)
        {
            set.remove(cache_key);
            if set.is_empty() {
                self.partition_index.remove(&partition);
            }
        }
        true
    }

    fn advance_key_epoch(&mut self, redis_key: &[u8]) {
        if !self.key_epochs.contains_key(redis_key) && self.key_epochs.len() >= self.max_key_epochs
        {
            // Advancing the generation before pruning ensures a miss that
            // captured a now-removed key epoch can never compare equal again.
            self.advance_generation();
            self.key_epochs.clear();
            self.next_key_epoch = 0;
        }

        if self.next_key_epoch == u64::MAX {
            self.advance_generation();
            self.key_epochs.clear();
            self.next_key_epoch = 0;
        }
        self.next_key_epoch += 1;
        self.key_epochs
            .insert(redis_key.to_vec(), self.next_key_epoch);
    }

    fn advance_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1);
    }

    fn partition_epoch(&self, partition: u16) -> PartitionEpoch {
        PartitionEpoch {
            partition,
            generation: self
                .partition_generations
                .get(&partition)
                .copied()
                .unwrap_or(0),
        }
    }

    fn advance_partition_generation(&mut self, partition: u16) {
        if self.partition_generations.get(&partition) == Some(&u64::MAX) {
            // Global advancement makes every outstanding token stale before
            // resetting bounded partition-generation bookkeeping.
            self.advance_generation();
            self.partition_generations.clear();
        }
        *self.partition_generations.entry(partition).or_default() += 1;
    }

    fn record_event(&self, event: CacheEvent, count: u64) {
        if count == 0 {
            return;
        }
        let counter = match event {
            CacheEvent::Hit => &self.hits,
            CacheEvent::Miss => &self.misses,
            CacheEvent::Invalidation => &self.invalidations,
            CacheEvent::Eviction => &self.evictions,
        };
        counter.fetch_add(count, Ordering::Relaxed);
        if let Some(recorder) = &self.recorder {
            recorder.cache_event(event, count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingMetrics {
        events: Mutex<Vec<(CacheEvent, u64)>>,
    }

    impl MetricsRecorder for RecordingMetrics {
        fn command_completed(
            &self,
            _command: &str,
            _duration: Duration,
            _error: Option<crate::metrics_layer::ErrorKind>,
        ) {
        }

        fn cache_event(&self, event: CacheEvent, count: u64) {
            self.events.lock().unwrap().push((event, count));
        }
    }

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Some(Bytes::from(s.to_string())))
    }

    fn frame(parts: &[&str]) -> Frame {
        Frame::Array(Some(parts.iter().map(|p| bulk(p)).collect()))
    }

    #[test]
    fn hget_fields_do_not_collide() {
        // The core bug: HGET h f1 and HGET h f2 must be distinct cache keys.
        let (k1, rk1) = extract_cache_entry(&frame(&["HGET", "h", "f1"])).unwrap();
        let (k2, rk2) = extract_cache_entry(&frame(&["HGET", "h", "f2"])).unwrap();
        assert_ne!(k1, k2, "different fields must not share a cache key");
        assert_eq!(rk1, rk2, "both still depend on Redis key `h`");
        assert_eq!(rk1, b"h".to_vec());
    }

    #[test]
    fn command_name_is_case_normalized() {
        let (lower, _) = extract_cache_entry(&frame(&["get", "k"])).unwrap();
        let (upper, _) = extract_cache_entry(&frame(&["GET", "k"])).unwrap();
        assert_eq!(lower, upper);
    }

    #[test]
    fn non_cacheable_returns_none() {
        assert!(extract_cache_entry(&frame(&["SET", "k", "v"])).is_none());
    }

    #[test]
    fn invalidate_evicts_all_variants_of_a_key() {
        let mut state = CacheState::default();
        let (k1, rk1) = extract_cache_entry(&frame(&["HGET", "h", "f1"])).unwrap();
        let (k2, rk2) = extract_cache_entry(&frame(&["HGET", "h", "f2"])).unwrap();
        state.insert(k1.clone(), rk1, bulk("v1"));
        state.insert(k2.clone(), rk2, bulk("v2"));
        assert_eq!(state.len(), 2);

        // Invalidating `h` must drop both variants.
        state.invalidate(b"h");
        assert_eq!(state.len(), 0);
        assert!(state.get(&k1).is_none());
        assert!(state.get(&k2).is_none());
    }

    #[test]
    fn invalidate_other_key_leaves_entry() {
        let mut state = CacheState::default();
        let (k, rk) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        state.insert(k.clone(), rk, bulk("v"));
        state.invalidate(b"b"); // unrelated key
        assert_eq!(state.len(), 1);
        assert!(state.get(&k).is_some());
    }

    #[test]
    fn eviction_cleans_reverse_index() {
        let mut state = CacheState::new(1, None);
        let (k1, rk1) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        let (k2, rk2) = extract_cache_entry(&frame(&["GET", "b"])).unwrap();
        state.insert(k1, rk1, bulk("va"));
        state.insert(k2.clone(), rk2, bulk("vb")); // evicts the "a" entry
        assert_eq!(state.len(), 1);
        // The evicted key's index link is gone, so invalidating it is a no-op
        // and does not disturb the surviving entry.
        state.invalidate(b"a");
        assert_eq!(state.len(), 1);
        assert!(state.get(&k2).is_some());
    }

    #[test]
    fn parse_invalidation_keys_are_bytes() {
        let f = Frame::Push(vec![
            bulk("invalidate"),
            Frame::Array(Some(vec![bulk("k1"), bulk("k2")])),
        ]);
        assert_eq!(
            parse_invalidation(&f),
            Some(vec![b"k1".to_vec(), b"k2".to_vec()])
        );
    }

    #[test]
    fn parse_invalidation_accepts_resp3_simple_strings() {
        let f = Frame::Push(vec![
            Frame::SimpleString(Bytes::from_static(b"invalidate")),
            Frame::Array(Some(vec![Frame::SimpleString(Bytes::from_static(b"key"))])),
        ]);
        assert_eq!(parse_invalidation(&f), Some(vec![b"key".to_vec()]));
    }

    #[test]
    fn malformed_recognized_invalidation_fails_safe_to_full_clear() {
        let f = Frame::Push(vec![
            Frame::SimpleString(Bytes::from_static(b"invalidate")),
            Frame::Array(Some(vec![Frame::Integer(42)])),
        ]);
        assert_eq!(parse_invalidation(&f), Some(Vec::new()));
    }

    #[test]
    fn malformed_or_missing_invalidation_payload_fails_safe_to_full_clear() {
        for frame in [
            Frame::Push(vec![bulk("invalidate")]),
            Frame::Push(vec![bulk("invalidate"), bulk("not-an-array")]),
        ] {
            assert_eq!(parse_invalidation(&frame), Some(Vec::new()));
        }
    }

    #[test]
    fn parse_invalidation_flush_is_empty() {
        let f = Frame::Push(vec![bulk("invalidate"), Frame::Null]);
        assert_eq!(parse_invalidation(&f), Some(Vec::new()));
    }

    #[test]
    fn parse_invalidation_non_invalidate_is_none() {
        let f = Frame::Push(vec![bulk("other"), Frame::Null]);
        assert!(parse_invalidation(&f).is_none());
    }

    #[test]
    fn managed_connection_state_commands_are_detected_without_blocking_diagnostics() {
        for (parts, expected) in [
            (&["CLIENT", "TRACKING", "OFF"][..], "CLIENT TRACKING"),
            (&["client", "caching", "yes"][..], "CLIENT CACHING"),
            (&["CLIENT", "REPLY", "OFF"][..], "CLIENT REPLY"),
            (&["RESET"][..], "RESET"),
            (&["HELLO", "2"][..], "HELLO"),
            (&["QUIT"][..], "QUIT"),
        ] {
            assert_eq!(managed_cache_state_command(&frame(parts)), Some(expected));
        }

        for parts in [
            &["CLIENT", "TRACKINGINFO"][..],
            &["CLIENT", "GETREDIR"][..],
            &["CLIENT", "ID"][..],
            &["GET", "key"][..],
        ] {
            assert_eq!(managed_cache_state_command(&frame(parts)), None);
        }
    }

    #[test]
    fn disabled_cache_passes_through_and_drops_writes() {
        let mut state = CacheState::default();
        let (k, rk) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        state.insert(k.clone(), rk.clone(), bulk("v"));
        assert!(state.get(&k).is_some());

        // Losing the tracking connection: disable clears entries and forces
        // every read to pass through (never serve stale data).
        state.disable();
        assert!(!state.is_enabled());
        assert_eq!(state.len(), 0);
        assert!(state.get(&k).is_none());

        // Writes while disabled are dropped, so nothing accumulates uninvalidated.
        state.insert(k.clone(), rk, bulk("v2"));
        assert_eq!(state.len(), 0);

        // Re-enabling (tracking restored) resumes normal caching.
        state.enable();
        let (k2, rk2) = extract_cache_entry(&frame(&["GET", "b"])).unwrap();
        state.insert(k2.clone(), rk2, bulk("vb"));
        assert!(state.get(&k2).is_some());
    }

    #[test]
    fn disabled_cache_does_not_issue_an_insertion_epoch() {
        let mut state = CacheState::default();
        let (_, redis_key) = extract_cache_entry(&frame(&["GET", "key"])).unwrap();

        state.disable();
        assert!(state.snapshot_epoch(&redis_key).is_none());

        state.enable();
        assert!(state.snapshot_epoch(&redis_key).is_some());
    }

    #[test]
    fn ttl_command_is_not_cacheable() {
        // A cached TTL countdown would be wrong the instant it is stored.
        assert!(extract_cache_entry(&frame(&["TTL", "k"])).is_none());
    }

    #[test]
    fn entry_expires_after_client_ttl() {
        let mut state = CacheState::new(0, Some(Duration::from_millis(10)));
        let (k, rk) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        state.insert(k.clone(), rk, bulk("v"));
        assert!(state.get(&k).is_some(), "fresh entry is a hit");

        std::thread::sleep(Duration::from_millis(25));
        assert!(
            state.get(&k).is_none(),
            "an entry past the client TTL must not be served"
        );
    }

    #[test]
    fn no_ttl_means_no_expiry() {
        let mut state = CacheState::new(0, None);
        let (k, rk) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        state.insert(k.clone(), rk, bulk("v"));
        std::thread::sleep(Duration::from_millis(15));
        assert!(
            state.get(&k).is_some(),
            "no TTL configured -> never expires"
        );
    }

    #[test]
    fn zero_ttl_never_serves_a_cached_response() {
        let mut state = CacheState::new(1, Some(Duration::ZERO));
        let (cache_key, redis_key) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        state.insert(cache_key.clone(), redis_key, bulk("value"));
        assert!(state.get(&cache_key).is_none());
    }

    #[test]
    fn key_invalidation_rejects_a_racing_miss() {
        let mut state = CacheState::default();
        let (cache_key, redis_key) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        let before_request = state.snapshot_epoch(&redis_key).unwrap();

        state.invalidate(&redis_key);

        assert!(!state.insert_if_current(
            cache_key.clone(),
            redis_key,
            bulk("stale"),
            before_request,
        ));
        assert!(state.get(&cache_key).is_none());
    }

    #[test]
    fn partition_invalidation_preserves_unrelated_entries() {
        let mut state = CacheState::default();
        let (a_cache_key, a_key) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        let (b_cache_key, b_key) = extract_cache_entry(&frame(&["GET", "b"])).unwrap();
        let a_epoch = state.snapshot_epoch_in_partition(&a_key, 1).unwrap();
        let b_epoch = state.snapshot_epoch_in_partition(&b_key, 2).unwrap();
        assert!(state.insert_if_current_in_partition(
            a_cache_key.clone(),
            a_key,
            bulk("va"),
            1,
            a_epoch,
        ));
        assert!(state.insert_if_current_in_partition(
            b_cache_key.clone(),
            b_key,
            bulk("vb"),
            2,
            b_epoch,
        ));

        state.invalidate_partition(1);

        assert!(state.get_in_partition(&a_cache_key, 1).is_none());
        assert!(state.get_in_partition(&b_cache_key, 2).is_some());
        assert_eq!(state.len(), 1);
    }

    #[test]
    fn partition_generation_rejects_racing_fills_across_owner_cycles() {
        let mut state = CacheState::default();
        let (cache_key, redis_key) = extract_cache_entry(&frame(&["GET", "moved"])).unwrap();
        let owner_a_first = state.snapshot_epoch_in_partition(&redis_key, 42).unwrap();

        // Observe A -> B, then capture a request while B owns the slot.
        state.invalidate_partition(42);
        let owner_b = state.snapshot_epoch_in_partition(&redis_key, 42).unwrap();

        // Observe B -> A. Neither response from a prior ownership generation
        // may become valid merely because the original owner has returned.
        state.invalidate_partition(42);
        assert!(!state.insert_if_current_in_partition(
            cache_key.clone(),
            redis_key.clone(),
            bulk("stale-a"),
            42,
            owner_a_first,
        ));
        assert!(!state.insert_if_current_in_partition(
            cache_key.clone(),
            redis_key.clone(),
            bulk("stale-b"),
            42,
            owner_b,
        ));

        let owner_a_second = state.snapshot_epoch_in_partition(&redis_key, 42).unwrap();
        assert!(state.insert_if_current_in_partition(
            cache_key.clone(),
            redis_key,
            bulk("fresh-a"),
            42,
            owner_a_second,
        ));
        assert!(state.get_in_partition(&cache_key, 42).is_some());
    }

    #[test]
    fn scoped_and_standalone_entries_cannot_cross_lookup_domains() {
        let mut state = CacheState::default();
        let (cache_key, redis_key) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        let epoch = state.snapshot_epoch_in_partition(&redis_key, 7).unwrap();
        assert!(state.insert_if_current_in_partition(
            cache_key.clone(),
            redis_key,
            bulk("value"),
            7,
            epoch,
        ));

        assert!(state.get(&cache_key).is_none());
        assert!(state.get_in_partition(&cache_key, 8).is_none());
        assert!(state.get_in_partition(&cache_key, 7).is_some());
    }

    #[test]
    fn suspension_rejects_hits_and_fills_but_preserves_existing_entries() {
        let mut state = CacheState::default();
        let (cached_key, cached_redis_key) =
            extract_cache_entry(&frame(&["GET", "cached"])).unwrap();
        let cached_epoch = state
            .snapshot_epoch_in_partition(&cached_redis_key, 1)
            .unwrap();
        assert!(state.insert_if_current_in_partition(
            cached_key.clone(),
            cached_redis_key,
            bulk("value"),
            1,
            cached_epoch,
        ));

        let (racing_key, racing_redis_key) =
            extract_cache_entry(&frame(&["GET", "racing"])).unwrap();
        let before_suspend = state
            .snapshot_epoch_in_partition(&racing_redis_key, 2)
            .unwrap();

        state.suspend();
        assert!(!state.is_enabled());
        assert!(state.get_in_partition(&cached_key, 1).is_none());
        assert!(
            state
                .snapshot_epoch_in_partition(&racing_redis_key, 2)
                .is_none()
        );
        assert!(!state.insert_if_current_in_partition(
            racing_key.clone(),
            racing_redis_key.clone(),
            bulk("stale"),
            2,
            before_suspend,
        ));
        assert_eq!(state.len(), 1, "suspension itself preserves entries");

        state.resume();
        assert!(state.is_enabled());
        assert!(state.get_in_partition(&cached_key, 1).is_some());
        assert!(!state.insert_if_current_in_partition(
            racing_key,
            racing_redis_key,
            bulk("late"),
            2,
            before_suspend,
        ));
    }

    #[test]
    fn key_and_capacity_eviction_clean_the_partition_index() {
        let mut state = CacheState::new(1, None);
        let (a_cache_key, a_key) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        let a_epoch = state.snapshot_epoch_in_partition(&a_key, 1).unwrap();
        assert!(state.insert_if_current_in_partition(
            a_cache_key,
            a_key.clone(),
            bulk("a"),
            1,
            a_epoch,
        ));
        state.invalidate(&a_key);
        assert!(!state.partition_index.contains_key(&1));

        let (b_cache_key, b_key) = extract_cache_entry(&frame(&["GET", "b"])).unwrap();
        let b_epoch = state.snapshot_epoch_in_partition(&b_key, 2).unwrap();
        assert!(state.insert_if_current_in_partition(b_cache_key, b_key, bulk("b"), 2, b_epoch,));
        let (c_cache_key, c_key) = extract_cache_entry(&frame(&["GET", "c"])).unwrap();
        let c_epoch = state.snapshot_epoch_in_partition(&c_key, 3).unwrap();
        assert!(state.insert_if_current_in_partition(c_cache_key, c_key, bulk("c"), 3, c_epoch,));
        assert!(!state.partition_index.contains_key(&2));
        assert!(state.partition_index.contains_key(&3));
    }

    #[test]
    fn full_clear_rejects_every_racing_miss() {
        let mut state = CacheState::default();
        let (cache_key, redis_key) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        let before_request = state.snapshot_epoch(&redis_key).unwrap();

        state.clear();

        assert!(!state.insert_if_current(cache_key, redis_key, bulk("stale"), before_request,));
    }

    #[test]
    fn pruning_key_epochs_advances_the_global_generation() {
        let mut state = CacheState {
            max_key_epochs: 1,
            ..CacheState::default()
        };
        let (cache_key, redis_key) = extract_cache_entry(&frame(&["GET", "old"])).unwrap();
        let before_prune = state.snapshot_epoch(&redis_key).unwrap();

        state.invalidate(b"first");
        state.invalidate(b"second"); // prunes `first` before recording `second`

        assert_eq!(state.key_epochs.len(), 1);
        assert!(!state.insert_if_current(cache_key, redis_key, bulk("stale"), before_prune,));
    }

    #[test]
    fn local_writes_invalidate_only_known_keys_and_advance_twice() {
        let mut state = CacheState::default();
        let (a_cache_key, a_key) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        let (b_cache_key, b_key) = extract_cache_entry(&frame(&["GET", "b"])).unwrap();
        state.insert(a_cache_key.clone(), a_key.clone(), bulk("old-a"));
        state.insert(b_cache_key.clone(), b_key, bulk("old-b"));

        state.invalidate_for_command(&frame(&["SET", "a", "new-a"]));
        let during_write = state.snapshot_epoch(&a_key).unwrap();
        state.invalidate_for_command(&frame(&["SET", "a", "new-a"]));

        assert!(state.get(&a_cache_key).is_none());
        assert!(state.get(&b_cache_key).is_some());
        assert!(!state.insert_if_current(a_cache_key, a_key, bulk("raced"), during_write));
    }

    #[test]
    fn known_reads_preserve_cache_and_unknown_commands_clear_it() {
        let mut state = CacheState::default();
        let (cache_key, redis_key) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        state.insert(cache_key.clone(), redis_key.clone(), bulk("value"));

        state.invalidate_for_command(&frame(&["TTL", "a"]));
        assert!(state.get(&cache_key).is_some());

        state.invalidate_for_command(&frame(&["FUTURE.WRITE", "a"]));
        assert!(state.get(&cache_key).is_none());
    }

    #[test]
    fn mutation_classifier_handles_reads_stateful_commands_and_xgroup_key() {
        assert!(!command_may_mutate(&frame(&["TTL", "a"])));
        assert!(command_may_mutate(&frame(&["SET", "a", "value"])));
        assert!(command_may_mutate(&frame(&["CLIENT", "TRACKING", "OFF"])));

        let mut state = CacheState::default();
        let (cache_key, redis_key) = extract_cache_entry(&frame(&["TYPE", "stream-key"])).unwrap();
        state.insert(cache_key.clone(), redis_key, bulk("none"));
        state.invalidate_for_command(&frame(&[
            "XGROUP",
            "CREATE",
            "stream-key",
            "group",
            "$",
            "MKSTREAM",
        ]));
        assert!(state.get(&cache_key).is_none());
    }

    #[test]
    fn cache_statistics_count_hits_misses_invalidations_and_evictions() {
        let mut state = CacheState::default();
        let (cache_key, redis_key) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();

        assert!(state.get(&cache_key).is_none());
        state.insert(cache_key.clone(), redis_key.clone(), bulk("value"));
        assert!(state.get(&cache_key).is_some());
        state.invalidate(&redis_key);

        assert_eq!(
            state.statistics(),
            CacheStatistics {
                hits: 1,
                misses: 1,
                invalidations: 1,
                evictions: 1,
            }
        );
    }

    #[test]
    fn cache_events_are_forwarded_to_the_metrics_recorder() {
        let recorder = Arc::new(RecordingMetrics::default());
        let metrics: Arc<dyn MetricsRecorder> = recorder.clone();
        let mut state = CacheState::new_with_recorder(1, None, Some(metrics));
        let (cache_key, redis_key) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();

        assert!(state.get(&cache_key).is_none());
        state.insert(cache_key.clone(), redis_key.clone(), bulk("value"));
        assert!(state.get(&cache_key).is_some());
        state.invalidate(&redis_key);

        assert_eq!(
            *recorder.events.lock().unwrap(),
            vec![
                (CacheEvent::Miss, 1),
                (CacheEvent::Hit, 1),
                (CacheEvent::Invalidation, 1),
                (CacheEvent::Eviction, 1),
            ]
        );
    }

    #[test]
    fn replacing_an_expired_entry_counts_an_eviction() {
        let mut state = CacheState::new(1, Some(Duration::from_millis(1)));
        let (cache_key, redis_key) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        state.insert(cache_key.clone(), redis_key.clone(), bulk("old"));
        std::thread::sleep(Duration::from_millis(5));
        assert!(state.get(&cache_key).is_none());

        state.insert(cache_key, redis_key, bulk("fresh"));

        assert_eq!(state.statistics().evictions, 1);
    }
}
