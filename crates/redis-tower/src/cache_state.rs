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

use redis_tower_core::Frame;

/// The cacheable read commands. The key these read is always argument 1.
const CACHEABLE: &[&[u8]] = &[
    b"GET",
    b"HGET",
    b"HGETALL",
    b"LRANGE",
    b"SMEMBERS",
    b"ZRANGE",
    b"TYPE",
    b"TTL",
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
pub(crate) fn extract_cache_entry(frame: &Frame) -> Option<(Vec<u8>, Vec<u8>)> {
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
pub(crate) fn parse_invalidation(frame: &Frame) -> Option<Vec<Vec<u8>>> {
    let items = match frame {
        Frame::Push(items) if items.len() >= 2 => items,
        Frame::Array(Some(items)) if items.len() >= 2 => items,
        _ => return None,
    };

    match &items[0] {
        Frame::BulkString(Some(b)) if b.as_ref() == b"invalidate" => {}
        _ => return None,
    }

    match &items[1] {
        Frame::Array(Some(keys)) => {
            let mut result = Vec::new();
            for key in keys {
                if let Frame::BulkString(Some(b)) = key {
                    result.push(b.to_vec());
                }
            }
            Some(result)
        }
        Frame::Null | Frame::Array(None) => Some(Vec::new()),
        _ => None,
    }
}

/// The client-side cache: response entries keyed by the full command, plus a
/// reverse index from Redis key to the cache entries that depend on it.
///
/// This is an opaque handle. Construct one with [`CacheState::default`] to
/// share a cache between a [`CacheService`](crate::cache_layer::CacheService)
/// and its invalidation task; the cache is managed internally by those types.
pub struct CacheState {
    /// `cache_key -> (redis_key, cached response)`. The redis_key is stored so
    /// eviction can clean up the reverse index without a scan.
    entries: HashMap<Vec<u8>, (Vec<u8>, Frame)>,
    /// `redis_key -> {cache_key}`.
    index: HashMap<Vec<u8>, HashSet<Vec<u8>>>,
    /// When `false`, the tracking connection is unhealthy: reads pass through to
    /// the server and nothing is cached, so stale data can never be served.
    enabled: bool,
}

impl Default for CacheState {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            index: HashMap::new(),
            enabled: true,
        }
    }
}

impl CacheState {
    /// Look up a cached response by its full-command cache key. Returns `None`
    /// while caching is disabled, so the caller fetches fresh from the server.
    pub(crate) fn get(&self, cache_key: &[u8]) -> Option<&Frame> {
        if !self.enabled {
            return None;
        }
        self.entries.get(cache_key).map(|(_, frame)| frame)
    }

    /// Store `frame` under `cache_key`, recording the reverse-index link to
    /// `redis_key`. If `max_size > 0` and the cache is full of *new* keys, one
    /// arbitrary existing entry is evicted first.
    pub(crate) fn insert(
        &mut self,
        cache_key: Vec<u8>,
        redis_key: Vec<u8>,
        frame: Frame,
        max_size: usize,
    ) {
        // Don't populate the cache while tracking is unhealthy.
        if !self.enabled {
            return;
        }
        if max_size > 0
            && !self.entries.contains_key(&cache_key)
            && self.entries.len() >= max_size
            && let Some(victim) = self.entries.keys().next().cloned()
        {
            self.remove(&victim);
        }
        self.index
            .entry(redis_key.clone())
            .or_default()
            .insert(cache_key.clone());
        self.entries.insert(cache_key, (redis_key, frame));
    }

    /// Evict every cache entry that depends on `redis_key`.
    pub(crate) fn invalidate(&mut self, redis_key: &[u8]) {
        if let Some(cache_keys) = self.index.remove(redis_key) {
            for ck in cache_keys {
                self.entries.remove(&ck);
            }
        }
    }

    /// Drop all cached entries and index links.
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.index.clear();
    }

    /// Disable caching because the tracking connection was lost: clears all
    /// entries and makes every read pass through to the server until
    /// [`enable`](Self::enable) is called. This is what prevents serving stale
    /// data after invalidations stop arriving.
    pub(crate) fn disable(&mut self) {
        self.enabled = false;
        self.clear();
    }

    /// Re-enable caching after the tracking connection is restored.
    pub(crate) fn enable(&mut self) {
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

    /// Remove a single entry and its reverse-index link.
    fn remove(&mut self, cache_key: &[u8]) {
        if let Some((redis_key, _)) = self.entries.remove(cache_key)
            && let Some(set) = self.index.get_mut(&redis_key)
        {
            set.remove(cache_key);
            if set.is_empty() {
                self.index.remove(&redis_key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

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
        state.insert(k1.clone(), rk1, bulk("v1"), 0);
        state.insert(k2.clone(), rk2, bulk("v2"), 0);
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
        state.insert(k.clone(), rk, bulk("v"), 0);
        state.invalidate(b"b"); // unrelated key
        assert_eq!(state.len(), 1);
        assert!(state.get(&k).is_some());
    }

    #[test]
    fn eviction_cleans_reverse_index() {
        let mut state = CacheState::default();
        let (k1, rk1) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        let (k2, rk2) = extract_cache_entry(&frame(&["GET", "b"])).unwrap();
        state.insert(k1, rk1, bulk("va"), 1);
        state.insert(k2.clone(), rk2, bulk("vb"), 1); // evicts the "a" entry
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
    fn disabled_cache_passes_through_and_drops_writes() {
        let mut state = CacheState::default();
        let (k, rk) = extract_cache_entry(&frame(&["GET", "a"])).unwrap();
        state.insert(k.clone(), rk.clone(), bulk("v"), 0);
        assert!(state.get(&k).is_some());

        // Losing the tracking connection: disable clears entries and forces
        // every read to pass through (never serve stale data).
        state.disable();
        assert!(!state.is_enabled());
        assert_eq!(state.len(), 0);
        assert!(state.get(&k).is_none());

        // Writes while disabled are dropped, so nothing accumulates uninvalidated.
        state.insert(k.clone(), rk, bulk("v2"), 0);
        assert_eq!(state.len(), 0);

        // Re-enabling (tracking restored) resumes normal caching.
        state.enable();
        let (k2, rk2) = extract_cache_entry(&frame(&["GET", "b"])).unwrap();
        state.insert(k2.clone(), rk2, bulk("vb"), 0);
        assert!(state.get(&k2).is_some());
    }
}
