//! Client-side caching with automatic invalidation.
//!
//! Uses two connections: one for commands, one for receiving invalidation
//! push messages via CLIENT TRACKING BCAST. The tracking connection runs
//! a background reader that processes invalidation pushes as they arrive.
//!
//! Cache keying and invalidation are shared with the Tower
//! [`CacheService`](crate::cache_layer::CacheService) so the two never diverge.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::caching::CachedClient;
//! use redis_tower::commands::*;
//!
//! let client = CachedClient::connect("127.0.0.1:6379").await?;
//!
//! // First call hits Redis and caches the result.
//! let val = client.execute(Get::new("key")).await?;
//!
//! // Second call returns cached value (no roundtrip).
//! let val = client.execute(Get::new("key")).await?;
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use redis_tower_commands::ClientTracking;
use redis_tower_core::{Command, Frame, RedisConnection, RedisError};
use tokio::sync::Mutex;

use crate::cache_state::{CacheState, extract_cache_entry, parse_invalidation};

/// A Redis client with local caching and automatic invalidation.
///
/// Uses two RESP3 connections internally: one for data commands and one
/// for receiving invalidation push messages via `CLIENT TRACKING ON BCAST`.
/// Cacheable read commands (GET, HGET, HGETALL, etc.) are served from
/// the local cache when available, and invalidated automatically when
/// the server notifies that keys have changed.
///
/// Cache keys are the full command argument vector, so `HGET h f1` and
/// `HGET h f2` are distinct entries; invalidation of a Redis key evicts every
/// cached variant that reads it.
///
/// # Example
///
/// ```ignore
/// use redis_tower::caching::CachedClient;
/// use redis_tower::commands::*;
///
/// let mut client = CachedClient::connect("127.0.0.1:6379").await?;
///
/// // First call hits Redis.
/// let val: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
///
/// // Second call returns cached value (no roundtrip).
/// let val: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
/// ```
pub struct CachedClient {
    conn: RedisConnection,
    cache: Arc<Mutex<CacheState>>,
    _reader_task: tokio::task::JoinHandle<()>,
}

impl CachedClient {
    /// Connect to Redis with client-side caching enabled.
    ///
    /// Opens two RESP3 connections:
    /// - Data connection for commands
    /// - Tracking connection with CLIENT TRACKING ON BCAST for invalidations
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        // Data connection.
        let conn = RedisConnection::connect_resp3(addr).await?;

        // Initial tracking connection -- fail fast if the server can't track.
        let initial_stream = connect_tracking_stream(addr).await?;

        let cache: Arc<Mutex<CacheState>> = Arc::new(Mutex::new(CacheState::default()));
        let cache_for_task = Arc::clone(&cache);
        let addr = addr.to_string();

        // Background reader: processes invalidations, and -- crucially -- when
        // the tracking connection dies it disables caching (so stale data is
        // never served) and reconnects with backoff before re-enabling.
        let reader_task = tokio::spawn(async move {
            let mut stream = initial_stream;
            loop {
                while let Some(Ok(frame)) = stream.next().await {
                    if let Some(keys) = parse_invalidation(&frame) {
                        let mut c = cache_for_task.lock().await;
                        if keys.is_empty() {
                            c.clear();
                        } else {
                            for key in &keys {
                                c.invalidate(key);
                            }
                        }
                    }
                }

                // Tracking connection lost: disable caching (clears entries and
                // forces every read to pass through to the server).
                cache_for_task.lock().await.disable();

                // Reconnect with capped exponential backoff, replaying CLIENT
                // TRACKING, then re-enable caching once healthy again.
                let mut backoff = Duration::from_millis(100);
                stream = loop {
                    tokio::time::sleep(backoff).await;
                    match connect_tracking_stream(&addr).await {
                        Ok(s) => break s,
                        Err(_) => backoff = (backoff * 2).min(Duration::from_secs(5)),
                    }
                };
                cache_for_task.lock().await.enable();
            }
        });

        Ok(Self {
            conn,
            cache,
            _reader_task: reader_task,
        })
    }

    /// Execute a command. Cacheable reads are served from cache if available.
    pub async fn execute<Cmd: Command>(&mut self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let frame = cmd.to_frame();

        if let Some((cache_key, redis_key)) = extract_cache_entry(&frame) {
            // Check cache.
            {
                let cache = self.cache.lock().await;
                if let Some(cached_frame) = cache.get(&cache_key) {
                    return cmd.parse_response(cached_frame.clone());
                }
            }

            // Cache miss -- fetch from server.
            let responses = self.conn.execute_pipeline(vec![frame]).await?;
            let response = responses
                .into_iter()
                .next()
                .ok_or(RedisError::ConnectionClosed)?;

            if let Frame::Error(ref e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
            }

            // Store in cache (unbounded; the layer-based cache bounds size).
            {
                let mut cache = self.cache.lock().await;
                cache.insert(cache_key, redis_key, response.clone(), 0);
            }

            cmd.parse_response(response)
        } else {
            // Non-cacheable command -- execute directly.
            self.conn.execute(cmd).await
        }
    }

    /// Get the number of entries in the cache.
    pub async fn cache_size(&self) -> usize {
        self.cache.lock().await.len()
    }

    /// Clear the local cache.
    pub async fn clear_cache(&self) {
        self.cache.lock().await.clear();
    }

    /// Whether client-side caching is currently active.
    ///
    /// Returns `false` while the tracking connection is down and being
    /// re-established; during that window reads pass through to the server
    /// (fresh, uncached) so stale data is never served. Use this as a health
    /// signal.
    pub async fn is_caching_healthy(&self) -> bool {
        self.cache.lock().await.is_enabled()
    }
}

/// Open a tracking connection (RESP3 + `CLIENT TRACKING ON BCAST`) and return
/// its invalidation-push stream. Both the initial connect and every reconnect
/// go through here, so they share one stream type.
async fn connect_tracking_stream(
    addr: &str,
) -> Result<
    // `use<>`: the returned stream owns its connection and does not borrow
    // `addr`, so it captures no lifetimes (Rust 2024 would otherwise assume it
    // borrows `addr`, which breaks moving it into the 'static reader task).
    impl futures::Stream<Item = Result<Frame, redis_tower_protocol::ProtocolError>>
    + Unpin
    + Send
    + use<>,
    RedisError,
> {
    let mut tracking_conn = RedisConnection::connect_resp3(addr).await?;
    tracking_conn.execute(ClientTracking::on().bcast()).await?;
    let tracking_framed = tracking_conn.into_framed()?;
    let (_sink, stream) = tracking_framed.split();
    Ok(stream)
}
