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

        // Tracking connection -- BCAST mode pushes all key invalidations.
        let mut tracking_conn = RedisConnection::connect_resp3(addr).await?;
        tracking_conn.execute(ClientTracking::on().bcast()).await?;

        // Take ownership of the tracking connection's framed stream.
        let tracking_framed = tracking_conn.into_framed()?;

        let cache: Arc<Mutex<CacheState>> = Arc::new(Mutex::new(CacheState::default()));
        let cache_for_task = Arc::clone(&cache);

        // Background reader: reads from the tracking connection and
        // processes invalidation messages.
        let reader_task = tokio::spawn(async move {
            let (_sink, mut stream) = tracking_framed.split();
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
}
