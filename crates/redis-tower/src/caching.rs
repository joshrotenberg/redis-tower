//! Client-side caching with automatic invalidation.
//!
//! Uses two connections: one for commands, one for receiving invalidation
//! push messages via CLIENT TRACKING BCAST. The tracking connection runs
//! a background reader that processes invalidation pushes as they arrive.
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

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use redis_tower_commands::ClientTracking;
use redis_tower_core::{Command, Frame, RedisConnection, RedisError};
use tokio::sync::Mutex;

/// A Redis client with local caching and automatic invalidation.
pub struct CachedClient {
    conn: RedisConnection,
    cache: Arc<Mutex<HashMap<String, Frame>>>,
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

        let cache: Arc<Mutex<HashMap<String, Frame>>> = Arc::new(Mutex::new(HashMap::new()));
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
                            c.retain(|cache_key, _| !cache_key.ends_with(&format!(":{key}")));
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

        if let Some(cache_key) = extract_cache_key(&frame) {
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

            // Store in cache.
            {
                let mut cache = self.cache.lock().await;
                cache.insert(cache_key, response.clone());
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

/// Parse an invalidation push message.
///
/// Handles both `Push` (RESP3) and `Array` (REDIRECT) frames.
fn parse_invalidation(frame: &Frame) -> Option<Vec<String>> {
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
                    result.push(String::from_utf8_lossy(b).into_owned());
                }
            }
            Some(result)
        }
        Frame::Null | Frame::Array(None) => Some(Vec::new()),
        _ => None,
    }
}

/// Extract a cache key from a command frame for cacheable reads.
fn extract_cache_key(frame: &Frame) -> Option<String> {
    let items = match frame {
        Frame::Array(Some(items)) if items.len() >= 2 => items,
        _ => return None,
    };

    let cmd_name = match &items[0] {
        Frame::BulkString(Some(b)) => b.as_ref(),
        _ => return None,
    };

    let upper: Vec<u8> = cmd_name.iter().map(|b| b.to_ascii_uppercase()).collect();

    let cacheable = matches!(
        upper.as_slice(),
        b"GET" | b"HGET" | b"HGETALL" | b"LRANGE" | b"SMEMBERS" | b"ZRANGE" | b"TYPE" | b"TTL"
    );

    if !cacheable {
        return None;
    }

    let key = match &items[1] {
        Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        _ => return None,
    };

    Some(format!("{}:{}", String::from_utf8_lossy(&upper), key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parse_invalidation_single_key() {
        let frame = Frame::Push(vec![
            Frame::BulkString(Some(Bytes::from("invalidate"))),
            Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from("mykey")))])),
        ]);
        let keys = parse_invalidation(&frame).unwrap();
        assert_eq!(keys, vec!["mykey"]);
    }

    #[test]
    fn parse_invalidation_multiple_keys() {
        let frame = Frame::Push(vec![
            Frame::BulkString(Some(Bytes::from("invalidate"))),
            Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from("key1"))),
                Frame::BulkString(Some(Bytes::from("key2"))),
            ])),
        ]);
        let keys = parse_invalidation(&frame).unwrap();
        assert_eq!(keys, vec!["key1", "key2"]);
    }

    #[test]
    fn parse_invalidation_as_array() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("invalidate"))),
            Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from("mykey")))])),
        ]));
        let keys = parse_invalidation(&frame).unwrap();
        assert_eq!(keys, vec!["mykey"]);
    }

    #[test]
    fn parse_invalidation_flush_all() {
        let frame = Frame::Push(vec![
            Frame::BulkString(Some(Bytes::from("invalidate"))),
            Frame::Null,
        ]);
        let keys = parse_invalidation(&frame).unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn parse_invalidation_not_invalidate() {
        let frame = Frame::Push(vec![
            Frame::BulkString(Some(Bytes::from("other"))),
            Frame::Null,
        ]);
        assert!(parse_invalidation(&frame).is_none());
    }

    #[test]
    fn cache_key_for_get() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("GET"))),
            Frame::BulkString(Some(Bytes::from("mykey"))),
        ]));
        assert_eq!(extract_cache_key(&frame), Some("GET:mykey".to_string()));
    }

    #[test]
    fn cache_key_for_set_is_none() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from("mykey"))),
            Frame::BulkString(Some(Bytes::from("value"))),
        ]));
        assert_eq!(extract_cache_key(&frame), None);
    }

    #[test]
    fn cache_key_for_hget() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("HGET"))),
            Frame::BulkString(Some(Bytes::from("hash"))),
            Frame::BulkString(Some(Bytes::from("field"))),
        ]));
        assert_eq!(extract_cache_key(&frame), Some("HGET:hash".to_string()));
    }
}
