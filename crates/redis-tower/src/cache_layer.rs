//! Tower cache layer for client-side caching at the Frame level.
//!
//! Wraps a `Service<Frame, Response=Frame>` and caches responses for
//! cacheable read commands. Receives invalidation messages via a channel.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use redis_tower_core::{Frame, RedisError};
use tokio::sync::Mutex;
use tower_service::Service;

/// Configuration for the cache layer.
#[derive(Default)]
pub struct CacheConfig {
    /// Maximum number of cached entries. 0 means unlimited.
    pub max_size: usize,
}

/// Tower `Service` that caches Frame responses for cacheable commands.
///
/// Sits between `CommandAdapter` and `FrameService` in the service stack:
///
/// ```text
/// CommandAdapter<CacheService<FrameService>>
///       Cmd -> Frame -> (cache check) -> Frame -> Cmd::Response
/// ```
pub struct CacheService<S> {
    inner: S,
    cache: Arc<Mutex<HashMap<String, Frame>>>,
    config: CacheConfig,
}

impl<S> CacheService<S> {
    /// Create a new cache service wrapping an inner Frame service.
    pub fn new(inner: S, config: CacheConfig) -> Self {
        Self {
            inner,
            cache: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Create with an existing shared cache (for invalidation integration).
    pub fn with_cache(
        inner: S,
        cache: Arc<Mutex<HashMap<String, Frame>>>,
        config: CacheConfig,
    ) -> Self {
        Self {
            inner,
            cache,
            config,
        }
    }

    /// Get a reference to the shared cache for invalidation wiring.
    pub fn cache(&self) -> &Arc<Mutex<HashMap<String, Frame>>> {
        &self.cache
    }

    /// Get the number of cached entries.
    pub async fn cache_size(&self) -> usize {
        self.cache.lock().await.len()
    }
}

impl<S> Service<Frame> for CacheService<S>
where
    S: Service<Frame, Response = Frame, Error = RedisError>,
    S::Future: Send + 'static,
{
    type Response = Frame;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Frame) -> Self::Future {
        let cache_key = extract_cache_key(&request);

        if let Some(ref key) = cache_key {
            let cache = Arc::clone(&self.cache);
            let key_clone = key.clone();

            // Try cache lookup.
            // We can't await inside call(), so we need to check synchronously
            // or defer to the future. Use try_lock for non-blocking check.
            if let Ok(guard) = cache.try_lock() {
                if let Some(cached) = guard.get(&key_clone) {
                    let result = cached.clone();
                    return Box::pin(async move { Ok(result) });
                }
            }
        }

        // Cache miss or non-cacheable -- call inner service.
        let future = self.inner.call(request);
        let cache = Arc::clone(&self.cache);
        let max_size = self.config.max_size;

        Box::pin(async move {
            let response = future.await?;

            // Cache the response if this was a cacheable command.
            if let Some(key) = cache_key {
                if !matches!(response, Frame::Error(_)) {
                    let mut guard = cache.lock().await;
                    // Evict oldest if at capacity (simple eviction -- not LRU).
                    if max_size > 0 && guard.len() >= max_size {
                        if let Some(first_key) = guard.keys().next().cloned() {
                            guard.remove(&first_key);
                        }
                    }
                    guard.insert(key, response.clone());
                }
            }

            Ok(response)
        })
    }
}

/// Spawn a background task that processes invalidation push messages
/// and removes entries from the cache.
///
/// Returns the `JoinHandle` for the task. The task runs until the
/// receiver is closed (tracking connection dropped).
pub fn spawn_invalidation_task(
    cache: Arc<Mutex<HashMap<String, Frame>>>,
    mut push_rx: impl futures::Stream<Item = Result<Frame, redis_tower_protocol::ProtocolError>>
    + Unpin
    + Send
    + 'static,
) -> tokio::task::JoinHandle<()> {
    use futures::StreamExt;
    tokio::spawn(async move {
        while let Some(Ok(frame)) = push_rx.next().await {
            if let Some(keys) = parse_invalidation(&frame) {
                let mut c = cache.lock().await;
                if keys.is_empty() {
                    c.clear();
                } else {
                    for key in &keys {
                        c.retain(|cache_key, _| !cache_key.ends_with(&format!(":{key}")));
                    }
                }
            }
        }
    })
}

/// Extract a cache key from a command frame.
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

/// Parse an invalidation push message.
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn cache_key_get() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("GET"))),
            Frame::BulkString(Some(Bytes::from("mykey"))),
        ]));
        assert_eq!(extract_cache_key(&frame), Some("GET:mykey".to_string()));
    }

    #[test]
    fn cache_key_set_not_cached() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from("mykey"))),
            Frame::BulkString(Some(Bytes::from("val"))),
        ]));
        assert_eq!(extract_cache_key(&frame), None);
    }

    #[test]
    fn invalidation_single() {
        let frame = Frame::Push(vec![
            Frame::BulkString(Some(Bytes::from("invalidate"))),
            Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from("k")))])),
        ]);
        assert_eq!(parse_invalidation(&frame), Some(vec!["k".to_string()]));
    }

    #[test]
    fn invalidation_flush() {
        let frame = Frame::Push(vec![
            Frame::BulkString(Some(Bytes::from("invalidate"))),
            Frame::Null,
        ]);
        assert_eq!(parse_invalidation(&frame), Some(vec![]));
    }
}
