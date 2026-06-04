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
use tokio::sync::RwLock;
use tower_service::Service;

/// Configuration for the [`CacheService`] layer.
///
/// # Defaults
///
/// - `max_size`: 0 (unlimited)
#[derive(Default)]
pub struct CacheConfig {
    /// Maximum number of cached entries. 0 means unlimited.
    pub max_size: usize,
}

/// Tower `Service` that caches Frame responses for cacheable read commands.
///
/// Caches responses for GET, HGET, HGETALL, LRANGE, SMEMBERS, ZRANGE,
/// TYPE, and TTL. Write commands bypass the cache entirely.
///
/// Sits between [`CommandAdapter`](crate::CommandAdapter) and
/// [`FrameService`](crate::FrameService) in the service stack:
///
/// ```text
/// CommandAdapter<CacheService<FrameService>>
///       Cmd -> Frame -> (cache check) -> Frame -> Cmd::Response
/// ```
///
/// For automatic invalidation via Redis server-assisted client caching,
/// use [`spawn_invalidation_task`] with a push message stream.
pub struct CacheService<S> {
    inner: S,
    cache: Arc<RwLock<HashMap<String, Frame>>>,
    config: CacheConfig,
}

impl<S> CacheService<S> {
    /// Create a new cache service wrapping an inner Frame service.
    pub fn new(inner: S, config: CacheConfig) -> Self {
        Self {
            inner,
            cache: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Create with an existing shared cache (for invalidation integration).
    pub fn with_cache(
        inner: S,
        cache: Arc<RwLock<HashMap<String, Frame>>>,
        config: CacheConfig,
    ) -> Self {
        Self {
            inner,
            cache,
            config,
        }
    }

    /// Get a reference to the shared cache for invalidation wiring.
    pub fn cache(&self) -> &Arc<RwLock<HashMap<String, Frame>>> {
        &self.cache
    }

    /// Get the number of cached entries.
    pub async fn cache_size(&self) -> usize {
        self.cache.read().await.len()
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
            if let Ok(guard) = cache.try_read()
                && let Some(cached) = guard.get(&key_clone)
            {
                let result = cached.clone();
                return Box::pin(async move { Ok(result) });
            }
        }

        // Cache miss or non-cacheable -- call inner service.
        let future = self.inner.call(request);
        let cache = Arc::clone(&self.cache);
        let max_size = self.config.max_size;

        Box::pin(async move {
            let response = future.await?;

            // Cache the response if this was a cacheable command.
            if let Some(key) = cache_key
                && !matches!(response, Frame::Error(_))
            {
                let mut guard = cache.write().await;
                // Evict oldest if at capacity (simple eviction -- not LRU).
                if max_size > 0
                    && guard.len() >= max_size
                    && let Some(first_key) = guard.keys().next().cloned()
                {
                    guard.remove(&first_key);
                }
                guard.insert(key, response.clone());
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
    cache: Arc<RwLock<HashMap<String, Frame>>>,
    mut push_rx: impl futures::Stream<Item = Result<Frame, redis_tower_protocol::ProtocolError>>
    + Unpin
    + Send
    + 'static,
) -> tokio::task::JoinHandle<()> {
    use futures::StreamExt;
    tokio::spawn(async move {
        while let Some(Ok(frame)) = push_rx.next().await {
            if let Some(keys) = parse_invalidation(&frame) {
                let mut c = cache.write().await;
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

    // -- behavioral tests with a mock inner Frame service --

    use std::sync::Mutex;

    /// A `Service<Frame>` that counts how many times `call` is invoked and
    /// returns a fixed response. Used to assert cache hit/miss behavior.
    struct CountingService {
        call_count: Arc<Mutex<usize>>,
        response: Frame,
    }

    impl Service<Frame> for CountingService {
        type Response = Frame;
        type Error = RedisError;
        type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Frame) -> Self::Future {
            let count = Arc::clone(&self.call_count);
            let resp = self.response.clone();
            Box::pin(async move {
                *count.lock().unwrap() += 1;
                Ok(resp)
            })
        }
    }

    fn make_get_frame(key: &str) -> Frame {
        Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("GET"))),
            Frame::BulkString(Some(Bytes::from(key.to_string()))),
        ]))
    }

    fn make_set_frame(key: &str, val: &str) -> Frame {
        Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from(key.to_string()))),
            Frame::BulkString(Some(Bytes::from(val.to_string()))),
        ]))
    }

    #[tokio::test]
    async fn cache_miss_calls_inner_service() {
        let call_count = Arc::new(Mutex::new(0usize));
        let svc = CountingService {
            call_count: Arc::clone(&call_count),
            response: Frame::BulkString(Some(Bytes::from("world"))),
        };
        let mut cache_svc = CacheService::new(svc, CacheConfig::default());

        let req = make_get_frame("hello");
        let resp = cache_svc.call(req).await.unwrap();
        assert!(matches!(resp, Frame::BulkString(_)));
        assert_eq!(*call_count.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn cache_hit_skips_inner_service() {
        let call_count = Arc::new(Mutex::new(0usize));
        let svc = CountingService {
            call_count: Arc::clone(&call_count),
            response: Frame::BulkString(Some(Bytes::from("world"))),
        };
        let mut cache_svc = CacheService::new(svc, CacheConfig::default());

        let req1 = make_get_frame("hello");
        cache_svc.call(req1).await.unwrap();

        // Second GET of the same key should be served from the cache.
        let req2 = make_get_frame("hello");
        let resp2 = cache_svc.call(req2).await.unwrap();
        assert!(matches!(resp2, Frame::BulkString(_)));
        assert_eq!(
            *call_count.lock().unwrap(),
            1,
            "inner service should not be called on cache hit"
        );
    }

    #[tokio::test]
    async fn set_is_not_cached() {
        let call_count = Arc::new(Mutex::new(0usize));
        let svc = CountingService {
            call_count: Arc::clone(&call_count),
            response: Frame::SimpleString(Bytes::from("OK")),
        };
        let mut cache_svc = CacheService::new(svc, CacheConfig::default());

        let req = make_set_frame("hello", "world");
        cache_svc.call(req.clone()).await.unwrap();
        cache_svc.call(req).await.unwrap();
        // SET is not cacheable, so the inner service is called every time.
        assert_eq!(*call_count.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn cache_max_size_evicts_oldest() {
        let call_count = Arc::new(Mutex::new(0usize));
        let svc = CountingService {
            call_count: Arc::clone(&call_count),
            response: Frame::BulkString(Some(Bytes::from("v"))),
        };
        let mut cache_svc = CacheService::new(svc, CacheConfig { max_size: 1 });

        // Fill the cache with key "a".
        cache_svc.call(make_get_frame("a")).await.unwrap();
        assert_eq!(cache_svc.cache_size().await, 1);

        // Add key "b" -- the single-entry cache evicts "a".
        cache_svc.call(make_get_frame("b")).await.unwrap();
        assert_eq!(cache_svc.cache_size().await, 1);

        // One miss per distinct key.
        assert_eq!(*call_count.lock().unwrap(), 2);
    }
}
