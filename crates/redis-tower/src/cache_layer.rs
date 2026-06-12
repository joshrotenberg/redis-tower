//! Tower cache layer for client-side caching at the Frame level.
//!
//! Wraps a `Service<Frame, Response=Frame>` and caches responses for
//! cacheable read commands. Receives invalidation messages via a channel.
//!
//! Cache keying and invalidation are shared with
//! [`CachedClient`](crate::caching::CachedClient) so the two never diverge.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use redis_tower_core::{Frame, RedisError};
use tokio::sync::RwLock;
use tower_service::Service;

use crate::cache_state::{CacheState, extract_cache_entry, parse_invalidation};

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
/// TYPE, and TTL. Write commands bypass the cache entirely. Entries are keyed
/// by the full command argument vector, so distinct arguments never collide.
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
    cache: Arc<RwLock<CacheState>>,
    config: CacheConfig,
}

impl<S> CacheService<S> {
    /// Create a new cache service wrapping an inner Frame service.
    pub fn new(inner: S, config: CacheConfig) -> Self {
        Self {
            inner,
            cache: Arc::new(RwLock::new(CacheState::default())),
            config,
        }
    }

    /// Create with an existing shared cache (for invalidation integration).
    pub fn with_cache(inner: S, cache: Arc<RwLock<CacheState>>, config: CacheConfig) -> Self {
        Self {
            inner,
            cache,
            config,
        }
    }

    /// Get a reference to the shared cache for invalidation wiring.
    pub fn cache(&self) -> &Arc<RwLock<CacheState>> {
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
        let entry = extract_cache_entry(&request);

        if let Some((ref cache_key, _)) = entry {
            // Non-blocking cache check; we can't await inside call().
            if let Ok(guard) = self.cache.try_read()
                && let Some(cached) = guard.get(cache_key)
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

            if let Some((cache_key, redis_key)) = entry
                && !matches!(response, Frame::Error(_))
            {
                let mut guard = cache.write().await;
                guard.insert(cache_key, redis_key, response.clone(), max_size);
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
    cache: Arc<RwLock<CacheState>>,
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
                        c.invalidate(key);
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
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
