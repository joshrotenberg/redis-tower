//! Common utilities for integration tests.
//!
//! These tests require a running Redis instance on localhost:6379.
//! Run with: docker-compose up -d redis

use redis_tower::client::RedisConnection;
use redis_tower::types::RedisError;

/// Default Redis address for tests
pub const REDIS_ADDR: &str = "127.0.0.1:6379";

/// Connect to the test Redis instance
pub async fn connect() -> Result<RedisConnection, RedisError> {
    RedisConnection::connect(REDIS_ADDR).await
}

/// Generate a unique test key to avoid collisions between tests
#[allow(dead_code)]
pub fn test_key(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let count = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("test:{}:{}", prefix, count)
}

/// Helper macro to skip test if Redis is not available
#[macro_export]
macro_rules! skip_if_no_redis {
    ($client:expr) => {
        if $client.is_err() {
            eprintln!("Skipping test: Redis not available at {}", REDIS_ADDR);
            return;
        }
    };
}
