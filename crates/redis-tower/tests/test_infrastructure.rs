//! Integration tests that require specific infrastructure.
//!
//! These tests are #[ignore] by default. Run them with:
//! - TLS: `cargo test --test test_infrastructure tls -- --ignored` (needs Redis with TLS)
//!
//! Cluster and sentinel integration tests live in the dedicated
//! `redis-tower-cluster` and `redis-tower-sentinel` crates.

use bytes::Bytes;
use redis_tower::RedisConnection;
use redis_tower::commands::*;

// ---------------------------------------------------------------------------
// TLS tests (#153)
//
// Requires: Redis server with TLS enabled, or REDIS_TLS_URL env var.
// Example: REDIS_TLS_URL=rediss://localhost:6380
//
// Build with a TLS feature: cargo test --test test_infrastructure \
//   --features tls-rustls tls -- --ignored
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Redis with TLS on port 6380 or REDIS_TLS_URL"]
async fn tls_connect_and_ping() {
    let url =
        std::env::var("REDIS_TLS_URL").unwrap_or_else(|_| "rediss://localhost:6380".to_string());
    let mut conn = RedisConnection::connect_url(&url)
        .await
        .expect("failed to connect with TLS");
    let pong = conn.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

#[tokio::test]
#[ignore = "requires Redis with TLS on port 6380 or REDIS_TLS_URL"]
async fn tls_set_get_roundtrip() {
    let url =
        std::env::var("REDIS_TLS_URL").unwrap_or_else(|_| "rediss://localhost:6380".to_string());
    let mut conn = RedisConnection::connect_url(&url).await.unwrap();
    conn.execute(Set::new("tls_test:key", "value"))
        .await
        .unwrap();
    let val = conn.execute(Get::new("tls_test:key")).await.unwrap();
    assert_eq!(val, Some(Bytes::from("value")));
    conn.execute(Del::new("tls_test:key")).await.unwrap();
}
