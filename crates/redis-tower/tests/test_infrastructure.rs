//! Integration tests that require specific infrastructure.
//!
//! These tests are #[ignore] by default. Run them with:
//! - TLS: `cargo test --test test_infrastructure tls -- --ignored` (needs Redis with TLS)
//! - Cluster: `cargo test --test test_infrastructure cluster -- --ignored` (needs 3-node cluster)
//! - Sentinel: `cargo test --test test_infrastructure sentinel -- --ignored` (needs sentinel setup)

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

// ---------------------------------------------------------------------------
// Cluster tests (#157)
//
// Requires: 3-node Redis cluster on ports 7000-7005, or REDIS_CLUSTER_ADDR
// env var.
//
// These are stubs for the future ClusterConnection type. Once
// redis-tower-cluster exists, replace the todo!() calls with real logic.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires 3-node Redis cluster or REDIS_CLUSTER_ADDR"]
async fn cluster_connect_and_ping() {
    let _addr =
        std::env::var("REDIS_CLUSTER_ADDR").unwrap_or_else(|_| "127.0.0.1:7000".to_string());
    // TODO(#157): use ClusterConnection::connect(&addr) once the crate exists
    todo!("cluster support not yet implemented");
}

#[tokio::test]
#[ignore = "requires 3-node Redis cluster or REDIS_CLUSTER_ADDR"]
async fn cluster_cross_slot_routing() {
    let _addr =
        std::env::var("REDIS_CLUSTER_ADDR").unwrap_or_else(|_| "127.0.0.1:7000".to_string());
    // TODO(#157): verify commands to keys in different slots are routed correctly
    // cluster.execute(Set::new("cluster_test:a", "1")).await.unwrap();
    // cluster.execute(Set::new("cluster_test:b", "2")).await.unwrap();
    todo!("cluster support not yet implemented");
}

#[tokio::test]
#[ignore = "requires 3-node Redis cluster or REDIS_CLUSTER_ADDR"]
async fn cluster_hash_tag_same_slot() {
    let _addr =
        std::env::var("REDIS_CLUSTER_ADDR").unwrap_or_else(|_| "127.0.0.1:7000".to_string());
    // TODO(#157): verify hash tags {tag}.k1 and {tag}.k2 land on the same slot
    todo!("cluster support not yet implemented");
}

#[tokio::test]
#[ignore = "requires 3-node Redis cluster or REDIS_CLUSTER_ADDR"]
async fn cluster_topology_info() {
    let _addr =
        std::env::var("REDIS_CLUSTER_ADDR").unwrap_or_else(|_| "127.0.0.1:7000".to_string());
    // TODO(#157): verify we can read cluster topology / slot ranges
    todo!("cluster support not yet implemented");
}

// ---------------------------------------------------------------------------
// Sentinel tests (#158)
//
// Requires: Sentinel setup with sentinels on ports 26379-26381,
// or REDIS_SENTINEL_ADDRS and REDIS_SENTINEL_MASTER env vars.
//
// These are stubs for the future SentinelConnection type. Once
// redis-tower-sentinel exists, replace the todo!() calls with real logic.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Redis Sentinel setup or REDIS_SENTINEL_ADDRS"]
async fn sentinel_discover_and_ping() {
    let _addrs_str = std::env::var("REDIS_SENTINEL_ADDRS")
        .unwrap_or_else(|_| "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381".to_string());
    let _master_name =
        std::env::var("REDIS_SENTINEL_MASTER").unwrap_or_else(|_| "mymaster".to_string());
    // TODO(#158): use SentinelConnection::connect(&addrs, &master_name)
    todo!("sentinel support not yet implemented");
}

#[tokio::test]
#[ignore = "requires Redis Sentinel setup or REDIS_SENTINEL_ADDRS"]
async fn sentinel_set_get() {
    let _addrs_str = std::env::var("REDIS_SENTINEL_ADDRS")
        .unwrap_or_else(|_| "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381".to_string());
    let _master_name =
        std::env::var("REDIS_SENTINEL_MASTER").unwrap_or_else(|_| "mymaster".to_string());
    // TODO(#158): connect via sentinel, set/get roundtrip
    todo!("sentinel support not yet implemented");
}

#[tokio::test]
#[ignore = "requires Redis Sentinel setup or REDIS_SENTINEL_ADDRS"]
async fn sentinel_discover_replicas() {
    let _addrs_str = std::env::var("REDIS_SENTINEL_ADDRS")
        .unwrap_or_else(|_| "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381".to_string());
    let _master_name =
        std::env::var("REDIS_SENTINEL_MASTER").unwrap_or_else(|_| "mymaster".to_string());
    // TODO(#158): discover replicas and verify the call succeeds
    todo!("sentinel support not yet implemented");
}
