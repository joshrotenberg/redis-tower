//! Sentinel integration tests using redis-test-harness.
//!
//! Run with: `cargo test -p redis-tower-sentinel --test sentinel_integration -- --ignored`

use std::sync::OnceLock;

use bytes::Bytes;
use redis_test_harness::sentinel::RedisSentinel;
use redis_tower_commands::*;
use redis_tower_sentinel::{SentinelClient, SentinelConnection};

static SENTINEL: OnceLock<RedisSentinel> = OnceLock::new();

fn ensure_sentinel() -> &'static RedisSentinel {
    SENTINEL.get_or_init(|| {
        use redis_test_harness::sentinel::SentinelConfig;
        // Use non-default ports to avoid conflicts with other services.
        let mut sentinel = RedisSentinel::new(SentinelConfig {
            master_port: 6390,
            replica_base_port: 6391,
            sentinel_base_port: 26389,
            ..Default::default()
        });
        let _ = sentinel.stop();
        std::thread::sleep(std::time::Duration::from_millis(500));
        sentinel.start().expect("failed to start sentinel topology");
        sentinel
            .wait_for_healthy(std::time::Duration::from_secs(15))
            .expect("sentinel topology not healthy");
        sentinel
    })
}

fn sentinel_addrs() -> Vec<String> {
    let s = ensure_sentinel();
    s.config()
        .sentinel_ports()
        .map(|p| format!("{}:{}", s.config().bind, p))
        .collect()
}

async fn sentinel_conn() -> SentinelConnection {
    let addrs = sentinel_addrs();
    SentinelConnection::connect(&addrs, "mymaster")
        .await
        .expect("failed to connect via sentinel")
}

fn key(test: &str, name: &str) -> String {
    format!("sentinel_test:{test}:{name}")
}

// Generate shared command tests for sentinel topology.
redis_test_harness::command_tests!(sentinel_conn, "sentinel_cmd", ignored);

// -- Sentinel-specific tests --

#[tokio::test]
#[ignore]
async fn sentinel_discovers_master() {
    let addrs = sentinel_addrs();
    let addr = redis_tower_sentinel::discovery::discover_master(&addrs, "mymaster")
        .await
        .unwrap();
    assert!(
        addr.contains("6390"),
        "expected master on port 6390, got {addr}"
    );
}

#[tokio::test]
#[ignore]
async fn sentinel_discovers_replicas() {
    let addrs = sentinel_addrs();
    let replicas = redis_tower_sentinel::discovery::discover_replicas(&addrs, "mymaster")
        .await
        .unwrap();
    // Default config: 2 replicas.
    assert_eq!(replicas.len(), 2, "expected 2 replicas, got {replicas:?}");
}

#[tokio::test]
#[ignore]
async fn sentinel_set_and_get() {
    let mut conn = sentinel_conn().await;
    let k = key("set_get", "k");
    conn.execute(Set::new(&k, "hello")).await.unwrap();
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("hello")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn sentinel_client_shared() {
    let addrs = sentinel_addrs();
    let client = SentinelClient::connect(&addrs, "mymaster").await.unwrap();
    let k = key("client_shared", "k");
    client.execute(Set::new(&k, "val")).await.unwrap();

    let c = client.clone();
    let k2 = k.clone();
    let handle = tokio::spawn(async move { c.execute(Get::new(&k2)).await.unwrap() });

    let val: Option<Bytes> = handle.await.unwrap();
    assert_eq!(val, Some(Bytes::from("val")));
    client.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn sentinel_rediscover() {
    let mut conn = sentinel_conn().await;
    // Force rediscovery -- should reconnect to the same master.
    conn.rediscover().await.unwrap();
    let pong = conn.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}
