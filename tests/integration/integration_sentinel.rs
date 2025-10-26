//! Integration tests for Redis Sentinel functionality
//!
//! **Prerequisites**: These tests require a running Redis Sentinel setup.
//! Start the sentinel cluster using docker-compose:
//!
//! ```bash
//! docker-compose up -d redis-sentinel-master redis-sentinel-replica-1 redis-sentinel-replica-2 sentinel-1 sentinel-2 sentinel-3
//! # Wait for sentinel to initialize
//! docker-compose logs sentinel-1
//! ```
//!
//! The setup consists of:
//! - 1 master on port 6380
//! - 2 replicas on ports 6381-6382
//! - 3 sentinels on ports 26379-26381
//! - Master name: "mymaster"
//!
//! **To run these tests**:
//! ```bash
//! cargo test --test integration_sentinel --features sentinel -- --test-threads=1
//! ```
//!
//! Note: Tests run sequentially (--test-threads=1) to avoid conflicts.

use redis_tower::commands::{Del, Get, Ping, Set};
use redis_tower::sentinel::{SentinelClient, SentinelConfig};
use tower::ServiceExt;

/// Setup sentinel client
/// Connects to sentinels to discover master
fn setup_sentinel() -> SentinelClient {
    let config = SentinelConfig::builder()
        .sentinel_node("localhost", 26379)
        .sentinel_node("localhost", 26380)
        .sentinel_node("localhost", 26381)
        .master_name("mymaster")
        .build()
        .expect("Failed to build sentinel config");

    SentinelClient::new(config)
}

#[tokio::test]
#[cfg(feature = "sentinel")]
async fn test_sentinel_ping() {
    let client = setup_sentinel();
    let master = client.master();

    // PING should work via sentinel-discovered master
    // Use oneshot() which handles readiness automatically
    let pong: String = master.oneshot(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

#[tokio::test]
#[cfg(feature = "sentinel")]
async fn test_sentinel_basic_operations() {
    let client = setup_sentinel();
    let key = "sentinel_test_key";

    // Clean up first
    let _: i64 = client
        .master()
        .oneshot(Del::new(vec![key.to_string()]))
        .await
        .unwrap();

    // SET should go to master
    let _: () = client
        .master()
        .oneshot(Set::new(key, "sentinel_value"))
        .await
        .unwrap();

    // GET should work (from master or replica)
    let value: Option<bytes::Bytes> = client.master().oneshot(Get::new(key)).await.unwrap();
    assert_eq!(value.unwrap().as_ref(), b"sentinel_value");

    // Clean up
    let _: i64 = client
        .master()
        .oneshot(Del::new(vec![key.to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
#[cfg(feature = "sentinel")]
async fn test_sentinel_master_discovery() {
    // Test that client can discover master through any sentinel
    let config = SentinelConfig::builder()
        .sentinel_node("localhost", 26379)
        .master_name("mymaster")
        .build()
        .unwrap();

    let client = SentinelClient::new(config);
    // Get fresh master for each operation

    // Should be able to communicate with discovered master
    let pong: String = client.master().oneshot(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

#[tokio::test]
#[cfg(feature = "sentinel")]
async fn test_sentinel_multiple_operations() {
    let client = setup_sentinel();
    // Get fresh master for each operation

    let keys = vec!["sentinel_key1", "sentinel_key2", "sentinel_key3"];

    // Clean up
    let key_strings: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    let _: i64 = client
        .master()
        .oneshot(Del::new(key_strings.clone()))
        .await
        .unwrap();

    // Set multiple values
    for (i, &key) in keys.iter().enumerate() {
        let value = format!("value{}", i);
        let _: () = client
            .master()
            .oneshot(Set::new(key, value.clone()))
            .await
            .unwrap();
    }

    // Get and verify values
    for (i, &key) in keys.iter().enumerate() {
        let value: Option<bytes::Bytes> = client.master().oneshot(Get::new(key)).await.unwrap();
        assert_eq!(value.unwrap().as_ref(), format!("value{}", i).as_bytes());
    }

    // Clean up
    let _: i64 = client
        .master()
        .oneshot(Del::new(key_strings))
        .await
        .unwrap();
}

#[tokio::test]
#[cfg(feature = "sentinel")]
async fn test_sentinel_connection_to_different_sentinels() {
    // Test connecting through different sentinel nodes
    let sentinel_configs = vec![
        vec![("localhost", 26379)],
        vec![("localhost", 26380)],
        vec![("localhost", 26381)],
        // All three
        vec![
            ("localhost", 26379),
            ("localhost", 26380),
            ("localhost", 26381),
        ],
    ];

    for sentinels in sentinel_configs {
        let mut builder = SentinelConfig::builder();
        for &(host, port) in sentinels.iter() {
            builder = builder.sentinel_node(host, port);
        }
        let config = builder.master_name("mymaster").build().unwrap();

        let client = SentinelClient::new(config);
        // Get fresh master for each operation

        // Verify connection works
        let pong: String = client.master().oneshot(Ping::new()).await.unwrap();
        assert_eq!(pong, "PONG");
    }
}

#[tokio::test]
#[cfg(feature = "sentinel")]
async fn test_sentinel_concurrent_operations() {
    use tokio::task::JoinSet;

    let client = setup_sentinel();
    let mut tasks = JoinSet::new();

    // Spawn 10 concurrent operations
    for i in 0..10 {
        let client_clone = client.clone();
        tasks.spawn(async move {
            let key = format!("sentinel_concurrent_key_{}", i);
            let value = format!("value_{}", i);

            // Clean up
            let _: i64 = client_clone
                .master()
                .oneshot(Del::new(vec![key.clone()]))
                .await
                .unwrap();

            // Set value
            let _: () = client_clone
                .master()
                .oneshot(Set::new(&key, value.clone()))
                .await
                .unwrap();

            // Get value
            let result: Option<bytes::Bytes> =
                client_clone.master().oneshot(Get::new(&key)).await.unwrap();

            // Clean up
            let _: i64 = client_clone
                .master()
                .oneshot(Del::new(vec![key]))
                .await
                .unwrap();

            result.unwrap()
        });
    }

    // Wait for all tasks and verify results
    let mut count = 0;
    while let Some(result) = tasks.join_next().await {
        let value = result.unwrap();
        assert!(value.starts_with(b"value_"));
        count += 1;
    }

    assert_eq!(count, 10);
}

#[tokio::test]
#[cfg(feature = "sentinel")]
async fn test_sentinel_failover_awareness() {
    // This test verifies that the sentinel client is aware of the master
    // In a real failover scenario, sentinels would promote a replica to master
    // and the client should discover the new master

    let client = setup_sentinel();
    // Get fresh master for each operation

    // Normal operation
    let key = "failover_test_key";
    let _: i64 = client
        .master()
        .oneshot(Del::new(vec![key.to_string()]))
        .await
        .unwrap();
    let _: () = client
        .master()
        .oneshot(Set::new(key, "before"))
        .await
        .unwrap();

    let value: Option<bytes::Bytes> = client.master().oneshot(Get::new(key)).await.unwrap();
    assert_eq!(value.unwrap().as_ref(), b"before");

    // Note: Actually triggering a failover would require:
    // 1. Stopping the master node
    // 2. Waiting for sentinels to detect failure (5 seconds)
    // 3. Waiting for sentinels to promote replica
    // 4. Client should automatically reconnect to new master
    //
    // This is tested manually or in chaos engineering scenarios

    // Clean up
    let _: i64 = client
        .master()
        .oneshot(Del::new(vec![key.to_string()]))
        .await
        .unwrap();
}
