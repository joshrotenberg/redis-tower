//! Integration tests for Redis Cluster functionality
//!
//! **Prerequisites**: These tests require a running Redis Cluster.
//! Start the cluster using docker-compose:
//!
//! ```bash
//! docker-compose up -d
//! # Wait for cluster to initialize (check logs)
//! docker-compose logs cluster-init
//! ```
//!
//! The cluster consists of 6 nodes (3 masters + 3 replicas) on ports 7100-7105.
//!
//! **To run these tests**:
//! ```bash
//! cargo test --test integration_cluster --features cluster -- --test-threads=1
//! ```
//!
//! Note: Tests run sequentially (--test-threads=1) to avoid conflicts.

use redis_tower::cluster::ClusterClient;
use redis_tower::commands::{Del, Get, Ping, Set};

/// Setup cluster client pointing to any cluster node
/// The client will discover other nodes automatically
async fn setup_cluster() -> ClusterClient {
    // Connect to first cluster node - it will discover the others
    let seeds = vec!["localhost:7100".to_string()];

    ClusterClient::new(seeds)
        .await
        .expect("Failed to connect to cluster - is docker-compose running?")
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn test_cluster_ping() {
    let client = setup_cluster().await;

    // PING should work on cluster
    let pong: String = client.call(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn test_cluster_basic_operations() {
    let client = setup_cluster().await;
    let key = "cluster_test_key";

    // Clean up first
    let _: i64 = client.call(Del::new(vec![key.to_string()])).await.unwrap();

    // SET should route to correct node based on key hash
    let _: () = client.call(Set::new(key, "cluster_value")).await.unwrap();

    // GET should route to same node (or replica)
    let value: Option<bytes::Bytes> = client.call(Get::new(key)).await.unwrap();
    assert_eq!(value.unwrap().as_ref(), b"cluster_value");

    // Clean up
    let _: i64 = client.call(Del::new(vec![key.to_string()])).await.unwrap();
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn test_cluster_key_routing() {
    let client = setup_cluster().await;

    // Keys that should hash to different slots
    let key1 = "user:1000";
    let key2 = "user:2000";
    let key3 = "user:3000";

    // Clean up
    let _: i64 = client
        .call(Del::new(vec![
            key1.to_string(),
            key2.to_string(),
            key3.to_string(),
        ]))
        .await
        .unwrap();

    // Set values - each routes to appropriate node
    let _: () = client.call(Set::new(key1, "value1")).await.unwrap();
    let _: () = client.call(Set::new(key2, "value2")).await.unwrap();
    let _: () = client.call(Set::new(key3, "value3")).await.unwrap();

    // Get values - should route correctly
    let v1: Option<bytes::Bytes> = client.call(Get::new(key1)).await.unwrap();
    let v2: Option<bytes::Bytes> = client.call(Get::new(key2)).await.unwrap();
    let v3: Option<bytes::Bytes> = client.call(Get::new(key3)).await.unwrap();

    assert_eq!(v1.unwrap().as_ref(), b"value1");
    assert_eq!(v2.unwrap().as_ref(), b"value2");
    assert_eq!(v3.unwrap().as_ref(), b"value3");

    // Clean up
    let _: i64 = client
        .call(Del::new(vec![
            key1.to_string(),
            key2.to_string(),
            key3.to_string(),
        ]))
        .await
        .unwrap();
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn test_cluster_hash_tags() {
    let client = setup_cluster().await;

    // Keys with same hash tag {user:1} should go to same slot
    let key1 = "{user:1}:name";
    let key2 = "{user:1}:email";
    let key3 = "{user:1}:age";

    // Clean up
    let _: i64 = client
        .call(Del::new(vec![
            key1.to_string(),
            key2.to_string(),
            key3.to_string(),
        ]))
        .await
        .unwrap();

    // Set multiple related keys (same hash tag = same slot)
    let _: () = client.call(Set::new(key1, "Alice")).await.unwrap();
    let _: () = client
        .call(Set::new(key2, "alice@example.com"))
        .await
        .unwrap();
    let _: () = client.call(Set::new(key3, "30")).await.unwrap();

    // Verify values
    let name: Option<bytes::Bytes> = client.call(Get::new(key1)).await.unwrap();
    let email: Option<bytes::Bytes> = client.call(Get::new(key2)).await.unwrap();
    let age: Option<bytes::Bytes> = client.call(Get::new(key3)).await.unwrap();

    assert_eq!(name.unwrap().as_ref(), b"Alice");
    assert_eq!(email.unwrap().as_ref(), b"alice@example.com");
    assert_eq!(age.unwrap().as_ref(), b"30");

    // Clean up
    let _: i64 = client
        .call(Del::new(vec![
            key1.to_string(),
            key2.to_string(),
            key3.to_string(),
        ]))
        .await
        .unwrap();
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn test_cluster_moved_redirect() {
    let client = setup_cluster().await;

    // This test verifies that MOVED redirects are handled automatically
    // When we connect, the cluster topology might change, causing MOVED responses
    // The client should follow these redirects transparently

    let key = "test_moved_key";

    // Clean up
    let _: i64 = client.call(Del::new(vec![key.to_string()])).await.unwrap();

    // Even if we get MOVED, the client should handle it
    let _: () = client.call(Set::new(key, "value")).await.unwrap();
    let value: Option<bytes::Bytes> = client.call(Get::new(key)).await.unwrap();

    assert_eq!(value.unwrap().as_ref(), b"value");

    // Clean up
    let _: i64 = client.call(Del::new(vec![key.to_string()])).await.unwrap();
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn test_cluster_multiple_keys_same_slot() {
    let client = setup_cluster().await;

    // Using hash tags to ensure keys go to same slot
    // This allows multi-key operations in cluster mode
    let keys = vec![
        "{product:100}:name",
        "{product:100}:price",
        "{product:100}:stock",
    ];

    // Clean up
    let key_strings: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    let _: i64 = client.call(Del::new(key_strings.clone())).await.unwrap();

    // Set values
    let _: () = client.call(Set::new(keys[0], "Widget")).await.unwrap();
    let _: () = client.call(Set::new(keys[1], "29.99")).await.unwrap();
    let _: () = client.call(Set::new(keys[2], "150")).await.unwrap();

    // Verify all values
    for &key in &keys {
        let value: Option<bytes::Bytes> = client.call(Get::new(key)).await.unwrap();
        assert!(value.is_some(), "Key {} should exist", key);
    }

    // Clean up
    let _: i64 = client.call(Del::new(key_strings)).await.unwrap();
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn test_cluster_connection_to_different_seeds() {
    // Test that we can connect to any node in the cluster
    let seeds_configs = vec![
        vec!["localhost:7100".to_string()],
        vec!["localhost:7101".to_string()],
        vec!["localhost:7102".to_string()],
        // Multiple seeds
        vec!["localhost:7100".to_string(), "localhost:7101".to_string()],
    ];

    for seeds in seeds_configs {
        let client = ClusterClient::new(seeds.clone())
            .await
            .unwrap_or_else(|_| panic!("Failed to connect with seeds: {:?}", seeds));

        // Verify connection works
        let pong: String = client.call(Ping::new()).await.unwrap();
        assert_eq!(pong, "PONG");
    }
}

#[tokio::test]
#[cfg(feature = "cluster")]
async fn test_cluster_concurrent_operations() {
    use tokio::task::JoinSet;

    let client = setup_cluster().await;
    let mut tasks = JoinSet::new();

    // Spawn 10 concurrent operations with different keys
    for i in 0..10 {
        let client_clone = client.clone();
        tasks.spawn(async move {
            let key = format!("concurrent_key_{}", i);
            let value = format!("value_{}", i);

            // Clean up
            let _: i64 = client_clone
                .call(Del::new(vec![key.clone()]))
                .await
                .unwrap();

            // Set value
            let _: () = client_clone
                .call(Set::new(&key, value.clone()))
                .await
                .unwrap();

            // Get value
            let result: Option<bytes::Bytes> = client_clone.call(Get::new(&key)).await.unwrap();

            // Clean up
            let _: i64 = client_clone.call(Del::new(vec![key])).await.unwrap();

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
