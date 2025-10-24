//! Integration tests for Redis Cluster support.
//!
//! These tests require a running Redis Cluster.
//! Start with: docker-compose up -d
//! Run with: cargo test --test test_cluster

use redis_tower::cluster::{ClusterClient, slot_for_key};
use redis_tower::commands::{Del, Get, Incr, Set};

/// Cluster seed nodes
const CLUSTER_NODES: &[&str] = &["127.0.0.1:7100", "127.0.0.1:7101", "127.0.0.1:7102"];

/// Helper to create a unique test key
fn test_key(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let count = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("test:cluster:{}:{}", prefix, count)
}

#[tokio::test]
async fn test_cluster_basic_operations() {
    let client = ClusterClient::new(CLUSTER_NODES.to_vec())
        .await
        .expect("Failed to connect to cluster");

    let key = test_key("basic");

    // SET
    client
        .execute(Set::new(&key, b"cluster_value".to_vec()))
        .await
        .expect("Failed to SET");

    // GET
    let value = client.execute(Get::new(&key)).await.expect("Failed to GET");

    assert_eq!(value, Some(bytes::Bytes::from("cluster_value")));

    // DEL
    let deleted = client
        .execute(Del::new(vec![key.clone()]))
        .await
        .expect("Failed to DEL");

    assert_eq!(deleted, 1);

    // Verify deleted
    let value = client
        .execute(Get::new(&key))
        .await
        .expect("Failed to GET after DEL");

    assert_eq!(value, None);
}

#[tokio::test]
async fn test_cluster_multiple_keys() {
    let client = ClusterClient::new(CLUSTER_NODES.to_vec())
        .await
        .expect("Failed to connect to cluster");

    // Create keys that will hash to different slots
    let key1 = test_key("multi1");
    let key2 = test_key("multi2");
    let key3 = test_key("multi3");

    // Verify they're on different slots (likely but not guaranteed)
    let slot1 = slot_for_key(key1.as_bytes());
    let slot2 = slot_for_key(key2.as_bytes());
    let slot3 = slot_for_key(key3.as_bytes());

    println!(
        "Key slots: {} -> {}, {} -> {}, {} -> {}",
        key1, slot1, key2, slot2, key3, slot3
    );

    // Set values on different nodes
    client
        .execute(Set::new(&key1, b"value1".to_vec()))
        .await
        .expect("Failed to SET key1");

    client
        .execute(Set::new(&key2, b"value2".to_vec()))
        .await
        .expect("Failed to SET key2");

    client
        .execute(Set::new(&key3, b"value3".to_vec()))
        .await
        .expect("Failed to SET key3");

    // Get all values
    let val1 = client
        .execute(Get::new(&key1))
        .await
        .expect("Failed to GET key1");
    let val2 = client
        .execute(Get::new(&key2))
        .await
        .expect("Failed to GET key2");
    let val3 = client
        .execute(Get::new(&key3))
        .await
        .expect("Failed to GET key3");

    assert_eq!(val1, Some(bytes::Bytes::from("value1")));
    assert_eq!(val2, Some(bytes::Bytes::from("value2")));
    assert_eq!(val3, Some(bytes::Bytes::from("value3")));

    // Cleanup
    client.execute(Del::new(vec![key1, key2, key3])).await.ok();
}

#[tokio::test]
async fn test_cluster_hash_tags() {
    let client = ClusterClient::new(CLUSTER_NODES.to_vec())
        .await
        .expect("Failed to connect to cluster");

    // Keys with same hash tag will be on the same slot
    let key1 = test_key("tag:{user123}:name");
    let key2 = test_key("tag:{user123}:age");
    let key3 = test_key("tag:{user123}:email");

    // Verify they're on the same slot
    let slot1 = slot_for_key(key1.as_bytes());
    let slot2 = slot_for_key(key2.as_bytes());
    let slot3 = slot_for_key(key3.as_bytes());

    assert_eq!(slot1, slot2);
    assert_eq!(slot2, slot3);

    println!("Hash tag slots: all keys -> slot {}", slot1);

    // Set multiple values with same hash tag
    client
        .execute(Set::new(&key1, b"Alice".to_vec()))
        .await
        .expect("Failed to SET key1");

    client
        .execute(Set::new(&key2, b"30".to_vec()))
        .await
        .expect("Failed to SET key2");

    client
        .execute(Set::new(&key3, b"alice@example.com".to_vec()))
        .await
        .expect("Failed to SET key3");

    // Verify all values
    let name = client.execute(Get::new(&key1)).await.unwrap();
    let age = client.execute(Get::new(&key2)).await.unwrap();
    let email = client.execute(Get::new(&key3)).await.unwrap();

    assert_eq!(name, Some(bytes::Bytes::from("Alice")));
    assert_eq!(age, Some(bytes::Bytes::from("30")));
    assert_eq!(email, Some(bytes::Bytes::from("alice@example.com")));

    // Cleanup
    client.execute(Del::new(vec![key1, key2, key3])).await.ok();
}

#[tokio::test]
async fn test_cluster_atomic_operations() {
    let client = ClusterClient::new(CLUSTER_NODES.to_vec())
        .await
        .expect("Failed to connect to cluster");

    let key = test_key("counter");

    // Multiple INCR operations
    let val1 = client
        .execute(Incr::new(&key))
        .await
        .expect("Failed to INCR");
    assert_eq!(val1, 1);

    let val2 = client
        .execute(Incr::new(&key))
        .await
        .expect("Failed to INCR");
    assert_eq!(val2, 2);

    let val3 = client
        .execute(Incr::new(&key))
        .await
        .expect("Failed to INCR");
    assert_eq!(val3, 3);

    // Verify final value
    let final_val = client.execute(Get::new(&key)).await.expect("Failed to GET");

    assert_eq!(final_val, Some(bytes::Bytes::from("3")));

    // Cleanup
    client.execute(Del::new(vec![key])).await.ok();
}

#[tokio::test]
async fn test_cluster_slot_distribution() {
    let client = ClusterClient::new(CLUSTER_NODES.to_vec())
        .await
        .expect("Failed to connect to cluster");

    // Create 100 keys and track which slots they go to
    let mut slot_counts: std::collections::HashMap<u16, usize> = std::collections::HashMap::new();
    let mut keys_to_cleanup = Vec::new();

    for i in 0..100 {
        let key = test_key(&format!("dist{}", i));
        let slot = slot_for_key(key.as_bytes());
        *slot_counts.entry(slot).or_insert(0) += 1;

        // Set a value
        client
            .execute(Set::new(&key, format!("value{}", i)))
            .await
            .expect("Failed to SET");

        keys_to_cleanup.push(key);
    }

    println!("Slot distribution: {} unique slots used", slot_counts.len());
    println!(
        "Keys per slot: min={}, max={}, avg={:.1}",
        slot_counts.values().min().unwrap_or(&0),
        slot_counts.values().max().unwrap_or(&0),
        100.0 / slot_counts.len() as f64
    );

    // Verify we're using multiple slots (good distribution)
    assert!(
        slot_counts.len() > 10,
        "Expected keys distributed across multiple slots"
    );

    // Verify we can read back all values
    for (i, key) in keys_to_cleanup.iter().enumerate() {
        let value = client.execute(Get::new(key)).await.expect("Failed to GET");

        assert_eq!(value, Some(bytes::Bytes::from(format!("value{}", i))));
    }

    // Cleanup
    client.execute(Del::new(keys_to_cleanup)).await.ok();
}

#[tokio::test]
async fn test_cluster_concurrent_operations() {
    let client = ClusterClient::new(CLUSTER_NODES.to_vec())
        .await
        .expect("Failed to connect to cluster");

    let key = test_key("concurrent");

    // Perform concurrent operations
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let client = client.clone();
            let key = key.clone();
            tokio::spawn(async move {
                client
                    .execute(Set::new(format!("{}:{}", key, i), format!("value{}", i)))
                    .await
            })
        })
        .collect();

    // Wait for all to complete
    for handle in handles {
        handle.await.expect("Task panicked").expect("SET failed");
    }

    // Verify all values
    for i in 0..10 {
        let value = client
            .execute(Get::new(format!("{}:{}", key, i)))
            .await
            .expect("Failed to GET");

        assert_eq!(value, Some(bytes::Bytes::from(format!("value{}", i))));
    }

    // Cleanup
    let cleanup_keys: Vec<String> = (0..10).map(|i| format!("{}:{}", key, i)).collect();
    client.execute(Del::new(cleanup_keys)).await.ok();
}

#[tokio::test]
async fn test_cluster_nonexistent_key() {
    let client = ClusterClient::new(CLUSTER_NODES.to_vec())
        .await
        .expect("Failed to connect to cluster");

    let key = test_key("nonexistent");

    // Try to get a key that doesn't exist
    let value = client
        .execute(Get::new(&key))
        .await
        .expect("Failed to GET nonexistent key");

    assert_eq!(value, None);
}

#[tokio::test]
async fn test_cluster_overwrite() {
    let client = ClusterClient::new(CLUSTER_NODES.to_vec())
        .await
        .expect("Failed to connect to cluster");

    let key = test_key("overwrite");

    // Set initial value
    client
        .execute(Set::new(&key, b"initial".to_vec()))
        .await
        .expect("Failed to SET initial");

    // Verify initial value
    let value = client.execute(Get::new(&key)).await.expect("Failed to GET");
    assert_eq!(value, Some(bytes::Bytes::from("initial")));

    // Overwrite with new value
    client
        .execute(Set::new(&key, b"updated".to_vec()))
        .await
        .expect("Failed to SET updated");

    // Verify updated value
    let value = client.execute(Get::new(&key)).await.expect("Failed to GET");
    assert_eq!(value, Some(bytes::Bytes::from("updated")));

    // Cleanup
    client.execute(Del::new(vec![key])).await.ok();
}
