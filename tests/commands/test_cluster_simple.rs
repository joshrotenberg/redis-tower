//! Simple cluster integration tests using direct connections.
//!
//! These tests bypass the ClusterClient's automatic slot discovery
//! and test commands against individual cluster nodes directly.
//!
//! Start with: docker-compose up -d
//! Run with: cargo test --test test_cluster_simple

use redis_tower::client::RedisConnection;
use redis_tower::cluster::slot_for_key;
use redis_tower::commands::{Del, Get, Set};

/// Connect to a specific cluster node
async fn connect_node(port: u16) -> RedisConnection {
    let addr = format!("127.0.0.1:{}", port);
    RedisConnection::connect(&addr)
        .await
        .unwrap_or_else(|_| panic!("Failed to connect to {}", addr))
}

/// Helper to create a unique test key
fn test_key(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let count = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("test:cluster:simple:{}:{}", prefix, count)
}

#[tokio::test]
async fn test_slot_calculation() {
    // Test that slot calculation is deterministic
    let key1 = "user:123";
    let slot1a = slot_for_key(key1.as_bytes());
    let slot1b = slot_for_key(key1.as_bytes());
    assert_eq!(slot1a, slot1b);

    // Test hash tags
    let key2 = "{user}:123";
    let key3 = "{user}:456";
    let slot2 = slot_for_key(key2.as_bytes());
    let slot3 = slot_for_key(key3.as_bytes());
    assert_eq!(
        slot2, slot3,
        "Keys with same hash tag should have same slot"
    );

    // Verify slots are in valid range
    assert!(slot1a < 16384);
    assert!(slot2 < 16384);

    println!("Slot for '{}': {}", key1, slot1a);
    println!("Slot for '{}': {}", key2, slot2);
    println!("Slot for '{}': {}", key3, slot3);
}

#[tokio::test]
async fn test_direct_node_operations() {
    // Connect to first master node
    let client = connect_node(7100).await;
    let key = test_key("direct");

    // SET
    client
        .execute(Set::new(&key, b"test_value".to_vec()))
        .await
        .expect("Failed to SET");

    // GET
    let value = client.execute(Get::new(&key)).await.expect("Failed to GET");

    assert_eq!(value, Some(bytes::Bytes::from("test_value")));

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
async fn test_cluster_slot_distribution() {
    // Connect to all three master nodes
    let node1 = connect_node(7100).await; // Slots 0-5460
    let node2 = connect_node(7101).await; // Slots 5461-10922
    let node3 = connect_node(7102).await; // Slots 10923-16383

    // Test that we can write to different nodes based on slot
    let mut keys = Vec::new();

    for i in 0..30 {
        let key = test_key(&format!("dist{}", i));
        let slot = slot_for_key(key.as_bytes());

        // Determine which node should handle this key
        let client = if slot <= 5460 {
            &node1
        } else if slot <= 10922 {
            &node2
        } else {
            &node3
        };

        // Write to the correct node
        client
            .execute(Set::new(&key, format!("value{}", i)))
            .await
            .expect("Failed to SET");

        keys.push(key);
    }

    // Verify we can read back from correct nodes
    for (i, key) in keys.iter().enumerate() {
        let slot = slot_for_key(key.as_bytes());

        let client = if slot <= 5460 {
            &node1
        } else if slot <= 10922 {
            &node2
        } else {
            &node3
        };

        let value = client.execute(Get::new(key)).await.expect("Failed to GET");

        assert_eq!(value, Some(bytes::Bytes::from(format!("value{}", i))));
    }

    // Cleanup
    for key in keys {
        let slot = slot_for_key(key.as_bytes());
        let client = if slot <= 5460 {
            &node1
        } else if slot <= 10922 {
            &node2
        } else {
            &node3
        };
        client.execute(Del::new(vec![key])).await.ok();
    }
}

#[tokio::test]
async fn test_hash_tag_same_node() {
    let node1 = connect_node(7100).await;
    let node2 = connect_node(7101).await;
    let node3 = connect_node(7102).await;

    // Keys with same hash tag
    let key1 = test_key("tag:{group}:a");
    let key2 = test_key("tag:{group}:b");
    let key3 = test_key("tag:{group}:c");

    // All should have same slot
    let slot1 = slot_for_key(key1.as_bytes());
    let slot2 = slot_for_key(key2.as_bytes());
    let slot3 = slot_for_key(key3.as_bytes());

    assert_eq!(slot1, slot2);
    assert_eq!(slot2, slot3);

    // Determine which node
    let client = if slot1 <= 5460 {
        &node1
    } else if slot1 <= 10922 {
        &node2
    } else {
        &node3
    };

    // Set all three keys
    client
        .execute(Set::new(&key1, b"value1".to_vec()))
        .await
        .unwrap();
    client
        .execute(Set::new(&key2, b"value2".to_vec()))
        .await
        .unwrap();
    client
        .execute(Set::new(&key3, b"value3".to_vec()))
        .await
        .unwrap();

    // Get all three keys from same node
    let v1 = client.execute(Get::new(&key1)).await.unwrap();
    let v2 = client.execute(Get::new(&key2)).await.unwrap();
    let v3 = client.execute(Get::new(&key3)).await.unwrap();

    assert_eq!(v1, Some(bytes::Bytes::from("value1")));
    assert_eq!(v2, Some(bytes::Bytes::from("value2")));
    assert_eq!(v3, Some(bytes::Bytes::from("value3")));

    // Cleanup
    client.execute(Del::new(vec![key1, key2, key3])).await.ok();
}

#[tokio::test]
async fn test_wrong_node_moved_error() {
    // Connect to node that doesn't own a specific slot
    let node1 = connect_node(7100).await; // Owns slots 0-5460

    // Create a key that hashes to a different slot range
    // We'll keep trying keys until we find one not in 0-5460
    for i in 0..1000 {
        let key = format!("test:moved:{}", i);
        let slot = slot_for_key(key.as_bytes());

        if slot > 5460 {
            // This key should be on a different node
            println!(
                "Testing key '{}' with slot {} (should be on different node)",
                key, slot
            );

            // Try to SET on wrong node - should get MOVED error
            let result = node1.execute(Set::new(&key, b"value".to_vec())).await;

            match result {
                Err(redis_tower::types::RedisError::Moved {
                    slot: moved_slot,
                    addr,
                }) => {
                    println!("Got MOVED redirect: slot={}, addr={}", moved_slot, addr);
                    assert_eq!(moved_slot, slot);
                    // Success - we got a MOVED error as expected
                    return;
                }
                Ok(_) => {
                    // Key happened to be on this node anyway, try another
                    continue;
                }
                Err(e) => {
                    panic!("Unexpected error (expected MOVED): {:?}", e);
                }
            }
        }
    }

    panic!("Could not find a key that produces MOVED error");
}
