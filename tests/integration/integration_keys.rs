//! Integration tests for key management commands
//!
//! Tests key operations like EXPIRE, TTL, RENAME, DEL, EXISTS, SCAN, etc.
//!
//! Run with: cargo test --test integration_keys

mod helpers;

use bytes::Bytes;
use helpers::standalone::setup_redis;
use redis_tower::commands::*;

#[tokio::test]
async fn test_expire_and_ttl() {
    let client = setup_redis().await;

    // Set a key and give it a TTL
    client.call(Set::new("expire_key", "value")).await.unwrap();

    let result: bool = client.call(Expire::new("expire_key", 60)).await.unwrap();
    assert!(result); // Key exists and TTL was set

    // Check TTL (should be around 60 seconds)
    let ttl: i64 = client.call(Ttl::new("expire_key")).await.unwrap();
    assert!(ttl > 0 && ttl <= 60);

    // Clean up
    client
        .call(Del::new(vec!["expire_key".to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_persist() {
    let client = setup_redis().await;

    // Set a key with expiration
    client.call(Set::new("persist_key", "value")).await.unwrap();
    client.call(Expire::new("persist_key", 60)).await.unwrap();

    // Verify it has a TTL
    let ttl: i64 = client.call(Ttl::new("persist_key")).await.unwrap();
    assert!(ttl > 0);

    // Remove the expiration
    let result: bool = client.call(Persist::new("persist_key")).await.unwrap();
    assert!(result);

    // Verify TTL is now -1 (no expiration)
    let ttl: i64 = client.call(Ttl::new("persist_key")).await.unwrap();
    assert_eq!(ttl, -1);

    // Clean up
    client
        .call(Del::new(vec!["persist_key".to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_exists() {
    let client = setup_redis().await;

    // Set some keys
    client.call(Set::new("exists1", "value1")).await.unwrap();
    client.call(Set::new("exists2", "value2")).await.unwrap();

    // Check single key
    let count: i64 = client.call(Exists::new("exists1")).await.unwrap();
    assert_eq!(count, 1);

    // Check multiple keys
    let count: i64 = client
        .call(Exists::multiple(vec!["exists1", "exists2", "nonexistent"]))
        .await
        .unwrap();
    assert_eq!(count, 2);

    // Clean up
    client
        .call(Del::new(vec!["exists1".to_string(), "exists2".to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_del() {
    let client = setup_redis().await;

    // Set some keys
    client.call(Set::new("del1", "value1")).await.unwrap();
    client.call(Set::new("del2", "value2")).await.unwrap();

    // Delete them
    let deleted: i64 = client
        .call(Del::new(vec!["del1".to_string(), "del2".to_string()]))
        .await
        .unwrap();
    assert_eq!(deleted, 2);

    // Verify they're gone
    let exists: i64 = client
        .call(Exists::multiple(vec!["del1", "del2"]))
        .await
        .unwrap();
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn test_rename() {
    let client = setup_redis().await;

    // Set a key
    client.call(Set::new("old_name", "value")).await.unwrap();

    // Rename it
    client
        .call(Rename::new("old_name", "new_name"))
        .await
        .unwrap();

    // Verify old name is gone
    let old_exists: i64 = client.call(Exists::new("old_name")).await.unwrap();
    assert_eq!(old_exists, 0);

    // Verify new name exists
    let new_value: Option<Bytes> = client.call(Get::new("new_name")).await.unwrap();
    assert_eq!(
        new_value.as_ref().map(|b| b.as_ref()),
        Some(b"value".as_ref())
    );

    // Clean up
    client
        .call(Del::new(vec!["new_name".to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_type() {
    let client = setup_redis().await;

    // String type
    client.call(Set::new("string_key", "value")).await.unwrap();
    let type_result: String = client.call(Type::new("string_key")).await.unwrap();
    assert_eq!(type_result, "string");

    // List type
    client
        .call(LPush::single("list_key", Bytes::from("value")))
        .await
        .unwrap();
    let type_result: String = client.call(Type::new("list_key")).await.unwrap();
    assert_eq!(type_result, "list");

    // Hash type
    client
        .call(HSet::new("hash_key", "field", Bytes::from("value")))
        .await
        .unwrap();
    let type_result: String = client.call(Type::new("hash_key")).await.unwrap();
    assert_eq!(type_result, "hash");

    // Clean up
    client
        .call(Del::new(vec![
            "string_key".to_string(),
            "list_key".to_string(),
            "hash_key".to_string(),
        ]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_keys_pattern() {
    let client = setup_redis().await;

    // Set some keys with unique patterns to avoid conflicts with parallel tests
    client
        .call(Set::new("test_keys_pattern:user:1:name", "Alice"))
        .await
        .unwrap();
    client
        .call(Set::new("test_keys_pattern:user:2:name", "Bob"))
        .await
        .unwrap();
    client
        .call(Set::new("test_keys_pattern:other:key", "value"))
        .await
        .unwrap();

    // Search for user keys
    let keys: Vec<String> = client
        .call(Keys::new("test_keys_pattern:user:*"))
        .await
        .unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"test_keys_pattern:user:1:name".to_string()));
    assert!(keys.contains(&"test_keys_pattern:user:2:name".to_string()));

    // Clean up
    client
        .call(Del::new(vec![
            "test_keys_pattern:user:1:name".to_string(),
            "test_keys_pattern:user:2:name".to_string(),
            "test_keys_pattern:other:key".to_string(),
        ]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_scan() {
    let client = setup_redis().await;

    // Set some keys
    for i in 0..10 {
        client
            .call(Set::new(format!("scan_key:{}", i), "value"))
            .await
            .unwrap();
    }

    // Scan for keys
    let (_cursor, keys): (u64, Vec<String>) = client
        .call(Scan::new(0).pattern("scan_key:*").count(100))
        .await
        .unwrap();

    // We should get all 10 keys (cursor may be 0 or non-zero depending on Redis)
    assert!(!keys.is_empty()); // At least some keys returned
    assert!(keys.iter().all(|k| k.starts_with("scan_key:")));

    // Clean up
    let del_keys: Vec<String> = (0..10).map(|i| format!("scan_key:{}", i)).collect();
    client.call(Del::new(del_keys)).await.unwrap();
}
