//! Integration tests for cursor-based SCAN commands
//!
//! Tests SCAN and HSCAN iteration with various options.
//!
//! Run with: cargo test --test integration_scan

mod helpers;

use helpers::standalone::setup_redis;
use redis_tower::commands::scan::{HScan, Scan};
use redis_tower::commands::*;

#[tokio::test]
async fn test_scan_basic() {
    let client = setup_redis().await;

    // Set up test keys
    for i in 0..20 {
        client
            .call(Set::new(format!("scan_test:{}", i), "value"))
            .await
            .unwrap();
    }

    // Start scanning from cursor 0
    let result = client.call(Scan::new(0)).await.unwrap();

    // Should get some keys (exact count depends on Redis)
    assert!(!result.keys.is_empty() || result.cursor != 0);

    // Clean up
    for i in 0..20 {
        client
            .call(Del::new(vec![format!("scan_test:{}", i)]))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_scan_with_pattern() {
    let client = setup_redis().await;

    // Set up test keys with different patterns
    for i in 0..10 {
        client
            .call(Set::new(format!("pattern_test:user:{}", i), "value"))
            .await
            .unwrap();
        client
            .call(Set::new(format!("pattern_test:session:{}", i), "value"))
            .await
            .unwrap();
    }

    // Scan for only user keys
    let result = client
        .call(Scan::new(0).pattern("pattern_test:user:*"))
        .await
        .unwrap();

    // All returned keys should match pattern
    for key in &result.keys {
        let key_str = String::from_utf8_lossy(key);
        assert!(key_str.starts_with("pattern_test:user:"));
    }

    // Clean up
    for i in 0..10 {
        client
            .call(Del::new(vec![
                format!("pattern_test:user:{}", i),
                format!("pattern_test:session:{}", i),
            ]))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_scan_with_count() {
    let client = setup_redis().await;

    // Set up many keys
    for i in 0..100 {
        client
            .call(Set::new(format!("count_test:{}", i), "value"))
            .await
            .unwrap();
    }

    // Scan with COUNT hint
    let result = client.call(Scan::new(0).count(10)).await.unwrap();

    // Should get results (exact count is a hint, not guaranteed)
    assert!(!result.keys.is_empty() || result.cursor != 0);

    // Clean up
    for i in 0..100 {
        client
            .call(Del::new(vec![format!("count_test:{}", i)]))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_scan_full_iteration() {
    let client = setup_redis().await;

    // Set up known keys
    let test_keys: Vec<String> = (0..30).map(|i| format!("iteration_test:{}", i)).collect();

    for key in &test_keys {
        client.call(Set::new(key, "value")).await.unwrap();
    }

    // Iterate until complete
    let mut cursor = 0u64;
    let mut all_keys = Vec::new();

    loop {
        let result = client
            .call(Scan::new(cursor).pattern("iteration_test:*"))
            .await
            .unwrap();

        all_keys.extend(result.keys);
        cursor = result.cursor;

        if cursor == 0 {
            break;
        }
    }

    // Should have found all our test keys
    assert!(all_keys.len() >= test_keys.len());

    // Clean up
    client.call(Del::new(test_keys)).await.unwrap();
}

#[tokio::test]
async fn test_hscan_basic() {
    let client = setup_redis().await;

    let hash_key = "hscan_test";

    // Set up hash with multiple fields
    for i in 0..20 {
        client
            .call(HSet::new(
                hash_key,
                format!("field{}", i),
                format!("value{}", i).into_bytes(),
            ))
            .await
            .unwrap();
    }

    // Start scanning hash from cursor 0
    let result = client.call(HScan::new(hash_key, 0)).await.unwrap();

    // Should get some fields
    assert!(!result.fields.is_empty() || result.cursor != 0);

    // Clean up
    client
        .call(Del::new(vec![hash_key.to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_hscan_with_pattern() {
    let client = setup_redis().await;

    let hash_key = "hscan_pattern_test";

    // Set up hash with different field patterns
    for i in 0..10 {
        client
            .call(HSet::new(
                hash_key,
                format!("user_{}", i),
                format!("user_value{}", i).into_bytes(),
            ))
            .await
            .unwrap();
        client
            .call(HSet::new(
                hash_key,
                format!("session_{}", i),
                format!("session_value{}", i).into_bytes(),
            ))
            .await
            .unwrap();
    }

    // Scan for only user fields
    let result = client
        .call(HScan::new(hash_key, 0).pattern("user_*"))
        .await
        .unwrap();

    // All returned fields should match pattern
    for (field, _value) in &result.fields {
        let field_str = String::from_utf8_lossy(field);
        assert!(field_str.starts_with("user_"));
    }

    // Clean up
    client
        .call(Del::new(vec![hash_key.to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_hscan_with_count() {
    let client = setup_redis().await;

    let hash_key = "hscan_count_test";

    // Set up hash with many fields
    for i in 0..50 {
        client
            .call(HSet::new(
                hash_key,
                format!("field{}", i),
                format!("value{}", i).into_bytes(),
            ))
            .await
            .unwrap();
    }

    // Scan with COUNT hint
    let result = client
        .call(HScan::new(hash_key, 0).count(10))
        .await
        .unwrap();

    // Should get results
    assert!(!result.fields.is_empty() || result.cursor != 0);

    // Clean up
    client
        .call(Del::new(vec![hash_key.to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_hscan_full_iteration() {
    let client = setup_redis().await;

    let hash_key = "hscan_iteration_test";

    // Set up known fields
    for i in 0..30 {
        client
            .call(HSet::new(
                hash_key,
                format!("field{}", i),
                format!("value{}", i).into_bytes(),
            ))
            .await
            .unwrap();
    }

    // Iterate until complete
    let mut cursor = 0u64;
    let mut all_fields = Vec::new();

    loop {
        let result = client.call(HScan::new(hash_key, cursor)).await.unwrap();

        all_fields.extend(result.fields);
        cursor = result.cursor;

        if cursor == 0 {
            break;
        }
    }

    // Should have found all our test fields
    assert_eq!(all_fields.len(), 30);

    // Clean up
    client
        .call(Del::new(vec![hash_key.to_string()]))
        .await
        .unwrap();
}
