//! Integration tests for cursor-based SCAN commands
//!
//! Tests SCAN, HSCAN, SSCAN, ZSCAN iteration with various options and streaming APIs.
//!
//! Run with: cargo test --test integration_scan

mod helpers;

use helpers::standalone::setup_redis;
use redis_tower::commands::scan::{HScan, SScan, Scan, ZScan};
use redis_tower::commands::*;
use redis_tower::streaming::{HScanStream, SScanStream, ScanStream, ZScanStream};

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

// === Streaming API Tests ===

#[tokio::test]
async fn test_scan_stream() {
    let client = setup_redis().await;

    // Set up test keys
    for i in 0..30 {
        client
            .call(Set::new(format!("stream_test:{}", i), "value"))
            .await
            .unwrap();
    }

    // Use streaming API
    let mut stream = ScanStream::new(client.clone()).pattern("stream_test:*");

    let mut total_keys = 0;
    while let Some(keys) = stream.next().await.unwrap() {
        total_keys += keys.len();
    }

    // Should have found all our test keys
    assert_eq!(total_keys, 30);

    // Clean up
    for i in 0..30 {
        client
            .call(Del::new(vec![format!("stream_test:{}", i)]))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_hscan_stream() {
    let client = setup_redis().await;

    let hash_key = "hscan_stream_test";

    // Set up hash with fields
    for i in 0..25 {
        client
            .call(HSet::new(
                hash_key,
                format!("field{}", i),
                format!("value{}", i).into_bytes(),
            ))
            .await
            .unwrap();
    }

    // Use streaming API
    let mut stream = HScanStream::new(client.clone(), hash_key);

    let mut total_fields = 0;
    while let Some(fields) = stream.next().await.unwrap() {
        total_fields += fields.len();
    }

    // Should have found all our fields
    assert_eq!(total_fields, 25);

    // Clean up
    client
        .call(Del::new(vec![hash_key.to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_sscan_stream() {
    let client = setup_redis().await;

    let set_key = "sscan_stream_test";

    // Set up set with members
    for i in 0..20 {
        client
            .call(Sadd::new(set_key, format!("member{}", i).into_bytes()))
            .await
            .unwrap();
    }

    // Use streaming API
    let mut stream = SScanStream::new(client.clone(), set_key);

    let mut total_members = 0;
    while let Some(members) = stream.next().await.unwrap() {
        total_members += members.len();
    }

    // Should have found all our members
    assert_eq!(total_members, 20);

    // Clean up
    client
        .call(Del::new(vec![set_key.to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_zscan_stream() {
    let client = setup_redis().await;

    let zset_key = "zscan_stream_test";

    // Set up sorted set with members and scores
    for i in 0..15 {
        let zadd = Zadd::new(zset_key).member(i as f64, format!("member{}", i).into_bytes());
        client.call(zadd).await.unwrap();
    }

    // Use streaming API
    let mut stream = ZScanStream::new(client.clone(), zset_key);

    let mut total_members = 0;
    while let Some(members) = stream.next().await.unwrap() {
        total_members += members.len();
    }

    // Should have found all our members
    assert_eq!(total_members, 15);

    // Clean up
    client
        .call(Del::new(vec![zset_key.to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_stream_with_count() {
    let client = setup_redis().await;

    // Set up many keys
    for i in 0..50 {
        client
            .call(Set::new(format!("count_stream:{}", i), "value"))
            .await
            .unwrap();
    }

    // Use streaming API with COUNT hint
    let mut stream = ScanStream::new(client.clone())
        .pattern("count_stream:*")
        .count(5);

    let mut batches = 0;
    let mut total_keys = 0;
    while let Some(keys) = stream.next().await.unwrap() {
        batches += 1;
        total_keys += keys.len();
    }

    // Should have found all keys
    assert_eq!(total_keys, 50);
    // Should have multiple batches (though exact count depends on Redis)
    assert!(batches > 1);

    // Clean up
    for i in 0..50 {
        client
            .call(Del::new(vec![format!("count_stream:{}", i)]))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_stream_reset() {
    let client = setup_redis().await;

    let set_key = "reset_test";

    // Set up set
    for i in 0..10 {
        client
            .call(Sadd::new(set_key, format!("member{}", i).into_bytes()))
            .await
            .unwrap();
    }

    // First iteration
    let mut stream = SScanStream::new(client.clone(), set_key);
    let mut first_count = 0;
    while let Some(members) = stream.next().await.unwrap() {
        first_count += members.len();
    }

    // Reset and iterate again
    stream.reset();
    let mut second_count = 0;
    while let Some(members) = stream.next().await.unwrap() {
        second_count += members.len();
    }

    // Both iterations should find same number of members
    assert_eq!(first_count, 10);
    assert_eq!(second_count, 10);

    // Clean up
    client
        .call(Del::new(vec![set_key.to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_sscan_basic() {
    let client = setup_redis().await;

    let set_key = "sscan_test";

    // Set up set with members
    for i in 0..15 {
        client
            .call(Sadd::new(set_key, format!("member{}", i).into_bytes()))
            .await
            .unwrap();
    }

    // Scan the set
    let result = client.call(SScan::new(set_key, 0)).await.unwrap();

    // Should get some members
    assert!(!result.members.is_empty() || result.cursor != 0);

    // Clean up
    client
        .call(Del::new(vec![set_key.to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_zscan_basic() {
    let client = setup_redis().await;

    let zset_key = "zscan_test";

    // Set up sorted set
    for i in 0..15 {
        let zadd = Zadd::new(zset_key).member(i as f64, format!("member{}", i).into_bytes());
        client.call(zadd).await.unwrap();
    }

    // Scan the sorted set
    let result = client.call(ZScan::new(zset_key, 0)).await.unwrap();

    // Should get some members
    assert!(!result.members.is_empty() || result.cursor != 0);

    // Verify scores are included
    if !result.members.is_empty() {
        for (member, score) in &result.members {
            assert!(!member.is_empty());
            assert!(*score >= 0.0);
        }
    }

    // Clean up
    client
        .call(Del::new(vec![zset_key.to_string()]))
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
