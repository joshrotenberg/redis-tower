mod helpers;

use helpers::standalone::setup_redis;
use redis_tower::commands::*;

#[tokio::test]
async fn test_xadd_and_xlen() {
    let client = setup_redis().await;
    let stream = "test_stream";

    // Add entry with auto-generated ID
    let id1: String = client
        .call(
            XAdd::new(stream)
                .field("sensor", "temperature")
                .field("value", "23.5"),
        )
        .await
        .unwrap();

    assert!(!id1.is_empty());
    assert!(id1.contains('-')); // Format: timestamp-sequence

    // Add another entry
    let id2: String = client
        .call(
            XAdd::new(stream)
                .field("sensor", "humidity")
                .field("value", "65"),
        )
        .await
        .unwrap();

    assert!(id2 > id1); // IDs are monotonically increasing

    // Check stream length
    let len: i64 = client.call(XLen::new(stream)).await.unwrap();
    assert_eq!(len, 2);
}

#[tokio::test]
async fn test_xadd_with_maxlen() {
    let client = setup_redis().await;
    let stream = "maxlen_stream";

    // Add entries with MAXLEN limit (exact trimming for predictable tests)
    for i in 0..10 {
        let _: String = client
            .call(
                XAdd::new(stream)
                    .field("count", i.to_string())
                    .maxlen_exact(5), // Keep exactly 5 entries
            )
            .await
            .unwrap();
    }

    // Stream should have exactly 5 entries
    let len: i64 = client.call(XLen::new(stream)).await.unwrap();
    assert_eq!(len, 5);
}

#[tokio::test]
async fn test_xdel() {
    let client = setup_redis().await;
    let stream = "del_stream";

    // Add some entries
    let id1: String = client
        .call(XAdd::new(stream).field("data", "entry1"))
        .await
        .unwrap();

    let _id2: String = client
        .call(XAdd::new(stream).field("data", "entry2"))
        .await
        .unwrap();

    // Delete one entry
    let deleted: i64 = client
        .call(XDel::new(stream).id(id1.clone()))
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    // Length should be 1 now
    let len: i64 = client.call(XLen::new(stream)).await.unwrap();
    assert_eq!(len, 1);
}

#[tokio::test]
async fn test_xtrim() {
    let client = setup_redis().await;
    let stream = "trim_stream";

    // Add 10 entries
    for i in 0..10 {
        let _: String = client
            .call(XAdd::new(stream).field("count", i.to_string()))
            .await
            .unwrap();
    }

    // Trim to keep exactly 5 most recent
    let trimmed: i64 = client
        .call(XTrim::new(stream).maxlen_exact(5))
        .await
        .unwrap();

    assert_eq!(trimmed, 5); // Should have trimmed 5 entries (10 - 5 = 5)

    // Length should be exactly 5
    let len: i64 = client.call(XLen::new(stream)).await.unwrap();
    assert_eq!(len, 5);
}

#[tokio::test]
async fn test_xlen_empty_stream() {
    let client = setup_redis().await;
    let stream = "empty_stream";

    // Non-existent stream has length 0
    let len: i64 = client.call(XLen::new(stream)).await.unwrap();
    assert_eq!(len, 0);
}

#[tokio::test]
async fn test_xadd_multiple_fields() {
    let client = setup_redis().await;
    let stream = "multi_field_stream";

    // Add entry with multiple fields
    let id: String = client
        .call(
            XAdd::new(stream)
                .field("sensor_id", "sensor_001")
                .field("temperature", "23.5")
                .field("humidity", "65")
                .field("pressure", "1013")
                .field("timestamp", "2024-01-01T12:00:00Z"),
        )
        .await
        .unwrap();

    assert!(!id.is_empty());

    let len: i64 = client.call(XLen::new(stream)).await.unwrap();
    assert_eq!(len, 1);
}

#[tokio::test]
async fn test_xadd_binary_data() {
    let client = setup_redis().await;
    let stream = "binary_stream";

    // Add entry with binary data in field values
    let id: String = client
        .call(
            XAdd::new(stream)
                .field("type", "binary")
                .field("data", "\\xFF\\x00\\xAB"),
        )
        .await
        .unwrap();

    assert!(!id.is_empty());
}

#[tokio::test]
async fn test_xdel_multiple_ids() {
    let client = setup_redis().await;
    let stream = "del_multi_stream";

    // Add several entries
    let id1: String = client
        .call(XAdd::new(stream).field("n", "1"))
        .await
        .unwrap();
    let id2: String = client
        .call(XAdd::new(stream).field("n", "2"))
        .await
        .unwrap();
    let id3: String = client
        .call(XAdd::new(stream).field("n", "3"))
        .await
        .unwrap();
    let _id4: String = client
        .call(XAdd::new(stream).field("n", "4"))
        .await
        .unwrap();

    // Delete multiple at once
    let deleted: i64 = client
        .call(XDel::new(stream).id(id1).id(id2).id(id3))
        .await
        .unwrap();

    assert_eq!(deleted, 3);

    let len: i64 = client.call(XLen::new(stream)).await.unwrap();
    assert_eq!(len, 1);
}

#[tokio::test]
async fn test_xdel_nonexistent_id() {
    let client = setup_redis().await;
    let stream = "del_nonexist_stream";

    let _id: String = client
        .call(XAdd::new(stream).field("data", "test"))
        .await
        .unwrap();

    // Try to delete a non-existent ID
    let deleted: i64 = client
        .call(XDel::new(stream).id("1234567890-0"))
        .await
        .unwrap();

    assert_eq!(deleted, 0);

    // Original entry should still be there
    let len: i64 = client.call(XLen::new(stream)).await.unwrap();
    assert_eq!(len, 1);
}

#[tokio::test]
async fn test_xtrim_approx() {
    let client = setup_redis().await;
    let stream = "trim_approx_stream";

    // Add 100 entries
    for i in 0..100 {
        let _: String = client
            .call(XAdd::new(stream).field("count", i.to_string()))
            .await
            .unwrap();
    }

    // Approximate trim (more efficient but less precise)
    let _: i64 = client.call(XTrim::new(stream).maxlen(50)).await.unwrap();

    let len: i64 = client.call(XLen::new(stream)).await.unwrap();
    // With ~, Redis may keep significantly more entries for efficiency
    // Typical behavior: keeps entries in macro nodes, which can be 100+ entries
    assert!((50..=100).contains(&len)); // Approximate, so allow generous margin
}

#[tokio::test]
async fn test_stream_as_message_queue() {
    let client = setup_redis().await;
    let stream = "message_queue";

    // Producer: Add messages
    for i in 0..5 {
        let _: String = client
            .call(
                XAdd::new(stream)
                    .field("job_id", format!("job_{}", i))
                    .field("status", "pending")
                    .field("data", format!("payload_{}", i)),
            )
            .await
            .unwrap();
    }

    // Check queue length
    let len: i64 = client.call(XLen::new(stream)).await.unwrap();
    assert_eq!(len, 5);
}

#[tokio::test]
async fn test_xadd_with_id_validation() {
    let client = setup_redis().await;
    let stream = "id_validation_stream";

    // Auto-generated ID should be valid format
    let id: String = client
        .call(XAdd::new(stream).field("test", "value"))
        .await
        .unwrap();

    // ID format: timestamp-sequence (e.g., "1609459200000-0")
    assert!(id.contains('-'));
    let parts: Vec<&str> = id.split('-').collect();
    assert_eq!(parts.len(), 2);

    // Both parts should be numeric
    assert!(parts[0].parse::<u64>().is_ok());
    assert!(parts[1].parse::<u64>().is_ok());
}

#[tokio::test]
async fn test_concurrent_xadd() {
    let client = setup_redis().await;
    let stream = "concurrent_stream";

    // Add entries concurrently
    let mut handles = vec![];
    for i in 0..10 {
        let client = client.clone();
        let stream = stream.to_string();
        let handle = tokio::spawn(async move {
            client
                .call(XAdd::new(&stream).field("worker", i.to_string()))
                .await
                .unwrap()
        });
        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // All 10 entries should be present
    let len: i64 = client.call(XLen::new(stream)).await.unwrap();
    assert_eq!(len, 10);
}
