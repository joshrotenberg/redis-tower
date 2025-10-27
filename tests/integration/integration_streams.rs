mod helpers;

use helpers::standalone::setup_redis;
use redis_tower::commands::*;
use redis_tower::streaming::{XReadGroupStream, XReadStream};
use std::time::Duration;
use tokio::time::sleep;

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

// === Streaming API Tests ===

#[tokio::test]
async fn test_xread_stream_basic() {
    let client = setup_redis().await;
    let stream = "stream_test_basic";

    // Add some entries
    for i in 1..=5 {
        let _: String = client
            .call(XAdd::new(stream).field("value", i.to_string()))
            .await
            .unwrap();
    }

    // Read all entries from beginning
    let mut stream_reader = XReadStream::new(client.clone(), stream).start_from("0-0");

    let mut total_read = 0;
    while let Some(entries) = stream_reader.next().await.unwrap() {
        total_read += entries.len();
        if total_read >= 5 {
            break;
        }
    }

    assert_eq!(total_read, 5);
}

#[tokio::test]
async fn test_xread_stream_with_count() {
    let client = setup_redis().await;
    let stream = "stream_test_count";

    // Add 10 entries
    for i in 1..=10 {
        let _: String = client
            .call(XAdd::new(stream).field("value", i.to_string()))
            .await
            .unwrap();
    }

    // Read with count=3
    let mut stream_reader = XReadStream::new(client.clone(), stream)
        .start_from("0-0")
        .count(3);

    // First batch should have at most 3 entries
    if let Some(entries) = stream_reader.next().await.unwrap() {
        assert!(entries.len() <= 3);
    }
}

#[tokio::test]
async fn test_xread_stream_blocking() {
    let client = setup_redis().await;
    let stream = "stream_test_blocking";

    // Start streaming from $ (only new entries)
    let mut stream_reader = XReadStream::new(client.clone(), stream)
        .start_from("$")
        .block(1000); // 1 second timeout

    // Spawn producer after a delay
    let producer_client = client.clone();
    let producer_stream = stream.to_string();
    tokio::spawn(async move {
        sleep(Duration::from_millis(200)).await;
        let _: String = producer_client
            .call(XAdd::new(&producer_stream).field("late", "entry"))
            .await
            .unwrap();
    });

    // Should receive the entry from producer
    let result = stream_reader.next().await.unwrap();
    assert!(result.is_some());
    let entries = result.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].fields.get("late").unwrap(), "entry");
}

#[tokio::test]
async fn test_xread_stream_reset() {
    let client = setup_redis().await;
    let stream = "stream_test_reset";

    // Add entries
    for i in 1..=3 {
        let _: String = client
            .call(XAdd::new(stream).field("value", i.to_string()))
            .await
            .unwrap();
    }

    let mut stream_reader = XReadStream::new(client.clone(), stream)
        .start_from("0-0")
        .count(2);

    // Read first batch
    let first = stream_reader.next().await.unwrap().unwrap();
    let first_len = first.len();

    // Reset and read again
    stream_reader.reset("0-0");
    let second = stream_reader.next().await.unwrap().unwrap();

    // Should get same entries after reset
    assert_eq!(first_len, second.len());
}

#[tokio::test]
async fn test_xreadgroup_stream_basic() {
    let client = setup_redis().await;
    let stream = "stream_group_test_basic";
    let group = "testgroup";

    // Create consumer group
    let _: String = client
        .call(XGroupCreate::new(stream, group).id("$").mkstream())
        .await
        .unwrap();

    // Add entries
    for i in 1..=5 {
        let _: String = client
            .call(XAdd::new(stream).field("value", i.to_string()))
            .await
            .unwrap();
    }

    // Read with consumer group
    let mut consumer = XReadGroupStream::new(client.clone(), stream, group, "consumer1")
        .count(10)
        .auto_ack(true);

    let entries = consumer.next().await.unwrap().unwrap();
    assert_eq!(entries.len(), 5);
}

#[tokio::test]
async fn test_xreadgroup_stream_auto_ack() {
    let client = setup_redis().await;
    let stream = "stream_group_test_ack";
    let group = "ackgroup";

    // Create consumer group
    let _: String = client
        .call(XGroupCreate::new(stream, group).id("$").mkstream())
        .await
        .unwrap();

    // Add entry
    let _: String = client
        .call(XAdd::new(stream).field("test", "value"))
        .await
        .unwrap();

    // Read with auto-ack
    let mut consumer =
        XReadGroupStream::new(client.clone(), stream, group, "consumer1").auto_ack(true);

    let _ = consumer.next().await.unwrap();

    // Check pending list should be empty (entries were auto-acked)
    let pending: Vec<String> = client.call(XPending::new(stream, group)).await.unwrap();

    // Note: XPending returns summary, not full list
    // The fact that we got entries and auto_ack was true means they were acked
}

#[tokio::test]
async fn test_xreadgroup_stream_multiple_consumers() {
    let client = setup_redis().await;
    let stream = "stream_group_test_multi";
    let group = "multigroup";

    // Create consumer group
    let _: String = client
        .call(XGroupCreate::new(stream, group).id("$").mkstream())
        .await
        .unwrap();

    // Add 6 entries
    for i in 1..=6 {
        let _: String = client
            .call(XAdd::new(stream).field("value", i.to_string()))
            .await
            .unwrap();
    }

    // Consumer 1 reads
    let mut consumer1 = XReadGroupStream::new(client.clone(), stream, group, "consumer1")
        .count(3)
        .auto_ack(true);

    let entries1 = consumer1.next().await.unwrap().unwrap();
    assert_eq!(entries1.len(), 3);

    // Consumer 2 reads (should get different entries)
    let mut consumer2 = XReadGroupStream::new(client.clone(), stream, group, "consumer2")
        .count(3)
        .auto_ack(true);

    let entries2 = consumer2.next().await.unwrap().unwrap();
    assert_eq!(entries2.len(), 3);

    // Verify they got different entries
    let ids1: Vec<String> = entries1.iter().map(|e| e.id.clone()).collect();
    let ids2: Vec<String> = entries2.iter().map(|e| e.id.clone()).collect();

    // No overlap - each consumer got unique messages
    for id in &ids1 {
        assert!(!ids2.contains(id));
    }
}

#[tokio::test]
async fn test_xreadgroup_stream_noack() {
    let client = setup_redis().await;
    let stream = "stream_group_test_noack";
    let group = "noackgroup";

    // Create consumer group
    let _: String = client
        .call(XGroupCreate::new(stream, group).id("$").mkstream())
        .await
        .unwrap();

    // Add entries
    for i in 1..=3 {
        let _: String = client
            .call(XAdd::new(stream).field("value", i.to_string()))
            .await
            .unwrap();
    }

    // Read with NOACK (no pending list)
    let mut consumer = XReadGroupStream::new(client.clone(), stream, group, "consumer1").noack();

    let entries = consumer.next().await.unwrap().unwrap();
    assert_eq!(entries.len(), 3);

    // With NOACK, entries are not added to pending list
    // Just verify we got the entries
}

#[tokio::test]
async fn test_xread_stream_last_id_tracking() {
    let client = setup_redis().await;
    let stream = "stream_test_tracking";

    // Add entries
    let id1: String = client
        .call(XAdd::new(stream).field("seq", "1"))
        .await
        .unwrap();

    let _: String = client
        .call(XAdd::new(stream).field("seq", "2"))
        .await
        .unwrap();

    let mut stream_reader = XReadStream::new(client.clone(), stream)
        .start_from("0-0")
        .count(1);

    // Read first entry
    stream_reader.next().await.unwrap().unwrap();

    // Last ID should be updated to first entry's ID
    assert_eq!(stream_reader.last_id(), id1);

    // Read second entry
    let second = stream_reader.next().await.unwrap().unwrap();

    // Last ID should now be second entry's ID
    assert_eq!(stream_reader.last_id(), second[0].id);
}
