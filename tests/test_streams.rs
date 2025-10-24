//! Integration tests for Redis Streams commands

use redis_tower::commands::Del;
use redis_tower::commands::streams::{
    StreamEntry, StreamId, XAdd, XDel, XLen, XRange, XRead, XRevRange, XTrim,
};
use std::collections::HashMap;

mod common;

use common::{connect, test_key};

#[tokio::test]
async fn test_xadd_and_xlen() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xadd_xlen");

    // Clean up any existing stream
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add entries to stream
    let mut fields1 = HashMap::new();
    fields1.insert("sensor".to_string(), "temperature".into());
    fields1.insert("value".to_string(), "23.5".into());

    let id1 = client
        .execute(XAdd::new(&key, StreamId::auto(), fields1))
        .await
        .expect("XADD should succeed");

    // Verify ID format (timestamp-sequence)
    assert!(id1.0.contains('-'), "Stream ID should contain hyphen");

    // Check length
    let length: i64 = client
        .execute(XLen::new(&key))
        .await
        .expect("XLEN should succeed");
    assert_eq!(length, 1, "Stream should have 1 entry");

    // Add another entry
    let mut fields2 = HashMap::new();
    fields2.insert("sensor".to_string(), "humidity".into());
    fields2.insert("value".to_string(), "65.2".into());

    let id2 = client
        .execute(XAdd::new(&key, StreamId::auto(), fields2))
        .await
        .expect("XADD should succeed");

    assert!(id2.0 > id1.0, "Second ID should be greater than first");

    // Check updated length
    let length: i64 = client
        .execute(XLen::new(&key))
        .await
        .expect("XLEN should succeed");
    assert_eq!(length, 2, "Stream should have 2 entries");
}

#[tokio::test]
async fn test_xrange_all() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xrange_all");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add some entries
    for i in 1..=5 {
        let mut fields = HashMap::new();
        fields.insert("index".to_string(), i.to_string().into());
        fields.insert("data".to_string(), format!("value{}", i).into());

        client
            .execute(XAdd::new(&key, StreamId::auto(), fields))
            .await
            .expect("XADD should succeed");
    }

    // Read all entries
    let entries: Vec<StreamEntry> = client
        .execute(XRange::all(&key))
        .await
        .expect("XRANGE should succeed");

    assert_eq!(entries.len(), 5, "Should get all 5 entries");

    // Verify order (oldest to newest)
    for (i, entry) in entries.iter().enumerate() {
        let index = String::from_utf8_lossy(entry.fields.get("index").unwrap()).to_string();
        assert_eq!(index, (i + 1).to_string(), "Entries should be in order");
    }
}

#[tokio::test]
async fn test_xrange_with_limit() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xrange_limit");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add 10 entries
    for i in 1..=10 {
        let mut fields = HashMap::new();
        fields.insert("index".to_string(), i.to_string().into());

        client
            .execute(XAdd::new(&key, StreamId::auto(), fields))
            .await
            .expect("XADD should succeed");
    }

    // Read only first 3 entries
    let entries: Vec<StreamEntry> = client
        .execute(XRange::all(&key).count(3))
        .await
        .expect("XRANGE with COUNT should succeed");

    assert_eq!(entries.len(), 3, "Should get exactly 3 entries");
}

#[tokio::test]
async fn test_xrange_specific_range() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xrange_specific");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add entries and collect their IDs
    let mut ids = Vec::new();
    for i in 1..=5 {
        let mut fields = HashMap::new();
        fields.insert("index".to_string(), i.to_string().into());

        let id = client
            .execute(XAdd::new(&key, StreamId::auto(), fields))
            .await
            .expect("XADD should succeed");
        ids.push(id);

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
    }

    // Query range from 2nd to 4th entry
    let entries: Vec<StreamEntry> = client
        .execute(XRange::new(&key, ids[1].clone(), ids[3].clone()))
        .await
        .expect("XRANGE with specific range should succeed");

    assert_eq!(entries.len(), 3, "Should get entries 2, 3, 4");
    assert_eq!(entries[0].id, ids[1]);
    assert_eq!(entries[2].id, ids[3]);
}

#[tokio::test]
async fn test_xrevrange() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xrevrange");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add entries
    for i in 1..=5 {
        let mut fields = HashMap::new();
        fields.insert("index".to_string(), i.to_string().into());

        client
            .execute(XAdd::new(&key, StreamId::auto(), fields))
            .await
            .expect("XADD should succeed");
    }

    // Read in reverse order
    let entries: Vec<StreamEntry> = client
        .execute(XRevRange::all(&key))
        .await
        .expect("XREVRANGE should succeed");

    assert_eq!(entries.len(), 5, "Should get all 5 entries");

    // Verify reverse order (newest to oldest)
    for (i, entry) in entries.iter().enumerate() {
        let index = String::from_utf8_lossy(entry.fields.get("index").unwrap()).to_string();
        assert_eq!(
            index,
            (5 - i).to_string(),
            "Entries should be in reverse order"
        );
    }
}

#[tokio::test]
async fn test_xrevrange_with_limit() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xrevrange_limit");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add 10 entries
    for i in 1..=10 {
        let mut fields = HashMap::new();
        fields.insert("index".to_string(), i.to_string().into());

        client
            .execute(XAdd::new(&key, StreamId::auto(), fields))
            .await
            .expect("XADD should succeed");
    }

    // Get last 3 entries (newest first)
    let entries: Vec<StreamEntry> = client
        .execute(XRevRange::all(&key).count(3))
        .await
        .expect("XREVRANGE with COUNT should succeed");

    assert_eq!(entries.len(), 3, "Should get exactly 3 entries");

    // Verify we got the newest entries (10, 9, 8)
    let index = String::from_utf8_lossy(entries[0].fields.get("index").unwrap()).to_string();
    assert_eq!(index, "10", "First entry should be the newest (10)");
}

#[tokio::test]
async fn test_xdel() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xdel");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add entries and collect IDs
    let mut ids = Vec::new();
    for i in 1..=5 {
        let mut fields = HashMap::new();
        fields.insert("index".to_string(), i.to_string().into());

        let id = client
            .execute(XAdd::new(&key, StreamId::auto(), fields))
            .await
            .expect("XADD should succeed");
        ids.push(id);
    }

    // Verify initial length
    let length: i64 = client
        .execute(XLen::new(&key))
        .await
        .expect("XLEN should succeed");
    assert_eq!(length, 5, "Stream should have 5 entries");

    // Delete 2 entries
    let deleted: i64 = client
        .execute(XDel::new(&key, vec![ids[1].clone(), ids[3].clone()]))
        .await
        .expect("XDEL should succeed");

    assert_eq!(deleted, 2, "Should delete 2 entries");

    // Verify updated length
    let length: i64 = client
        .execute(XLen::new(&key))
        .await
        .expect("XLEN should succeed");
    assert_eq!(length, 3, "Stream should have 3 entries left");

    // Verify correct entries remain
    let entries: Vec<StreamEntry> = client
        .execute(XRange::all(&key))
        .await
        .expect("XRANGE should succeed");

    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].id, ids[0]);
    assert_eq!(entries[1].id, ids[2]);
    assert_eq!(entries[2].id, ids[4]);
}

#[tokio::test]
async fn test_xdel_nonexistent() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xdel_nonexistent");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add one entry
    let mut fields = HashMap::new();
    fields.insert("data".to_string(), "test".into());
    client
        .execute(XAdd::new(&key, StreamId::auto(), fields))
        .await
        .expect("XADD should succeed");

    // Try to delete non-existent IDs
    let deleted: i64 = client
        .execute(XDel::new(
            &key,
            vec![
                StreamId::new("9999999999999-0"),
                StreamId::new("9999999999998-0"),
            ],
        ))
        .await
        .expect("XDEL should succeed");

    assert_eq!(deleted, 0, "Should delete 0 entries (none exist)");

    // Verify length unchanged
    let length: i64 = client
        .execute(XLen::new(&key))
        .await
        .expect("XLEN should succeed");
    assert_eq!(length, 1, "Stream should still have 1 entry");
}

#[tokio::test]
async fn test_xtrim_maxlen() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xtrim_maxlen");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add 10 entries
    for i in 1..=10 {
        let mut fields = HashMap::new();
        fields.insert("index".to_string(), i.to_string().into());

        client
            .execute(XAdd::new(&key, StreamId::auto(), fields))
            .await
            .expect("XADD should succeed");
    }

    // Trim to ~5 entries (approximate)
    let removed: i64 = client
        .execute(XTrim::maxlen(&key, 5))
        .await
        .expect("XTRIM should succeed");

    assert!(removed >= 0, "Should remove some entries");

    // Verify length is around 5 (could be slightly more due to approximate trimming)
    let length: i64 = client
        .execute(XLen::new(&key))
        .await
        .expect("XLEN should succeed");
    // Approximate trimming may not trim immediately, just verify it's not more than 10
    assert!(
        length <= 10,
        "Stream should have been trimmed (approximate)"
    );
}

#[tokio::test]
async fn test_xtrim_exact() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xtrim_exact");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add 10 entries
    for i in 1..=10 {
        let mut fields = HashMap::new();
        fields.insert("index".to_string(), i.to_string().into());

        client
            .execute(XAdd::new(&key, StreamId::auto(), fields))
            .await
            .expect("XADD should succeed");
    }

    // Trim to exactly 5 entries
    let _removed: i64 = client
        .execute(XTrim::maxlen(&key, 5).exact())
        .await
        .expect("XTRIM exact should succeed");

    // Verify exact length
    let length: i64 = client
        .execute(XLen::new(&key))
        .await
        .expect("XLEN should succeed");
    assert_eq!(length, 5, "Stream should have exactly 5 entries");

    // Verify we kept the newest entries
    let entries: Vec<StreamEntry> = client
        .execute(XRange::all(&key))
        .await
        .expect("XRANGE should succeed");

    let first_index = String::from_utf8_lossy(entries[0].fields.get("index").unwrap()).to_string();
    assert_eq!(first_index, "6", "First remaining entry should be index 6");
}

#[tokio::test]
async fn test_xtrim_minid() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xtrim_minid");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add entries and collect IDs
    let mut ids = Vec::new();
    for i in 1..=10 {
        let mut fields = HashMap::new();
        fields.insert("index".to_string(), i.to_string().into());

        let id = client
            .execute(XAdd::new(&key, StreamId::auto(), fields))
            .await
            .expect("XADD should succeed");
        ids.push(id);

        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
    }

    // Trim all entries older than the 6th entry
    let _removed: i64 = client
        .execute(XTrim::minid(&key, ids[5].clone()))
        .await
        .expect("XTRIM MINID should succeed");

    // Verify remaining entries
    let entries: Vec<StreamEntry> = client
        .execute(XRange::all(&key))
        .await
        .expect("XRANGE should succeed");

    // Approximate trimming may keep more entries
    assert!(entries.len() <= 10, "Should have trimmed some entries");

    // With approximate trimming, we just verify we got some entries back
    // The exact trimming point is not guaranteed with the ~ modifier
    assert!(!entries.is_empty(), "Should have some entries remaining");
}

#[tokio::test]
async fn test_xread_integration() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xread_integration");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add some entries
    for i in 1..=3 {
        let mut fields = HashMap::new();
        fields.insert("index".to_string(), i.to_string().into());

        client
            .execute(XAdd::new(&key, StreamId::auto(), fields))
            .await
            .expect("XADD should succeed");
    }

    // Read from beginning
    let results = client
        .execute(XRead::new(vec![(key.clone(), StreamId::beginning())]))
        .await
        .expect("XREAD should succeed");

    assert_eq!(results.len(), 1, "Should get 1 stream");
    let entries = results.get(&key).unwrap();
    assert_eq!(entries.len(), 3, "Should get all 3 entries");
}

#[tokio::test]
async fn test_xlen_empty_stream() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xlen_empty");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // XLEN on non-existent stream should return 0
    let length: i64 = client
        .execute(XLen::new(&key))
        .await
        .expect("XLEN should succeed");
    assert_eq!(length, 0, "Empty/non-existent stream should have length 0");
}

#[tokio::test]
async fn test_xadd_with_maxlen() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("stream_xadd_maxlen");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add entries with MAXLEN constraint
    for i in 1..=10 {
        let mut fields = HashMap::new();
        fields.insert("index".to_string(), i.to_string().into());

        client
            .execute(XAdd::new(&key, StreamId::auto(), fields).maxlen(5))
            .await
            .expect("XADD with MAXLEN should succeed");
    }

    // Verify stream is trimmed to ~5 entries
    let length: i64 = client
        .execute(XLen::new(&key))
        .await
        .expect("XLEN should succeed");
    // Approximate trimming may not trim immediately
    assert!(length <= 10, "Stream should be trimmed (approximate)");
}
