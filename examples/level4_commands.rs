//! Level 4 Commands Example - Stateful/Blocking Operations
//!
//! This example demonstrates redis-tower's most complex command patterns:
//! - BLPOP/BRPOP: Blocking list operations with timeout
//! - XADD: Stream writes with auto-generated IDs
//! - XREAD: Stream reads (non-blocking and blocking)
//!
//! Level 4 characteristics:
//! 1. Commands that block the connection
//! 2. Timeout handling (None on timeout)
//! 3. Complex nested response structures
//! 4. State management across operations
//!
//! Prerequisites:
//! - Redis server running on localhost:6379
//!
//! Run with: cargo run --example level4_commands

use redis_tower::RedisClient;
use redis_tower::commands::{lists, streams, strings};
use std::collections::HashMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("Redis-Tower: Level 4 Commands - Blocking & Streams\n");

    let client = RedisClient::connect("localhost:6379").await?;

    // ========== Part 1: Blocking List Operations ==========
    println!("=== Part 1: BLPOP/BRPOP - Blocking List Operations ===\n");

    println!("Pattern: Blocking commands hold connection until:");
    println!("  - Data becomes available, OR");
    println!("  - Timeout is reached\n");

    // Setup: Create a producer task
    println!("1. Testing BLPOP with timeout:");
    println!("   Starting background producer task...");

    let client_clone = client.clone();
    let producer = tokio::spawn(async move {
        // Wait a bit, then push data
        tokio::time::sleep(Duration::from_secs(2)).await;
        let _len: i64 = client_clone
            .call(lists::RPush::single("work_queue", "job1"))
            .await
            .unwrap();
        println!("   [Producer] Pushed job1 after 2 seconds");

        tokio::time::sleep(Duration::from_secs(2)).await;
        let _len: i64 = client_clone
            .call(lists::RPush::single("work_queue", "job2"))
            .await
            .unwrap();
        println!("   [Producer] Pushed job2 after 4 seconds");
    });

    // Consumer: Block waiting for data
    println!("   [Consumer] Blocking for up to 5 seconds...");
    let start = std::time::Instant::now();

    let result = client
        .call(lists::BLPop::new(vec!["work_queue".to_string()], 5))
        .await?;

    match result {
        Some((key, value)) => {
            let elapsed = start.elapsed();
            println!(
                "   [Consumer] Got '{}' from '{}' after {:.1}s",
                String::from_utf8_lossy(&value),
                String::from_utf8_lossy(&key),
                elapsed.as_secs_f32()
            );
        }
        None => println!("   [Consumer] Timeout - no data received"),
    }

    // Get second item (should be faster since producer already pushed it)
    println!("\n   [Consumer] Blocking again for second item...");
    let result2 = client
        .call(lists::BLPop::new(vec!["work_queue".to_string()], 5))
        .await?;

    if let Some((_, value)) = result2 {
        println!("   [Consumer] Got '{}'", String::from_utf8_lossy(&value));
    }

    producer.await?;

    // Test timeout scenario
    println!("\n2. Testing timeout (no data available):");
    println!("   Blocking for 2 seconds on empty queue...");
    let timeout_start = std::time::Instant::now();

    let timeout_result = client
        .call(lists::BLPop::new(vec!["empty_queue".to_string()], 2))
        .await?;

    let elapsed = timeout_start.elapsed();
    match timeout_result {
        Some(_) => println!("   Unexpected data!"),
        None => println!(
            "   Timeout after {:.1}s (expected)\n",
            elapsed.as_secs_f32()
        ),
    }

    // ========== Part 2: Redis Streams ==========
    println!("=== Part 2: Redis Streams - XADD & XREAD ===\n");

    println!("Pattern: Streams are append-only logs with:");
    println!("  - Auto-generated IDs (timestamp-sequence)");
    println!("  - Nested structure (stream -> entries -> fields)");
    println!("  - Optional blocking reads\n");

    // XADD - Write to stream
    println!("1. XADD - Writing events to stream:");

    let mut event1 = HashMap::new();
    event1.insert("type".to_string(), "temperature".into());
    event1.insert("sensor".to_string(), "living_room".into());
    event1.insert("value".to_string(), "22.5".into());

    let id1 = client
        .call(streams::XAdd::new(
            "sensor_events",
            streams::StreamId::auto(),
            event1,
        ))
        .await?;
    println!("   Added event with ID: {}", id1);

    let mut event2 = HashMap::new();
    event2.insert("type".to_string(), "temperature".into());
    event2.insert("sensor".to_string(), "bedroom".into());
    event2.insert("value".to_string(), "21.0".into());

    let id2 = client
        .call(streams::XAdd::new(
            "sensor_events",
            streams::StreamId::auto(),
            event2,
        ))
        .await?;
    println!("   Added event with ID: {}", id2);

    let mut event3 = HashMap::new();
    event3.insert("type".to_string(), "motion".into());
    event3.insert("sensor".to_string(), "front_door".into());
    event3.insert("detected".to_string(), "true".into());

    let id3 = client
        .call(streams::XAdd::new(
            "sensor_events",
            streams::StreamId::auto(),
            event3,
        ))
        .await?;
    println!("   Added event with ID: {}\n", id3);

    // XREAD - Read from stream (non-blocking)
    println!("2. XREAD - Reading from stream (non-blocking):");

    let streams_to_read = vec![("sensor_events".to_string(), streams::StreamId::beginning())];

    let results = client.call(streams::XRead::new(streams_to_read)).await?;

    for (stream_name, entries) in &results {
        println!("   Stream '{}': {} entries", stream_name, entries.len());
        for entry in entries {
            println!("     ID: {}", entry.id);
            for (field, value) in &entry.fields {
                println!("       {}: {}", field, String::from_utf8_lossy(value));
            }
        }
    }

    // XREAD with COUNT
    println!("\n3. XREAD with COUNT (limit results):");
    let streams_limited = vec![("sensor_events".to_string(), streams::StreamId::beginning())];

    let limited_results = client
        .call(streams::XRead::new(streams_limited).count(2))
        .await?;

    for (stream_name, entries) in &limited_results {
        println!(
            "   Stream '{}': {} entries (limited)",
            stream_name,
            entries.len()
        );
        for entry in entries {
            let sensor = entry
                .fields
                .get("sensor")
                .map(|v| String::from_utf8_lossy(v));
            println!("     ID: {}, sensor: {:?}", entry.id, sensor);
        }
    }

    // XREAD blocking (with timeout)
    println!("\n4. XREAD with BLOCK (blocking read):");
    println!("   Blocking for 3 seconds, waiting for new events...");

    // Start from latest (only new events)
    let latest_streams = vec![("sensor_events".to_string(), streams::StreamId::latest())];

    // Spawn producer to add event after delay
    let client_clone2 = client.clone();
    let stream_producer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut new_event = HashMap::new();
        new_event.insert("type".to_string(), "alert".into());
        new_event.insert("message".to_string(), "Battery low".into());

        let id = client_clone2
            .call(streams::XAdd::new(
                "sensor_events",
                streams::StreamId::auto(),
                new_event,
            ))
            .await
            .unwrap();
        println!("   [Producer] Added new event: {}", id);
    });

    let blocking_results = client
        .call(streams::XRead::new(latest_streams).block(3000))
        .await?;

    if blocking_results.is_empty() {
        println!("   No new events (timeout)");
    } else {
        for (stream_name, entries) in &blocking_results {
            println!(
                "   Received {} new entries from '{}':",
                entries.len(),
                stream_name
            );
            for entry in entries {
                println!("     ID: {}", entry.id);
                for (field, value) in &entry.fields {
                    println!("       {}: {}", field, String::from_utf8_lossy(value));
                }
            }
        }
    }

    stream_producer.await?;

    // ========== Cleanup ==========
    println!("\n=== Cleanup ===");
    let deleted: i64 = client
        .call(strings::Del::new(vec![
            "work_queue".to_string(),
            "empty_queue".to_string(),
            "sensor_events".to_string(),
        ]))
        .await?;
    println!("Deleted {} keys\n", deleted);

    // ========== Summary ==========
    println!("=== Level 4 Patterns Demonstrated ===\n");

    println!("Blocking Operations:");
    println!("  ✓ BLPOP with timeout (returns None on timeout)");
    println!("  ✓ Connection held until data or timeout");
    println!("  ✓ Background producer coordination\n");

    println!("Stream Operations:");
    println!("  ✓ XADD with auto-generated IDs");
    println!("  ✓ XREAD with complex nested responses");
    println!("  ✓ HashMap<String, Vec<StreamEntry>> structure");
    println!("  ✓ Blocking reads with BLOCK option\n");

    println!("Type Safety:");
    println!("  ✓ BlockingPopResult = Option<(Bytes, Bytes)>");
    println!("  ✓ XReadResult = HashMap<String, Vec<StreamEntry>>");
    println!("  ✓ StreamEntry {{ id, fields: HashMap<String, Bytes> }}\n");

    println!("These patterns enable:");
    println!("  • Work queue patterns with blocking consumers");
    println!("  • Event sourcing with append-only logs");
    println!("  • Real-time data processing");
    println!("  • Complex coordination across async tasks");

    Ok(())
}
