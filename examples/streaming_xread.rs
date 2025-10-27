//! Example demonstrating streaming Redis Streams with XREAD and XREADGROUP
//!
//! This example shows real-time event streaming from Redis Streams, including:
//! - XReadStream for simple continuous reading
//! - XReadGroupStream for distributed consumer groups with auto-ACK
//! - Blocking mode for efficient real-time processing
//! - Producer-consumer patterns
//!
//! Run with: cargo run --example streaming_xread

use redis_tower::RedisClient;
use redis_tower::commands::Del;
use redis_tower::commands::streams::{XAdd, XGroupCreate};
use redis_tower::streaming::{XReadGroupStream, XReadStream};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Redis Streams Streaming API Demo ===\n");

    let client = RedisClient::connect("localhost:6379").await?;
    let stream_key = "demo:events";

    // Clean up
    let _ = client.call(Del::new(vec![stream_key.to_string()])).await;

    // === Part 1: Basic XReadStream ===
    println!("1. XReadStream - Reading from beginning\n");

    // Add some initial entries
    for i in 1..=5 {
        client
            .call(
                XAdd::new(stream_key)
                    .field("event", format!("event-{}", i))
                    .field("value", i.to_string()),
            )
            .await?;
    }

    // Read all entries from the beginning
    let mut stream = XReadStream::new(client.clone(), stream_key)
        .start_from("0-0") // Start from beginning
        .count(2); // Read 2 at a time

    println!("Reading initial entries (batch size 2):");
    let mut total = 0;
    while let Some(entries) = stream.next().await? {
        for entry in &entries {
            println!("  ID: {}, Event: {:?}", entry.id, entry.fields.get("event"));
            total += 1;
        }
        if total >= 5 {
            break; // We know there are 5 entries
        }
    }
    println!("Last seen ID: {}\n", stream.last_id());

    // === Part 2: Real-time streaming with blocking ===
    println!("2. XReadStream - Real-time with blocking\n");

    // Spawn a producer task
    let producer_client = client.clone();
    let producer_stream = stream_key.to_string();
    tokio::spawn(async move {
        sleep(Duration::from_millis(500)).await;
        for i in 6..=10 {
            let _ = producer_client
                .call(
                    XAdd::new(&producer_stream)
                        .field("event", format!("realtime-{}", i))
                        .field("value", i.to_string()),
                )
                .await;
            sleep(Duration::from_millis(300)).await;
        }
    });

    // Stream with blocking - waits for new entries
    let mut stream = XReadStream::new(client.clone(), stream_key)
        .start_from("$") // Only new entries from now
        .block(2000); // Block for 2 seconds

    println!("Streaming real-time entries (blocking mode):");
    let mut count = 0;
    while count < 5 {
        if let Some(entries) = stream.next().await? {
            for entry in entries {
                println!(
                    "  New entry ID: {}, Event: {:?}",
                    entry.id,
                    entry.fields.get("event")
                );
                count += 1;
            }
        } else {
            println!("  (timeout - no new entries)");
            break;
        }
    }
    println!();

    // === Part 3: Consumer Groups with XReadGroupStream ===
    println!("3. XReadGroupStream - Consumer groups with auto-ACK\n");

    // Clean up and create new stream
    let group_stream = "demo:orders";
    let _ = client.call(Del::new(vec![group_stream.to_string()])).await;

    // Create consumer group
    client
        .call(
            XGroupCreate::new(group_stream, "processors")
                .id("$")
                .mkstream(),
        )
        .await?;
    println!("Created consumer group 'processors'");

    // Spawn producer for orders
    let producer_client = client.clone();
    let producer_stream = group_stream.to_string();
    tokio::spawn(async move {
        for i in 1..=8 {
            let _ = producer_client
                .call(
                    XAdd::new(&producer_stream)
                        .field("order_id", format!("ORDER-{:04}", i))
                        .field("amount", (i * 100).to_string())
                        .field("status", "pending"),
                )
                .await;
            sleep(Duration::from_millis(200)).await;
        }
    });

    // Consumer 1 - processes orders with auto-ACK
    let mut consumer1 =
        XReadGroupStream::new(client.clone(), group_stream, "processors", "worker-1")
            .count(2)
            .block(3000)
            .auto_ack(true); // Automatically acknowledge after receiving

    println!("\nConsumer 'worker-1' processing orders:");
    let mut processed = 0;
    while processed < 4 {
        if let Some(entries) = consumer1.next().await? {
            for entry in entries {
                println!(
                    "  [worker-1] Processing order: {:?}",
                    entry.fields.get("order_id")
                );
                processed += 1;
            }
        } else {
            break;
        }
    }

    // Consumer 2 - different worker in same group gets different messages
    let mut consumer2 =
        XReadGroupStream::new(client.clone(), group_stream, "processors", "worker-2")
            .count(2)
            .block(2000)
            .auto_ack(true);

    println!("\nConsumer 'worker-2' processing remaining orders:");
    let mut processed = 0;
    while processed < 4 {
        if let Some(entries) = consumer2.next().await? {
            for entry in entries {
                println!(
                    "  [worker-2] Processing order: {:?}",
                    entry.fields.get("order_id")
                );
                processed += 1;
            }
        } else {
            break;
        }
    }

    println!();

    // === Part 4: NOACK mode for fire-and-forget ===
    println!("4. XReadGroupStream - NOACK mode (no delivery guarantees)\n");

    let logs_stream = "demo:logs";
    let _ = client.call(Del::new(vec![logs_stream.to_string()])).await;

    client
        .call(
            XGroupCreate::new(logs_stream, "log_processors")
                .id("$")
                .mkstream(),
        )
        .await?;

    // Add some log entries
    for level in &["INFO", "WARN", "ERROR", "DEBUG"] {
        client
            .call(
                XAdd::new(logs_stream)
                    .field("level", *level)
                    .field("message", format!("This is a {} message", level)),
            )
            .await?;
    }

    // NOACK mode - no pending list, better performance
    let mut log_consumer =
        XReadGroupStream::new(client.clone(), logs_stream, "log_processors", "logger-1")
            .start_from("0-0")
            .noack(); // Don't track delivery - fire and forget

    println!("Processing logs with NOACK (no delivery tracking):");
    if let Some(entries) = log_consumer.next().await? {
        for entry in entries {
            println!(
                "  [{}] {}",
                entry.fields.get("level").unwrap_or(&"UNKNOWN".to_string()),
                entry.fields.get("message").unwrap_or(&"".to_string())
            );
        }
    }
    println!();

    // === Part 5: Reset and replay ===
    println!("5. Stream Reset - Replaying from specific ID\n");

    let mut replay_stream = XReadStream::new(client.clone(), stream_key)
        .start_from("0-0")
        .count(3);

    println!("First read (first 3 entries):");
    if let Some(entries) = replay_stream.next().await? {
        for entry in &entries {
            println!("  ID: {}", entry.id);
        }
        println!("Last ID: {}", replay_stream.last_id());
    }

    println!("\nResetting to beginning and reading again:");
    replay_stream.reset("0-0");
    if let Some(entries) = replay_stream.next().await? {
        for entry in &entries {
            println!("  ID: {}", entry.id);
        }
    }
    println!();

    // Clean up
    let _ = client.call(Del::new(vec![stream_key.to_string()])).await;
    let _ = client.call(Del::new(vec![group_stream.to_string()])).await;
    let _ = client.call(Del::new(vec![logs_stream.to_string()])).await;

    println!("=== Demo Complete ===");
    println!("\nKey Takeaways:");
    println!("- XReadStream: Simple streaming with automatic ID tracking");
    println!("- XReadGroupStream: Distributed processing with consumer groups");
    println!("- Blocking mode: Efficient real-time event processing");
    println!("- Auto-ACK: Convenient automatic acknowledgment");
    println!("- NOACK: High-performance fire-and-forget mode");

    Ok(())
}
