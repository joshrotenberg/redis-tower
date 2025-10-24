//! Basic usage example
//!
//! This example demonstrates the basic usage of redis-tower with strongly typed commands.
//!
//! Prerequisites:
//! - Redis server running on localhost:6379
//!
//! Run with: cargo run --example basic

use redis_tower::RedisClient;
use redis_tower::commands::{Del, Get, Set};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("Connecting to Redis at localhost:6379...");

    // Connect to Redis
    let client = RedisClient::connect("localhost:6379").await?;

    println!("Connected successfully!\n");

    // Demonstrate SET command - strongly typed!
    println!("Setting key 'greeting' to 'Hello, Tower!'");
    client.call(Set::new("greeting", "Hello, Tower!")).await?;
    println!("SET successful\n");

    // Demonstrate GET command - strongly typed response!
    println!("Getting key 'greeting'");
    let value: Option<bytes::Bytes> = client.call(Get::new("greeting")).await?;

    if let Some(bytes) = value {
        let string_value = String::from_utf8_lossy(&bytes);
        println!("GET result: {}\n", string_value);
    } else {
        println!("Key not found\n");
    }

    // Demonstrate GET on non-existent key
    println!("Getting non-existent key 'does_not_exist'");
    let missing: Option<bytes::Bytes> = client.call(Get::new("does_not_exist")).await?;
    println!("GET result: {:?}\n", missing);

    // Demonstrate SET with different value
    println!("Setting key 'counter' to '42'");
    client.call(Set::new("counter", "42")).await?;
    println!("SET successful\n");

    // Get the counter
    println!("Getting key 'counter'");
    let counter_value: Option<bytes::Bytes> = client.call(Get::new("counter")).await?;
    if let Some(bytes) = counter_value {
        let string_value = String::from_utf8_lossy(&bytes);
        println!("Counter value: {}\n", string_value);
    }

    // Demonstrate DEL command
    println!("Deleting keys 'greeting' and 'counter'");
    let deleted_count: i64 = client
        .call(Del::new(vec![
            "greeting".to_string(),
            "counter".to_string(),
        ]))
        .await?;
    println!("Deleted {} keys\n", deleted_count);

    // Verify deletion
    println!("Verifying deletion by getting 'greeting'");
    let after_delete: Option<bytes::Bytes> = client.call(Get::new("greeting")).await?;
    println!("GET after DELETE: {:?}\n", after_delete);

    println!("Example completed successfully!");
    println!("\nKey features demonstrated:");
    println!("  - Strongly typed commands (Get, Set, Del)");
    println!("  - Type-safe responses (Option<Bytes>, i64)");
    println!("  - Clean async API using Tower patterns");

    Ok(())
}
