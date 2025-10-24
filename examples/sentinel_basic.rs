//! Basic Sentinel example
//!
//! This example demonstrates how to use redis-tower with Redis Sentinel
//! for automatic master discovery and failover.
//!
//! To run this example:
//! 1. Set up a Redis Sentinel cluster (see SENTINEL_DESIGN.md for setup instructions)
//! 2. Run: cargo run --example sentinel_basic

use redis_tower::commands::{Get, Set};
use redis_tower::sentinel::{SentinelClient, SentinelConfig};
use tower::ServiceExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Configure Sentinel client with multiple sentinel nodes
    let config = SentinelConfig::builder()
        .sentinel_node("localhost", 26379)
        .sentinel_node("localhost", 26380)
        .sentinel_node("localhost", 26381)
        .master_name("mymaster")
        .build()?;

    println!("Creating Sentinel client...");
    let client = SentinelClient::new(config);

    // Get a master connection with automatic failover
    // Tower's Reconnect middleware will automatically reconnect to the new master
    // if a failover occurs
    let mut master = client.master();

    println!("Waiting for master connection to be ready...");
    master.ready().await?;

    // Execute SET command
    println!("Setting key 'greeting' to 'Hello from Sentinel'");
    master
        .call(Set::new("greeting", "Hello from Sentinel"))
        .await?;

    // Execute GET command
    println!("Getting key 'greeting'");
    let value: Option<bytes::Bytes> = master.call(Get::new("greeting")).await?;

    match value {
        Some(v) => println!("Value: {}", String::from_utf8_lossy(&v)),
        None => println!("Key not found"),
    }

    // Increment a counter
    println!("\nTesting counter increment...");
    use redis_tower::commands::Incr;

    master.call(Set::new("counter", "0")).await?;

    for i in 1..=5 {
        let count: i64 = master.call(Incr::new("counter")).await?;
        println!("Counter incremented to: {}", count);
        assert_eq!(count, i);
    }

    println!("\nSentinel basic example completed successfully!");
    println!("The connection will automatically reconnect to a new master if failover occurs.");

    Ok(())
}
