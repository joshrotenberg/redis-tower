//! Basic Sentinel example
//!
//! This example demonstrates how to use redis-tower with Redis Sentinel
//! for automatic master discovery and failover.
//!
//! To run this example:
//! 1. Set up a Redis Sentinel cluster (see SENTINEL_DESIGN.md for setup instructions)
//! 2. Run: cargo run --example sentinel_basic

use redis_tower::commands::{Get, Incr, Set};
use redis_tower::sentinel::{SentinelClient, SentinelConfig};
use tower::ServiceExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Configure Sentinel client with multiple sentinel nodes
    //
    // Example 1: No authentication (default setup)
    let config = SentinelConfig::builder()
        .sentinel_node("localhost", 26379)
        .sentinel_node("localhost", 26380)
        .sentinel_node("localhost", 26381)
        .master_name("mymaster")
        .build()?;

    // Example 2: With simple password authentication (pre-ACL)
    // Uncomment to use:
    /*
    let config = SentinelConfig::builder()
        .sentinel_node("localhost", 26379)
        .sentinel_node("localhost", 26380)
        .sentinel_node("localhost", 26381)
        .master_name("mymaster")
        .sentinel_password("sentinel-password")  // Simple AUTH for Sentinel nodes
        .redis_password("redis-password")         // Simple AUTH for Redis nodes
        .build()?;
    */

    // Example 3: With ACL authentication (Redis 6.2+)
    // Uncomment to use:
    /*
    let config = SentinelConfig::builder()
        .sentinel_node("localhost", 26379)
        .sentinel_node("localhost", 26380)
        .sentinel_node("localhost", 26381)
        .master_name("mymaster")
        .sentinel_username("sentinel-user")       // ACL username for Sentinel nodes
        .sentinel_password("sentinel-password")   // ACL password for Sentinel nodes
        .redis_username("redis-user")             // ACL username for Redis nodes
        .redis_password("redis-password")         // ACL password for Redis nodes
        .build()?;
    */

    println!("Creating Sentinel client...");
    let client = SentinelClient::new(config);

    // Get a master connection with automatic failover
    // Tower's Reconnect middleware will automatically reconnect to the new master
    // if a failover occurs
    let master = client.master();

    // Execute SET command using oneshot (handles poll_ready automatically)
    println!("Setting key 'greeting' to 'Hello from Sentinel'");
    master
        .oneshot(Set::new("greeting", "Hello from Sentinel"))
        .await
        .map_err(|e| format!("Failed to set key: {}", e))?;

    // Get another master connection for next command
    let master = client.master();

    // Execute GET command
    println!("Getting key 'greeting'");
    let value: Option<bytes::Bytes> = master
        .oneshot(Get::new("greeting"))
        .await
        .map_err(|e| format!("Failed to get key: {}", e))?;

    match value {
        Some(ref v) => println!("Value: {}", String::from_utf8_lossy(v)),
        None => println!("Key not found"),
    }

    // Increment a counter
    println!("\nTesting counter increment...");

    let master = client.master();
    master
        .oneshot(Set::new("counter", "0"))
        .await
        .map_err(|e| format!("Failed to set counter: {}", e))?;

    for i in 1..=5 {
        let master = client.master();
        let count: i64 = master
            .oneshot(Incr::new("counter"))
            .await
            .map_err(|e| format!("Failed to increment counter: {}", e))?;
        println!("Counter incremented to: {}", count);
        assert_eq!(count, i);
    }

    println!("\nSentinel basic example completed successfully!");
    println!("The connection will automatically reconnect to a new master if failover occurs.");

    Ok(())
}
