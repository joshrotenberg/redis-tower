//! Example demonstrating automatic reconnection with ResilientRedisClient
//!
//! This example shows how to use the ResilientRedisClient with configurable
//! automatic reconnection on connection failures.

use redis_tower::ResilientRedisClient;
use redis_tower::commands::{Get, Incr, Set};
use redis_tower::config::ClientConfig;
use std::time::Duration;
use tower_resilience::reconnect::{ReconnectConfig, ReconnectPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("Redis Tower - Resilient Client Example");
    println!("=======================================\n");

    // Example 1: Default reconnection settings
    println!("Example 1: Using default reconnection settings");
    println!("  - Exponential backoff: 100ms -> 5s");
    println!("  - Unlimited retry attempts\n");

    let client = ResilientRedisClient::connect("127.0.0.1:6379").await?;

    // Set a value
    client.call(Set::new("counter", "0")).await?;
    println!("Set counter = 0");

    // Increment
    let value: i64 = client.call(Incr::new("counter")).await?;
    println!("Incremented counter to {}", value);

    // Get the value
    let value: Option<bytes::Bytes> = client.call(Get::new("counter")).await?;
    let value_str = value.map(|b| String::from_utf8_lossy(&b).to_string());
    println!("Retrieved counter = {:?}\n", value_str);

    // Example 2: Custom reconnection configuration
    println!("Example 2: Using custom reconnection configuration");
    println!("  - Exponential backoff: 200ms -> 10s");
    println!("  - Maximum 5 retry attempts\n");

    let reconnect_config = ReconnectConfig::builder()
        .policy(ReconnectPolicy::exponential(
            Duration::from_millis(200),
            Duration::from_secs(10),
        ))
        .max_attempts(5)
        .retry_on_reconnect(true)
        .build();

    let config = ClientConfig::builder().reconnect(reconnect_config).build();

    let resilient_client =
        ResilientRedisClient::connect_with_full_config("127.0.0.1:6379", config).await?;

    // Use the client
    resilient_client
        .call(Set::new("test_key", "test_value"))
        .await?;
    println!("Set test_key = test_value");

    let value: Option<bytes::Bytes> = resilient_client.call(Get::new("test_key")).await?;
    let value_str = value.map(|b| String::from_utf8_lossy(&b).to_string());
    println!("Retrieved test_key = {:?}\n", value_str);

    // Example 3: Fixed interval reconnection
    println!("Example 3: Using fixed interval reconnection");
    println!("  - Fixed delay: 1 second between attempts");
    println!("  - Maximum 3 retry attempts\n");

    let fixed_config = ReconnectConfig::builder()
        .policy(ReconnectPolicy::Fixed(
            tower_resilience::reconnect::FixedInterval::new(Duration::from_secs(1)),
        ))
        .max_attempts(3)
        .build();

    let config = ClientConfig::builder().reconnect(fixed_config).build();

    let fixed_client =
        ResilientRedisClient::connect_with_full_config("127.0.0.1:6379", config).await?;

    fixed_client
        .call(Set::new("fixed_key", "fixed_value"))
        .await?;
    println!("Set fixed_key = fixed_value");

    let value: Option<bytes::Bytes> = fixed_client.call(Get::new("fixed_key")).await?;
    let value_str = value.map(|b| String::from_utf8_lossy(&b).to_string());
    println!("Retrieved fixed_key = {:?}\n", value_str);

    println!("All examples completed successfully!");
    println!("\nNote: If the Redis connection fails, the client will automatically");
    println!("      attempt to reconnect according to the configured policy.");

    Ok(())
}
