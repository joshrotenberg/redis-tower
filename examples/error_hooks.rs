//! Error and reconnection hooks example
//!
//! Demonstrates how to use error and reconnection hooks to monitor
//! connection health and handle errors gracefully.
//!
//! This example shows:
//! - Error callbacks for all Redis errors
//! - Connect callbacks for successful connections
//! - Reconnect attempt callbacks for monitoring retry attempts
//! - Integration with metrics and logging
//!
//! Run with:
//! ```bash
//! cargo run --example error_hooks
//! ```

use redis_tower::client::ResilientRedisClient;
use redis_tower::commands::{Get, Incr, Set};
use redis_tower::config::ClientConfig;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tower::ServiceExt;
use tower_resilience::reconnect::{ReconnectConfig, ReconnectPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for better visibility
    tracing_subscriber::fmt()
        .with_env_filter("redis_tower=info")
        .init();

    println!("=== Redis Error and Reconnection Hooks Example ===\n");

    // Shared counters for tracking events
    let error_count = Arc::new(AtomicUsize::new(0));
    let connect_count = Arc::new(AtomicUsize::new(0));
    let reconnect_attempt_count = Arc::new(AtomicUsize::new(0));

    // Clone counters for use in closures
    let error_count_clone = error_count.clone();
    let connect_count_clone = connect_count.clone();
    let reconnect_attempt_count_clone = reconnect_attempt_count.clone();

    // Create configuration with hooks
    let config = ClientConfig::builder()
        .reconnect(
            ReconnectConfig::builder()
                .policy(ReconnectPolicy::exponential(
                    Duration::from_millis(100),
                    Duration::from_secs(2),
                ))
                .max_attempts(5)
                .build(),
        )
        .on_error(move |error| {
            let count = error_count_clone.clone();
            async move {
                let n = count.fetch_add(1, Ordering::SeqCst) + 1;
                eprintln!("❌ Error #{}: {:?}", n, error);
            }
        })
        .on_connect(move |attempt| {
            let count = connect_count_clone.clone();
            async move {
                let n = count.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt == 1 {
                    println!("✅ Initial connection #{} established", n);
                } else {
                    println!("✅ Reconnected #{} after {} attempts", n, attempt);
                }
            }
        })
        .on_reconnect_attempt(move |attempt| {
            let count = reconnect_attempt_count_clone.clone();
            async move {
                let n = count.fetch_add(1, Ordering::SeqCst) + 1;
                println!("🔄 Reconnection attempt #{} (retry #{})", n, attempt);
            }
        })
        .build();

    println!("Connecting to Redis with error hooks enabled...\n");

    // Connect with hooks
    let client = ResilientRedisClient::connect_with_full_config("localhost:6379", config).await?;

    println!("Connected! Now performing normal operations...\n");

    // Perform some successful operations
    println!("=== Normal Operations ===");
    client.clone().oneshot(Set::new("counter", "0")).await?;
    println!("Set counter = 0");

    for i in 1..=3 {
        let count: i64 = client.clone().oneshot(Incr::new("counter")).await?;
        println!("Incremented counter to {}", count);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let value: Option<String> = client.clone().oneshot(Get::new("counter")).await?;
    println!("Final counter value: {:?}\n", value);

    // Try to access a non-existent key (not an error, just returns None)
    println!("=== Accessing Non-Existent Key ===");
    let missing: Option<String> = client.clone().oneshot(Get::new("nonexistent")).await?;
    println!("Non-existent key: {:?}\n", missing);

    // Simulate connection issues by connecting to a bad address
    println!("=== Simulating Connection Errors ===");
    println!("Attempting to connect to invalid address...");

    let bad_config = ClientConfig::builder()
        .reconnect(
            ReconnectConfig::builder()
                .policy(ReconnectPolicy::fixed(Duration::from_millis(100)))
                .max_attempts(3)
                .build(),
        )
        .on_error(|error| async move {
            eprintln!("⚠️  Connection error: {:?}", error);
        })
        .on_connect(|attempt| async move {
            println!("✅ Connected on attempt {}", attempt);
        })
        .on_reconnect_attempt(|attempt| async move {
            println!("🔄 Attempting reconnect #{}", attempt);
        })
        .build();

    match ResilientRedisClient::connect_with_full_config("localhost:9999", bad_config).await {
        Ok(_) => println!("Unexpectedly connected!"),
        Err(e) => println!("Expected connection failure: {}\n", e),
    }

    // Print statistics
    println!("=== Event Statistics ===");
    println!("Total errors: {}", error_count.load(Ordering::SeqCst));
    println!(
        "Total connections: {}",
        connect_count.load(Ordering::SeqCst)
    );
    println!(
        "Total reconnect attempts: {}",
        reconnect_attempt_count.load(Ordering::SeqCst)
    );

    println!("\n=== Advanced Usage ===");
    println!("Hooks can be used to:");
    println!("- Send metrics to monitoring systems (Prometheus, DataDog, etc.)");
    println!("- Trigger alerts when error rates exceed thresholds");
    println!("- Log errors to centralized logging systems");
    println!("- Implement custom circuit breakers or rate limiters");
    println!("- Re-authenticate or re-subscribe after reconnection");
    println!("- Update application state based on connection health");

    Ok(())
}
