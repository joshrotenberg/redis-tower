//! MONITOR streaming example
//!
//! Demonstrates how to use the MONITOR command to stream all Redis commands
//! in real-time. This is useful for debugging and understanding Redis traffic.
//!
//! WARNING: MONITOR has performance implications - it should only be used
//! in development/debugging, not production.
//!
//! Run with:
//! ```bash
//! cargo run --example monitor_streaming
//! ```
//!
//! In another terminal, run Redis commands to see them appear in the stream:
//! ```bash
//! redis-cli SET mykey "hello"
//! redis-cli GET mykey
//! redis-cli INCR counter
//! ```

use redis_tower::RedisClient;
use redis_tower::monitor::MonitorStream;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for better visibility
    tracing_subscriber::fmt()
        .with_env_filter("redis_tower=debug")
        .init();

    println!("=== Redis MONITOR Streaming Example ===\n");

    // Connect to Redis
    let connection = RedisClient::connect("redis://localhost:6379").await?;

    println!("Starting MONITOR stream...");
    println!("Run Redis commands in another terminal to see them appear here.");
    println!("Press Ctrl+C to stop.\n");

    // Create a MONITOR stream
    let mut stream = MonitorStream::new(connection.into_inner()).await?;

    // Read events for 30 seconds, then exit
    // In a real application, you'd run this until interrupted
    let result = timeout(Duration::from_secs(30), async {
        let mut event_count = 0;

        while let Some(event_result) = stream.next().await {
            match event_result {
                Ok(event) => {
                    event_count += 1;

                    // Format the event nicely
                    println!(
                        "[{}] DB{} {} - {} {}",
                        event.timestamp,
                        event.database,
                        event.client_address,
                        event.command,
                        event.args.join(" ")
                    );

                    // Also show the raw line for reference
                    println!("  └─ Raw: {}", event.raw);

                    println!();
                }
                Err(e) => {
                    eprintln!("Error reading MONITOR event: {}", e);
                    break;
                }
            }
        }

        println!("\nReceived {} events total", event_count);
    })
    .await;

    match result {
        Ok(_) => println!("Stream ended normally"),
        Err(_) => println!("Timeout after 30 seconds - stream closed"),
    }

    Ok(())
}
