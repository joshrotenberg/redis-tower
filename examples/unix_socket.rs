//! Example demonstrating Unix domain socket connections
//!
//! This example shows how to connect to Redis using Unix domain sockets
//! instead of TCP. Unix sockets provide lower latency for local connections.
//!
//! # Prerequisites
//!
//! Configure Redis to listen on a Unix socket by adding to redis.conf:
//! ```
//! unixsocket /tmp/redis.sock
//! unixsocketperm 777
//! ```
//!
//! Then restart Redis.
//!
//! # Run this example
//! ```bash
//! cargo run --example unix_socket
//! ```

use redis_tower::RedisClient;
use redis_tower::commands::strings::{Get, Incr, Set};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Redis Tower - Unix Socket Example\n");

    // Method 1: Connect with unix:// URL scheme
    println!("1. Connecting with unix:// URL...");
    let client = RedisClient::connect("unix:///tmp/redis.sock").await?;
    println!("   ✓ Connected via unix:// URL\n");

    // Method 2: Connect with direct path (alternative)
    println!("2. Connecting with direct path...");
    let client2 = RedisClient::connect("/tmp/redis.sock").await?;
    println!("   ✓ Connected via direct path\n");

    // Perform basic operations
    println!("3. Performing basic operations...");

    // SET a key
    client
        .call(Set::new("greeting", "Hello from Unix socket!"))
        .await?;
    println!("   ✓ SET greeting = 'Hello from Unix socket!'");

    // GET the key
    let value: Option<bytes::Bytes> = client.call(Get::new("greeting")).await?;
    let value_str = value.map(|b| String::from_utf8_lossy(&b).to_string());
    println!("   ✓ GET greeting = {:?}", value_str);

    // Increment a counter
    let count: i64 = client.call(Incr::new("counter")).await?;
    println!("   ✓ INCR counter = {}", count);

    // Verify from second client
    let count2: Option<bytes::Bytes> = client2.call(Get::new("counter")).await?;
    let count2_str = count2.map(|b| String::from_utf8_lossy(&b).to_string());
    println!("   ✓ GET counter from client2 = {:?}\n", count2_str);

    // Performance comparison note
    println!("4. Performance Benefits:");
    println!("   - Lower latency than TCP (no network stack)");
    println!("   - No TCP handshake overhead");
    println!("   - Perfect for sidecar/same-host deployments");
    println!("   - Common in production for co-located services\n");

    // Use case examples
    println!("5. Common Use Cases:");
    println!("   - Application and Redis on same host");
    println!("   - Kubernetes sidecar containers");
    println!("   - Docker Compose local development");
    println!("   - High-performance cache scenarios");

    Ok(())
}
