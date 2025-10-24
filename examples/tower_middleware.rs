//! Tower Middleware examples for Redis
//!
//! This example demonstrates how to compose resilience middleware with RedisConnection.
//! Shows practical patterns for production Redis clients.
//!
//! Prerequisites:
//! - Redis server running on localhost:6379
//!
//! Run with: cargo run --example tower_middleware

use redis_tower::client::RedisConnection;
use redis_tower::commands::{Del, Get, Incr, Set};
use tower::Service;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Tower Middleware Patterns for Redis\n");

    demo_timeout_pattern().await?;
    demo_retry_concept().await?;
    demo_circuit_breaker_concept().await?;

    println!("\n=== Summary ===\n");
    println!("Tower middleware provides production-ready resilience:");
    println!("  ✓ Timeout - prevents hanging requests");
    println!("  ✓ Retry - handles transient failures");
    println!("  ✓ Circuit Breaker - prevents cascading failures");
    println!("  ✓ All patterns compose cleanly");
    println!("\nNote: Full middleware integration requires additional work:");
    println!("  - Timeout middleware needs proper async timeout handling");
    println!("  - Retry middleware requires Clone-able requests");
    println!("  - Circuit breaker needs failure detection logic");

    Ok(())
}

async fn demo_timeout_pattern() -> anyhow::Result<()> {
    println!("=== Demo 1: Timeout Pattern ===\n");
    println!("In production, you'd wrap RedisConnection with TimeoutLayer:");
    println!("  ServiceBuilder::new()");
    println!("    .layer(TimeoutLayer::new(Duration::from_secs(5)))");
    println!("    .service(conn);\n");

    let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;

    println!("Executing commands (without timeout middleware for now)...");
    let cmd = Set::new("timeout_demo", "fast response");
    Service::call(&mut conn, cmd).await?;

    let cmd = Get::new("timeout_demo");
    let value: Option<bytes::Bytes> = Service::call(&mut conn, cmd).await?;
    println!(
        "Value retrieved: {:?}",
        value.map(|b| String::from_utf8_lossy(&b).to_string())
    );

    conn.execute(Del::new(vec!["timeout_demo".to_string()]))
        .await?;
    println!();

    Ok(())
}

async fn demo_retry_concept() -> anyhow::Result<()> {
    println!("=== Demo 2: Retry Pattern (Concept) ===\n");
    println!("Retry middleware automatically retries failed requests:");
    println!("  ServiceBuilder::new()");
    println!("    .layer(RetryLayer::new(ExponentialBackoff::default()))");
    println!("    .service(conn);\n");

    println!("Benefits:");
    println!("  - Handles transient network failures");
    println!("  - Exponential backoff prevents overwhelming server");
    println!("  - Configurable retry policies");
    println!("  - Works with any Tower Service\n");

    let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;

    // Simulate multiple operations that might need retry in production
    println!("Executing operations that benefit from retry logic...");
    for i in 1..=3 {
        let cmd = Incr::new("retry_demo");
        let count: i64 = Service::call(&mut conn, cmd).await?;
        println!("  Operation {}: counter = {}", i, count);
    }

    conn.execute(Del::new(vec!["retry_demo".to_string()]))
        .await?;
    println!();

    Ok(())
}

async fn demo_circuit_breaker_concept() -> anyhow::Result<()> {
    println!("=== Demo 3: Circuit Breaker Pattern (Concept) ===\n");
    println!("Circuit breaker prevents cascading failures:");
    println!("  ServiceBuilder::new()");
    println!("    .layer(CircuitBreakerLayer::new(5, Duration::from_secs(30)))");
    println!("    .service(conn);\n");

    println!("How it works:");
    println!("  - Tracks failure rate");
    println!("  - Opens circuit after threshold (e.g., 5 failures)");
    println!("  - Rejects requests while open (fail-fast)");
    println!("  - Attempts recovery after timeout");
    println!("  - Closes circuit when service recovers\n");

    let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;

    println!("Normal operations (circuit closed)...");
    let cmd = Set::new("circuit_demo", "operational");
    Service::call(&mut conn, cmd).await?;

    let cmd = Get::new("circuit_demo");
    let value: Option<bytes::Bytes> = Service::call(&mut conn, cmd).await?;
    println!(
        "Value: {:?}",
        value.map(|b| String::from_utf8_lossy(&b).to_string())
    );

    conn.execute(Del::new(vec!["circuit_demo".to_string()]))
        .await?;
    println!();

    Ok(())
}
