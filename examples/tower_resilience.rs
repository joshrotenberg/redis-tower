//! Tower middleware resilience patterns with redis-tower
//!
//! This example demonstrates how to use tower-resilience middleware
//! to add production-grade resilience to Redis clients.
//!
//! Run with: cargo run --example tower_resilience

use redis_tower::client::RedisClient;
use redis_tower::commands::{Get, Incr, Set};
use std::time::Duration;
use tower::ServiceBuilder;
use tower::ServiceExt;
use tower_resilience::{
    CircuitBreakerLayer, ExponentialBackoff, RateLimitLayer, RetryLayer, TimeoutLayer,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing to see what's happening
    tracing_subscriber::fmt()
        .with_env_filter("redis_tower=debug,tower_resilience=info")
        .init();

    println!("=== Tower Resilience Patterns ===\n");

    // Setup: Connect to Redis
    let client = RedisClient::connect("127.0.0.1:6379").await?;
    client.call(Set::new("demo:key", "initial_value")).await?;
    println!("✓ Connected to Redis\n");

    // 1. Timeout Pattern
    println!("1. Timeout Pattern");
    println!("   Prevents requests from hanging indefinitely\n");
    {
        let mut service = ServiceBuilder::new()
            .layer(TimeoutLayer::new(Duration::from_secs(5)))
            .service(client.clone());

        let value: Option<bytes::Bytes> = service.call(Get::new("demo:key")).await?;
        println!(
            "   Value: {:?}",
            value.map(|b| String::from_utf8_lossy(&b).to_string())
        );
        println!("   (Would timeout after 5 seconds if Redis didn't respond)\n");
    }

    // 2. Retry Pattern with Exponential Backoff
    println!("2. Retry Pattern with Exponential Backoff");
    println!("   Automatically retries failed requests with increasing delays\n");
    {
        let mut service = ServiceBuilder::new()
            .layer(
                RetryLayer::builder()
                    .max_attempts(3)
                    .backoff(ExponentialBackoff::new(Duration::from_millis(100)))
                    .on_retry(|attempt, delay| {
                        println!("   ⚠️  Retry attempt {} after {:?}", attempt, delay);
                    })
                    .build(),
            )
            .service(client.clone());

        let count: i64 = service.call(Incr::new("demo:counter")).await?;
        println!("   Counter: {}", count);
        println!("   (Would retry up to 3 times: 100ms → 200ms → 400ms)\n");
    }

    // 3. Circuit Breaker Pattern
    println!("3. Circuit Breaker Pattern");
    println!("   Prevents cascading failures by failing fast when error rate is high\n");
    {
        let mut service = ServiceBuilder::new()
            .layer(
                CircuitBreakerLayer::builder()
                    .failure_rate_threshold(0.5) // Open at 50% failure rate
                    .sliding_window_size(100) // Over last 100 requests
                    .wait_duration_in_open(Duration::from_secs(30)) // Wait 30s before retry
                    .build(),
            )
            .service(client.clone());

        let value: Option<bytes::Bytes> = service.call(Get::new("demo:key")).await?;
        println!(
            "   Value: {:?}",
            value.map(|b| String::from_utf8_lossy(&b).to_string())
        );
        println!("   Circuit breaker state: CLOSED (normal operation)");
        println!("   (Would open if 50% of requests fail, then reject all requests)\n");
    }

    // 4. Rate Limiting Pattern
    println!("4. Rate Limiting Pattern");
    println!("   Prevents overwhelming Redis with too many requests\n");
    {
        let mut service = ServiceBuilder::new()
            .layer(RateLimitLayer::new(1000, Duration::from_secs(1))) // Max 1000 req/sec
            .service(client.clone());

        // Send a few requests
        for i in 0..5 {
            let count: i64 = service.call(Incr::new("demo:rate_limited")).await?;
            println!("   Request {}: counter = {}", i + 1, count);
        }
        println!("   (Allows up to 1000 requests/second, excess requests wait)\n");
    }

    // 5. Complete Resilience Stack
    println!("5. Complete Resilience Stack");
    println!("   Combining all patterns for production-grade resilience\n");
    {
        let mut service = ServiceBuilder::new()
            // Rate limit to 1000 req/sec
            .layer(RateLimitLayer::new(1000, Duration::from_secs(1)))
            // Timeout after 5 seconds
            .layer(TimeoutLayer::new(Duration::from_secs(5)))
            // Circuit breaker
            .layer(
                CircuitBreakerLayer::builder()
                    .failure_rate_threshold(0.5)
                    .sliding_window_size(100)
                    .wait_duration_in_open(Duration::from_secs(30))
                    .build(),
            )
            // Retry with exponential backoff
            .layer(
                RetryLayer::builder()
                    .max_attempts(3)
                    .backoff(ExponentialBackoff::new(Duration::from_millis(100)))
                    .on_retry(|attempt, delay| {
                        println!("   ⚠️  Retry {} after {:?}", attempt, delay);
                    })
                    .build(),
            )
            .service(client.clone());

        let value: Option<bytes::Bytes> = service.call(Get::new("demo:key")).await?;
        println!(
            "   Value: {:?}",
            value.map(|b| String::from_utf8_lossy(&b).to_string())
        );
        println!("\n   This request was protected by:");
        println!("   ✓ Rate limiting (1000/sec max)");
        println!("   ✓ Timeout (5 second max)");
        println!("   ✓ Circuit breaker (fails fast if error rate > 50%)");
        println!("   ✓ Retry logic (3 attempts with exponential backoff)");
    }

    println!("\n=== Resilience Patterns Demonstrated ===\n");
    println!("All patterns are composable - you can mix and match based on your needs!");
    println!("\nFor production web servers, use:");
    println!("  ConnectionPool (for concurrency)");
    println!("  + ResilientRedisClient (for auto-reconnect)");
    println!("  + Tower layers (for retry, circuit breaking, timeouts)");

    Ok(())
}
