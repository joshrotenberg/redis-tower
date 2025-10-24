//! Cluster with Tower Middleware example
//!
//! This example demonstrates using ClusterClient with Tower's middleware layers
//! for resilience (timeouts, retries, circuit breakers).
//!
//! NOTE: Due to the generic nature of the Service trait, each middleware stack
//! is bound to a specific command type. This example shows the pattern for
//! wrapping specific operations with resilience layers.
//!
//! Prerequisites:
//! - Redis cluster running on localhost:7000-7005
//! - Set up with: redis-cli --cluster create 127.0.0.1:7000 ... --cluster-replicas 1
//!
//! Run with: cargo run --example cluster_with_tower

use redis_tower::cluster::ClusterClient;
use redis_tower::commands::{Del, Get, Incr, Set};
use std::time::Duration;
use tower::{Layer, Service};
use tower_resilience::{
    circuitbreaker::CircuitBreakerLayer,
    retry::{ExponentialBackoff, RetryLayer},
    timelimiter::TimeLimiterLayer,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Cluster with Tower Middleware Example\n");

    demo_basic_cluster().await?;
    demo_timeout_pattern().await?;
    demo_retry_pattern().await?;
    demo_full_stack_pattern().await?;

    println!("\nCluster with Tower middleware complete!");
    println!("\nKey benefits:");
    println!("  ✓ ClusterClient implements tower::Service<Cmd>");
    println!("  ✓ Automatic slot-based routing");
    println!("  ✓ MOVED/ASK redirect handling");
    println!("  ✓ Composable middleware (timeout, retry, circuit breaker)");
    println!("  ✓ Type-safe commands with cluster support");

    Ok(())
}

async fn demo_basic_cluster() -> anyhow::Result<()> {
    println!("=== Demo 1: Basic Cluster Client ===\n");

    let seed_nodes = vec![
        "127.0.0.1:7000".to_string(),
        "127.0.0.1:7001".to_string(),
        "127.0.0.1:7002".to_string(),
    ];

    let client = ClusterClient::new(seed_nodes).await?;

    // Set a key
    println!("Setting key in cluster...");
    let _: () = client
        .execute(Set::new("cluster_demo", "Hello Cluster!"))
        .await?;

    // Get the key
    println!("Getting key from cluster...");
    let value: Option<bytes::Bytes> = client.execute(Get::new("cluster_demo")).await?;
    println!(
        "Value: {:?}",
        value.map(|b| String::from_utf8_lossy(&b).to_string())
    );

    // Clean up
    client
        .execute(Del::new(vec!["cluster_demo".to_string()]))
        .await?;
    println!();

    Ok(())
}

async fn demo_timeout_pattern() -> anyhow::Result<()> {
    println!("=== Demo 2: Timeout Pattern ===\n");
    println!("Wrapping critical operations with timeout middleware\n");

    let seed_nodes = vec![
        "127.0.0.1:7000".to_string(),
        "127.0.0.1:7001".to_string(),
        "127.0.0.1:7002".to_string(),
    ];

    let client = ClusterClient::new(seed_nodes).await?;

    // Build timeout layer
    let timeout_layer = TimeLimiterLayer::builder()
        .timeout_duration(Duration::from_secs(5))
        .build();

    // For SET operations with timeout
    println!("SET with 5s timeout...");
    let mut set_service = timeout_layer.layer(client.clone());
    let _: () = set_service
        .call(Set::new("timeout_demo", "Protected!"))
        .await?;

    // For GET operations with timeout
    println!("GET with 5s timeout...");
    let mut get_service = timeout_layer.layer(client.clone());
    let value: Option<bytes::Bytes> = get_service.call(Get::new("timeout_demo")).await?;
    println!(
        "Value: {:?}",
        value.map(|b| String::from_utf8_lossy(&b).to_string())
    );

    // Cleanup without timeout
    client
        .execute(Del::new(vec!["timeout_demo".to_string()]))
        .await?;
    println!();

    Ok(())
}

async fn demo_retry_pattern() -> anyhow::Result<()> {
    println!("=== Demo 3: Retry Pattern ===\n");
    println!("Incrementing counter with automatic retry on transient failures\n");

    let seed_nodes = vec![
        "127.0.0.1:7000".to_string(),
        "127.0.0.1:7001".to_string(),
        "127.0.0.1:7002".to_string(),
    ];

    let client = ClusterClient::new(seed_nodes).await?;

    // Build retry layer with exponential backoff
    let retry_layer = RetryLayer::builder()
        .max_attempts(3)
        .backoff(ExponentialBackoff::new(Duration::from_millis(100)))
        .build();

    // Wrap INCR operations with retry
    let mut incr_service = retry_layer.layer(client.clone());

    println!("Incrementing counter with retry support (3 attempts max)...");
    for i in 1..=3 {
        let count: i64 = incr_service.call(Incr::new("retry_counter")).await?;
        println!("  Iteration {}: counter = {}", i, count);
    }

    // Cleanup
    client
        .execute(Del::new(vec!["retry_counter".to_string()]))
        .await?;
    println!();

    Ok(())
}

async fn demo_full_stack_pattern() -> anyhow::Result<()> {
    println!("=== Demo 4: Full Resilience Stack Pattern ===\n");
    println!("Combining: Circuit Breaker + Timeout + Retry\n");

    let seed_nodes = vec![
        "127.0.0.1:7000".to_string(),
        "127.0.0.1:7001".to_string(),
        "127.0.0.1:7002".to_string(),
    ];

    let client = ClusterClient::new(seed_nodes).await?;

    // Stack 1: Resilient SET operations
    println!("Setting key with full resilience stack...");
    let retry_layer_set = RetryLayer::builder()
        .max_attempts(3)
        .backoff(ExponentialBackoff::new(Duration::from_millis(50)))
        .build();
    let cb_layer_set = CircuitBreakerLayer::builder()
        .failure_rate_threshold(0.5)
        .sliding_window_size(10)
        .wait_duration_in_open(Duration::from_secs(30))
        .build();
    let timeout_layer_set = TimeLimiterLayer::builder()
        .timeout_duration(Duration::from_secs(5))
        .build();

    let mut set_service =
        cb_layer_set.layer(timeout_layer_set.layer(retry_layer_set.layer(client.clone())));
    let _: () = set_service
        .call(Set::new("resilient_demo", "Highly resilient!"))
        .await?;

    // Stack 2: Resilient INCR operations
    println!("Incrementing counter 5 times with resilience...");
    let retry_layer_incr = RetryLayer::builder()
        .max_attempts(3)
        .backoff(ExponentialBackoff::new(Duration::from_millis(50)))
        .build();
    let cb_layer_incr = CircuitBreakerLayer::builder()
        .failure_rate_threshold(0.5)
        .sliding_window_size(10)
        .wait_duration_in_open(Duration::from_secs(30))
        .build();
    let timeout_layer_incr = TimeLimiterLayer::builder()
        .timeout_duration(Duration::from_secs(5))
        .build();

    let mut incr_service =
        cb_layer_incr.layer(timeout_layer_incr.layer(retry_layer_incr.layer(client.clone())));

    for i in 1..=5 {
        let count: i64 = incr_service.call(Incr::new("resilient_counter")).await?;
        println!("  Request {}: counter = {}", i, count);
    }

    // Verify without middleware
    println!("\nVerifying final value...");
    let value: Option<bytes::Bytes> = client.execute(Get::new("resilient_counter")).await?;
    println!(
        "Final counter: {:?}",
        value.map(|b| String::from_utf8_lossy(&b).to_string())
    );

    // Cleanup
    println!("\nCleaning up...");
    client
        .execute(Del::new(vec![
            "resilient_demo".to_string(),
            "resilient_counter".to_string(),
        ]))
        .await?;

    println!("\nResilience stack demonstrated:");
    println!("  ✓ Circuit Breaker: Opens on repeated failures");
    println!("  ✓ Timeout: Prevents hanging requests (5s max)");
    println!("  ✓ Retry: Exponential backoff on transient errors");
    println!("  ✓ All layers compose seamlessly with Tower!");
    println!("\nNote: Each middleware stack is typed to specific commands.");
    println!("In production, wrap your client once and reuse the service.");

    Ok(())
}
