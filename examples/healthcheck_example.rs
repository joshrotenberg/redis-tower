//! Proactive health checking for Redis connections using tower-resilience-healthcheck.
//!
//! This example demonstrates:
//! - Continuous health monitoring of multiple Redis connections
//! - Intelligent selection strategies (RoundRobin, FirstAvailable, PreferHealthy)
//! - Automatic failover to healthy replicas
//! - Threshold-based state transitions to prevent flapping
//!
//! Run with: cargo run --example healthcheck_example

use redis_tower::{
    RedisClient,
    commands::{Get, Ping, Set},
};
use std::time::Duration;
use tower_resilience_healthcheck::{
    HealthCheckWrapper, HealthChecker, HealthStatus, SelectionStrategy,
};

/// Health checker for Redis connections using PING command.
struct RedisHealthChecker;

impl HealthChecker<RedisClient> for RedisHealthChecker {
    async fn check(&self, client: &RedisClient) -> HealthStatus {
        // Try to PING with a short timeout
        match tokio::time::timeout(Duration::from_millis(100), client.call(Ping::new())).await {
            Ok(Ok(_)) => {
                // PING succeeded - connection is healthy
                HealthStatus::Healthy
            }
            Ok(Err(_)) => {
                // PING failed - connection is unhealthy
                HealthStatus::Unhealthy
            }
            Err(_) => {
                // PING timed out - connection is degraded (slow but working)
                HealthStatus::Degraded
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Redis Health Check Example ===\n");

    // Create multiple Redis connections (simulating primary + replicas)
    println!("Connecting to Redis instances...");
    let primary = RedisClient::connect("redis://localhost:6379").await?;
    let replica1 = RedisClient::connect("redis://localhost:6380").await?;
    let replica2 = RedisClient::connect("redis://localhost:6381").await?;

    println!("Connected to 3 Redis instances");
    println!("  - Primary: localhost:6379");
    println!("  - Replica1: localhost:6380");
    println!("  - Replica2: localhost:6381\n");

    // Create health check wrapper with multiple connections
    let wrapper = HealthCheckWrapper::builder()
        .with_context(primary.clone(), "primary")
        .with_context(replica1.clone(), "replica1")
        .with_context(replica2.clone(), "replica2")
        .with_checker(RedisHealthChecker)
        .with_interval(Duration::from_secs(2)) // Check every 2 seconds
        .with_initial_delay(Duration::from_millis(100)) // Start checking quickly
        .with_failure_threshold(2) // Mark unhealthy after 2 consecutive failures
        .with_success_threshold(2) // Mark healthy after 2 consecutive successes
        .with_selection_strategy(SelectionStrategy::RoundRobin) // Load balance across healthy instances
        .build();

    // Start background health checking
    println!("Starting background health checks (every 2 seconds)...\n");
    wrapper.start().await;

    // Wait for initial health checks to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Show initial health status
    println!("Initial health status:");
    let statuses = wrapper.get_all_statuses().await;
    for (name, status) in &statuses {
        println!("  {}: {:?}", name, status);
    }
    println!();

    // Get detailed health information
    println!("Detailed health information:");
    let details = wrapper.get_health_details().await;
    for detail in &details {
        println!("  {}:", detail.name);
        println!("    Status: {:?}", detail.status);
        println!(
            "    Consecutive successes: {}",
            detail.consecutive_successes
        );
        println!("    Consecutive failures: {}", detail.consecutive_failures);
    }
    println!();

    // Demonstrate using healthy connections
    println!("Writing test data using healthy connection...");
    if let Some(client) = wrapper.get_healthy().await {
        client.call(Set::new("healthcheck:test", "value")).await?;
        println!("  SET healthcheck:test = value");
    } else {
        println!("  No healthy connection available!");
    }

    // Demonstrate round-robin load balancing
    println!("\nReading data with round-robin load balancing:");
    for i in 1..=5 {
        if let Some(client) = wrapper.get_healthy().await {
            match client.call(Get::new("healthcheck:test")).await {
                Ok(value) => {
                    let val = value.map(|v| String::from_utf8_lossy(&v).to_string());
                    println!("  Request {}: {:?}", i, val);
                }
                Err(e) => {
                    println!("  Request {} failed: {}", i, e);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Show which connections are being used
    println!("\nCurrent health status (showing load distribution):");
    let statuses = wrapper.get_all_statuses().await;
    for (name, status) in &statuses {
        println!("  {}: {:?}", name, status);
    }

    // Demonstrate PreferHealthy strategy
    println!("\nSwitching to PreferHealthy strategy...");
    let wrapper_prefer = HealthCheckWrapper::builder()
        .with_context(primary.clone(), "primary")
        .with_context(replica1.clone(), "replica1")
        .with_context(replica2.clone(), "replica2")
        .with_checker(RedisHealthChecker)
        .with_interval(Duration::from_secs(2))
        .with_initial_delay(Duration::from_millis(100))
        .with_selection_strategy(SelectionStrategy::PreferHealthy) // Prefer healthy over degraded
        .build();

    wrapper_prefer.start().await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("PreferHealthy will choose healthy connections first, fallback to degraded");

    // Show statistics before cleanup
    println!("\nFinal health check statistics:");
    let details = wrapper.get_health_details().await;
    for detail in &details {
        println!("  {}:", detail.name);
        println!("    Final status: {:?}", detail.status);
        println!("    Total successes: {}", detail.consecutive_successes);
        println!("    Total failures: {}", detail.consecutive_failures);
    }

    // Stop health checking
    println!("\nStopping health checks...");
    wrapper.stop().await;
    wrapper_prefer.stop().await;

    println!("Done!");

    Ok(())
}
