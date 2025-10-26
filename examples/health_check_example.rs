//! Health checking example
//!
//! Demonstrates connection health checking with configurable policies.
//!
//! Run with:
//! ```bash
//! cargo run --example health_check_example
//! ```

use redis_tower::ResilientRedisClient;
use redis_tower::commands::{Get, Ping, Set};
use redis_tower::config::ClientConfig;
use redis_tower::health::HealthCheckConfig;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("=== Redis Health Check Example ===\n");

    // Example 1: Default health check configuration
    println!("Example 1: Default Health Checks (30s interval, 3 failure threshold)");
    {
        let config = ClientConfig::builder().build();

        let mut client =
            ResilientRedisClient::connect_with_full_config("localhost:6379", config).await?;

        // Perform some operations
        client.call(Set::new("health_test", "value")).await?;
        let _: Option<bytes::Bytes> = client.call(Get::new("health_test")).await?;

        // Check health status
        let status = client.health_status().await;
        let stats = client.health_stats();

        println!("  Status: {:?}", status);
        println!("  Total checks: {}", stats.total_checks);
        println!("  Success rate: {:.2}%", stats.success_rate());
        println!();
    }

    // Example 2: Aggressive health checking
    println!("Example 2: Aggressive Health Checks (5s interval, 2 failure threshold)");
    {
        let health_check = HealthCheckConfig::builder()
            .interval(Duration::from_secs(5))
            .timeout(Duration::from_secs(2))
            .failure_threshold(2)
            .success_threshold(1)
            .build();

        let config = ClientConfig::builder().health_check(health_check).build();

        let mut client =
            ResilientRedisClient::connect_with_full_config("localhost:6379", config).await?;

        // Perform operations that will trigger health checks
        for i in 0..5 {
            client.call(Set::new(format!("key{}", i), "value")).await?;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let status = client.health_status().await;
        let stats = client.health_stats();

        println!("  Status: {:?}", status);
        println!("  Total checks: {}", stats.total_checks);
        println!("  Consecutive successes: {}", stats.consecutive_successes);
        println!("  Success rate: {:.2}%", stats.success_rate());
        println!();
    }

    // Example 3: Disabled health checks
    println!("Example 3: Health Checks Disabled");
    {
        let config = ClientConfig::builder().no_health_check().build();

        let mut client =
            ResilientRedisClient::connect_with_full_config("localhost:6379", config).await?;

        // Perform operations - no health checks will occur
        for i in 0..10 {
            client.call(Set::new(format!("key{}", i), "value")).await?;
        }

        let stats = client.health_stats();

        println!("  Total checks: {} (should be 0)", stats.total_checks);
        println!();
    }

    // Example 4: Manual health check
    println!("Example 4: Manual Health Check");
    {
        let config = ClientConfig::builder().build();

        let client =
            ResilientRedisClient::connect_with_full_config("localhost:6379", config).await?;

        // Perform a manual health check using PING
        let result: String = client.call(Ping::new()).await?;
        println!("  PING result: {}", result);

        let status = client.health_status().await;
        println!("  Health status: {:?}", status);
        println!();
    }

    // Example 5: Monitoring health status over time
    println!("Example 5: Health Status Monitoring");
    {
        let health_check = HealthCheckConfig::builder()
            .interval(Duration::from_secs(2))
            .timeout(Duration::from_secs(1))
            .build();

        let config = ClientConfig::builder().health_check(health_check).build();

        let mut client =
            ResilientRedisClient::connect_with_full_config("localhost:6379", config).await?;

        // Monitor health over 10 seconds
        for i in 0..5 {
            // Perform some work
            client
                .call(Set::new(format!("monitor_key{}", i), "value"))
                .await?;

            let status = client.health_status().await;
            let stats = client.health_stats();

            println!(
                "  [{:2}s] Status: {:?}, Checks: {}, Success Rate: {:.2}%",
                i * 2,
                status,
                stats.total_checks,
                stats.success_rate()
            );

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        println!();
    }

    // Example 6: Health status transitions
    println!("Example 6: Demonstrating Health Status Transitions");
    {
        let health_check = HealthCheckConfig::builder()
            .interval(Duration::from_millis(100))
            .failure_threshold(3)
            .success_threshold(2)
            .build();

        let config = ClientConfig::builder().health_check(health_check).build();

        let mut client =
            ResilientRedisClient::connect_with_full_config("localhost:6379", config).await?;

        println!("  Initial status: {:?}", client.health_status().await);

        // Perform operations to trigger health checks
        for i in 0..10 {
            client
                .call(Set::new(format!("status_key{}", i), "value"))
                .await?;
            tokio::time::sleep(Duration::from_millis(150)).await;

            let status = client.health_status().await;
            let stats = client.health_stats();

            if stats.total_checks > 0 {
                println!(
                    "  After {} ops: {:?} (checks: {}, consecutive successes: {})",
                    i + 1,
                    status,
                    stats.total_checks,
                    stats.consecutive_successes
                );
            }
        }
    }

    println!("\n=== All Examples Complete ===");
    Ok(())
}
