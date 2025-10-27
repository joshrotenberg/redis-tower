//! Dynamic connection pool scaling example
//!
//! This example demonstrates how redis-tower's connection pool can automatically
//! scale up and down based on load, efficiently managing resources.
//!
//! Run with: cargo run --example pool_dynamic_scaling
//!
//! Prerequisites:
//! - Redis server running on localhost:6379

use redis_tower::commands::{Get, Ping, Set};
use redis_tower::pool::{ConnectionPool, PoolConfig};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing to see scaling events
    tracing_subscriber::fmt()
        .with_env_filter("pool_dynamic_scaling=debug,redis_tower=debug")
        .init();

    println!("=== Connection Pool Dynamic Scaling Demo ===\n");

    // Configure pool with dynamic scaling enabled
    let config = PoolConfig::new(20) // Max 20 connections
        .with_min_idle(2) // Keep at least 2 connections
        .with_dynamic_scaling(true) // Enable auto-scaling
        .with_scale_up_threshold(0.8) // Scale up at 80% utilization
        .with_scale_down_threshold(0.2) // Scale down at 20% utilization
        .with_scale_increment(2) // Add/remove 2 connections at a time
        .with_reaper_interval(Some(Duration::from_secs(2))); // Check every 2 seconds

    let pool = ConnectionPool::with_config("localhost:6379".to_string(), config);

    // Start the background reaper (required for dynamic scaling)
    pool.start_reaper().await;
    println!("Started connection pool with dynamic scaling enabled");
    println!("Min: 2 connections, Max: 20 connections");
    println!("Scale up threshold: 80%, Scale down threshold: 20%\n");

    // Phase 1: Low load - pool should scale down
    println!("Phase 1: Low Load (should scale down to min_idle)");
    println!("-----------------------------------------------");

    // Create initial connections
    let conn = pool.get().await?;
    conn.execute(Ping).await?;
    println!(
        "Pool size: {}, Utilization: {:.1}%",
        pool.size().await,
        pool.stats().utilization_percent(pool.max_size())
    );

    sleep(Duration::from_secs(5)).await;

    let stats = pool.stats();
    println!("After 5 seconds:");
    println!("  Pool size: {}", pool.size().await);
    println!("  Scale-down operations: {}", stats.total_scale_downs);
    println!();

    // Phase 2: Medium load
    println!("Phase 2: Medium Load (stable, no scaling)");
    println!("------------------------------------------");

    let mut connections = Vec::new();
    for i in 0..5 {
        let conn = pool.get().await?;
        conn.execute(Set::new(format!("key{}", i), format!("value{}", i)))
            .await?;
        connections.push(conn);
    }

    println!(
        "Pool size: {}, In use: {}, Utilization: {:.1}%",
        pool.size().await,
        pool.stats().in_use_count,
        pool.stats().utilization_percent(pool.max_size())
    );

    sleep(Duration::from_secs(5)).await;

    let stats = pool.stats();
    println!("After 5 seconds:");
    println!("  Pool size: {}", pool.size().await);
    println!("  No scaling expected (utilization moderate)");
    println!();

    // Phase 3: High load - pool should scale up
    println!("Phase 3: High Load (should scale up)");
    println!("-------------------------------------");

    // Simulate high utilization by getting many connections
    for i in 5..15 {
        let conn = pool.get().await?;
        conn.execute(Set::new(format!("key{}", i), format!("value{}", i)))
            .await?;
        connections.push(conn);
    }

    println!(
        "Pool size: {}, In use: {}, Utilization: {:.1}%",
        pool.size().await,
        pool.stats().in_use_count,
        pool.stats().utilization_percent(pool.max_size())
    );

    // Wait for scaling to happen
    sleep(Duration::from_secs(5)).await;

    let stats = pool.stats();
    println!("After 5 seconds:");
    println!("  Pool size: {}", pool.size().await);
    println!("  Scale-up operations: {}", stats.total_scale_ups);
    println!("  Connections added: {}", stats.scaled_up_connections);
    println!();

    // Phase 4: Release connections - pool should scale down
    println!("Phase 4: Load Decrease (should scale down)");
    println!("-------------------------------------------");

    // Release most connections
    connections.clear();

    println!(
        "Pool size: {}, In use: {}, Utilization: {:.1}%",
        pool.size().await,
        pool.stats().in_use_count,
        pool.stats().utilization_percent(pool.max_size())
    );

    sleep(Duration::from_secs(5)).await;

    let stats = pool.stats();
    println!("After 5 seconds:");
    println!("  Pool size: {}", pool.size().await);
    println!("  Scale-down operations: {}", stats.total_scale_downs);
    println!("  Connections removed: {}", stats.scaled_down_connections);
    println!();

    // Final statistics
    println!("=== Final Pool Statistics ===");
    println!("Total connections created: {}", stats.total_created);
    println!("Total scale-up operations: {}", stats.total_scale_ups);
    println!("Total scale-down operations: {}", stats.total_scale_downs);
    println!(
        "Total connections added via scaling: {}",
        stats.scaled_up_connections
    );
    println!(
        "Total connections removed via scaling: {}",
        stats.scaled_down_connections
    );
    println!();

    println!("=== Key Benefits of Dynamic Scaling ===");
    println!("1. Efficient resource usage - pool grows with demand");
    println!("2. Cost savings - fewer idle connections during low load");
    println!("3. Automatic adaptation - no manual tuning required");
    println!("4. Performance - enough connections during high load");
    println!();

    println!("Dynamic scaling demo completed successfully!");

    Ok(())
}
