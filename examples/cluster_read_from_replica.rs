//! Example demonstrating read-from-replica functionality in Redis Cluster
//!
//! This example shows how to configure the cluster client to route read-only
//! commands to replica nodes, reducing load on master nodes and improving
//! read throughput.
//!
//! Prerequisites:
//! - Redis cluster running with replicas configured
//! - At least one replica per master
//!
//! Run with:
//! ```bash
//! cargo run --example cluster_read_from_replica
//! ```

use redis_tower::cluster::{ClusterClient, ReadPreference};
use redis_tower::commands::{Get, Set};
use redis_tower::pool::PoolConfig;
use redis_tower::tls::TlsConfig;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("Redis Cluster Read-from-Replica Example");
    println!("=========================================\n");

    // Cluster seed nodes
    let seed_nodes = vec!["127.0.0.1:7000".to_string()];

    // Configure connection pool
    let pool_config = PoolConfig::new(10)
        .with_min_idle(2)
        .with_max_lifetime(Some(Duration::from_secs(1800)))
        .with_idle_timeout(Some(Duration::from_secs(600)))
        .with_test_on_checkout(true);

    println!("Example 1: Default behavior (Master only)");
    println!("------------------------------------------");
    let client_master =
        ClusterClient::with_pool_config(seed_nodes.clone(), pool_config.clone()).await?;

    // Write a value
    println!("Writing key 'user:123' with value 'Alice'...");
    client_master.execute(Set::new("user:123", "Alice")).await?;

    // Read the value (from master)
    let value: Option<bytes::Bytes> = client_master.execute(Get::new("user:123")).await?;
    println!(
        "Read from master: {}",
        value
            .map(|v| String::from_utf8_lossy(&v).to_string())
            .unwrap_or_else(|| "None".to_string())
    );
    println!();

    println!("Example 2: Read from replicas (PreferReplica)");
    println!("----------------------------------------------");
    let client_replica = ClusterClient::with_full_config(
        seed_nodes.clone(),
        pool_config.clone(),
        TlsConfig::None,
        ReadPreference::PreferReplica,
    )
    .await?;

    // Write another value (goes to master)
    println!("Writing key 'user:456' with value 'Bob'...");
    client_replica.execute(Set::new("user:456", "Bob")).await?;

    // Give replication a moment to propagate
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Read the value (from replica if available, master if not)
    let value: Option<bytes::Bytes> = client_replica.execute(Get::new("user:456")).await?;
    println!(
        "Read from replica: {}",
        value
            .map(|v| String::from_utf8_lossy(&v).to_string())
            .unwrap_or_else(|| "None".to_string())
    );
    println!();

    println!("Example 3: Read from replicas only (Replica)");
    println!("---------------------------------------------");
    let client_replica_only = ClusterClient::with_full_config(
        seed_nodes.clone(),
        pool_config,
        TlsConfig::None,
        ReadPreference::Replica,
    )
    .await?;

    // Write a value (goes to master)
    println!("Writing key 'user:789' with value 'Charlie'...");
    client_replica_only
        .execute(Set::new("user:789", "Charlie"))
        .await?;

    // Give replication a moment to propagate
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Read the value (from replica only, fails if no replica available)
    let value: Option<bytes::Bytes> = client_replica_only.execute(Get::new("user:789")).await?;
    println!(
        "Read from replica: {}",
        value
            .map(|v| String::from_utf8_lossy(&v).to_string())
            .unwrap_or_else(|| "None".to_string())
    );
    println!();

    println!("Benefits of read-from-replica:");
    println!("- Reduced load on master nodes");
    println!("- Improved read throughput");
    println!("- Better resource utilization");
    println!();

    println!("Trade-offs:");
    println!("- Potential replication lag (slightly stale reads)");
    println!("- Requires cluster with replicas configured");
    println!();

    println!("Use cases:");
    println!("- Read-heavy workloads");
    println!("- Analytics and reporting queries");
    println!("- Serving cached content");
    println!("- Applications that can tolerate eventual consistency");

    Ok(())
}
