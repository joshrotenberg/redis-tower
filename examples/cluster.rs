//! # Cluster Example
//!
//! Demonstrates connecting to a Redis Cluster via `ClusterConnection` and
//! performing basic SET/GET operations. Slot routing is transparent -- the
//! client automatically sends each command to the correct node.
//!
//! **Prerequisites:** A Redis Cluster running with at least one seed node
//! reachable at `127.0.0.1:7000`. Adjust the address as needed.
//!
//! For multi-key operations spanning multiple keys, use hash tags (`{tag}`)
//! to force keys into the same slot, e.g., `user:{alice}:name` and
//! `user:{alice}:age`.

use redis_tower::RedisValueExt;
use redis_tower::commands::*;
use redis_tower_cluster::ClusterConnection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the cluster via a single seed node.
    // The client discovers the full topology automatically via CLUSTER SLOTS.
    let mut conn = ClusterConnection::connect("127.0.0.1:7000").await?;

    // SET and GET work transparently -- the client routes to the correct node
    // based on the key's CRC16 hash slot.
    conn.execute(Set::new("cluster:key", "hello-cluster"))
        .await?;
    let val: String = conn.execute(Get::new("cluster:key")).await?.parse_into()?;
    println!("Got: {val}");

    // Use hash tags for multi-key operations.
    // Both keys hash to the same slot, so MSET/MGET will work.
    conn.execute(Set::new("{user:1}:name", "Alice")).await?;
    conn.execute(Set::new("{user:1}:age", "30")).await?;
    let name: String = conn
        .execute(Get::new("{user:1}:name"))
        .await?
        .parse_into()?;
    println!("User name: {name}");

    // Clean up.
    conn.execute(Del::new("cluster:key")).await?;
    conn.execute(Del::new("{user:1}:name")).await?;
    conn.execute(Del::new("{user:1}:age")).await?;

    println!("Cluster example complete.");
    Ok(())
}
