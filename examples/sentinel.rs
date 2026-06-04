//! # Sentinel Example
//!
//! Demonstrates connecting to Redis via Sentinel for automatic master
//! discovery. The Sentinel cluster monitors the Redis master and automatically
//! elects a new master on failure.
//!
//! **Prerequisites:**
//! - At least one Sentinel running and reachable (default: `127.0.0.1:26379`).
//! - The monitored master name configured as `"mymaster"` in sentinel.conf.
//! - Adjust addresses and master name to match your setup.

use redis_tower::RedisValueExt;
use redis_tower::commands::*;
use redis_tower_sentinel::SentinelConnection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Discover the current master via Sentinel.
    // Pass all sentinel addresses for redundancy -- the client tries each in
    // order until one responds.
    let mut conn = SentinelConnection::connect(
        &["127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"],
        "mymaster",
    )
    .await?;

    // Commands go to the discovered master transparently.
    conn.execute(Set::new("sentinel:key", "hello-sentinel"))
        .await?;
    let val: String = conn.execute(Get::new("sentinel:key")).await?.parse_into()?;
    println!("Got: {val}");

    // Clean up.
    conn.execute(Del::new("sentinel:key")).await?;

    println!("Sentinel example complete.");
    Ok(())
}
