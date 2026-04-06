//! Auto-reconnecting client that survives connection drops.

use std::time::Duration;

use redis_tower::commands::*;
use redis_tower::reconnect::{AddrConnectionFactory, ReconnectConfig};
use redis_tower::ResilientConnection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ReconnectConfig::default()
        .max_retries(5)
        .base_delay(Duration::from_millis(200))
        .max_delay(Duration::from_secs(2));

    let mut conn =
        ResilientConnection::new(AddrConnectionFactory::new("127.0.0.1:6379"), config)
            .await?
            .on_reconnect(|attempt| {
                println!("Reconnected after {attempt} attempt(s)");
            });

    // Normal usage -- reconnects transparently on failure.
    conn.execute(Set::new("rc:key", "resilient")).await?;
    let val: Option<bytes::Bytes> = conn.execute(Get::new("rc:key")).await?;
    println!("Got: {val:?}");

    // Clean up.
    conn.execute(Del::new("rc:key")).await?;

    Ok(())
}
