//! # TLS Example
//!
//! Demonstrates connecting to Redis with TLS using a `rediss://` URL
//! (note the double `s`).
//!
//! **Prerequisites:**
//! - A Redis server with TLS enabled, listening on port 6380.
//! - Either the `tls-rustls` or `tls-native-tls` feature flag enabled for
//!   `redis-tower` (see `examples/Cargo.toml`).
//! - Adjust the URL to match your server's address and port.
//!
//! **Feature flags:**
//! - `tls-rustls` — TLS via the rustls backend (pure Rust, recommended).
//! - `tls-native-tls` — TLS via the native-tls backend (uses system OpenSSL).
//!
//! For custom certificate validation (e.g., self-signed certs), use
//! `TlsConfig` from `redis_tower_core::tls` with
//! `RedisConnection::connect_tls`.

use redis_tower::commands::*;
use redis_tower::{RedisConnection, RedisValueExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a `rediss://` URL to connect with TLS.
    // This requires the `tls-rustls` or `tls-native-tls` feature.
    let mut conn = RedisConnection::connect_url("rediss://127.0.0.1:6380").await?;

    conn.execute(Set::new("tls:key", "hello-tls")).await?;
    let val: String = conn.execute(Get::new("tls:key")).await?.parse_into()?;
    println!("Got: {val}");

    conn.execute(Del::new("tls:key")).await?;
    println!("TLS example complete.");
    Ok(())
}
