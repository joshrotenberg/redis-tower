//! Resilience capabilities: a self-healing client, health checks, and the
//! error taxonomy used to classify failures.
//!
//! For circuit breaking, rate limiting, and bulkhead isolation, compose the
//! frame-level service with the `tower-resilience` crate family and hand the
//! result to `MultiplexedClient::from_layered`.

use redis_tower::ResilientRedisClient;
use redis_tower::commands::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `ResilientRedisClient` auto-reconnects with exponential backoff + jitter,
    // single-flighting reconnects across clones. It is `Clone + Send + Sync`.
    let client = ResilientRedisClient::connect("127.0.0.1:6379").await?;

    client.execute(Set::new("res:key", "ok")).await?;
    let val: Option<bytes::Bytes> = client.execute(Get::new("res:key")).await?;
    println!("Got: {val:?}");

    // Health check -- handy for `/health` endpoints and Kubernetes readiness
    // probes.
    match client.health_check().await {
        Ok(()) => println!("healthy"),
        Err(e) => println!("unhealthy: {e}"),
    }

    // Error taxonomy: classify a failure to decide how to react. INCR on a
    // non-integer value is a command error -- `is_retryable()` is false, so a
    // retry would be pointless.
    if let Err(e) = client.execute(Incr::new("res:key")).await {
        println!(
            "incr failed: retryable={}, connection_error={}",
            e.is_retryable(),
            e.is_connection_error(),
        );
    }

    client.execute(Del::new("res:key")).await?;
    Ok(())
}
