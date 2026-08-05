//! Cloneable client-side caching with RESP3 invalidation tracking.

use std::time::Duration;

use redis_tower::commands::{Del, Get, Set};
use redis_tower::{CacheTrackingMode, CachedClientConfig, CachedMultiplexedClient, RedisError};

#[tokio::main]
async fn main() -> Result<(), RedisError> {
    let config = CachedClientConfig::new()
        .max_entries(10_000)
        .client_ttl(Some(Duration::from_secs(30)))
        .tracking_mode(CacheTrackingMode::broadcast_with_prefixes(["example:"]));

    let client = CachedMultiplexedClient::connect_with_config("127.0.0.1:6379", config).await?;
    client
        .execute(Set::new("example:greeting", "hello"))
        .await?;

    let first = client.execute(Get::new("example:greeting")).await?;
    let second = client.clone().execute(Get::new("example:greeting")).await?;
    assert_eq!(first, second);

    let statistics = client.cache_statistics().await;
    println!(
        "hits={} misses={} invalidations={} evictions={}",
        statistics.hits, statistics.misses, statistics.invalidations, statistics.evictions,
    );

    client.execute(Del::new("example:greeting")).await?;
    client.shutdown().await;
    Ok(())
}
