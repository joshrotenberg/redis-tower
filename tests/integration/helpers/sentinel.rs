use redis_tower::sentinel::{SentinelClient, SentinelConfig};

/// Setup Redis Sentinel client
///
/// **Prerequisites**: This assumes a Redis Sentinel cluster is already running:
/// - 1 master on port 6380
/// - 2 replicas on ports 6381-6382
/// - 3 sentinels on ports 26379-26381
///
/// You can start the sentinel cluster using docker-compose:
/// ```bash
/// docker-compose --profile sentinel up -d
/// ```
///
/// Or manually using docker-wrapper in a separate script/test setup.
pub async fn setup_sentinel() -> SentinelClient {
    // Create SentinelClient - assumes sentinel is already running
    let config = SentinelConfig::builder()
        .sentinel_node("localhost", 26379)
        .sentinel_node("localhost", 26380)
        .sentinel_node("localhost", 26381)
        .master_name("mymaster")
        .build()
        .expect("Failed to build sentinel config");

    SentinelClient::new(config)
}
