//! Integration tests for Pub/Sub commands
//!
//! **Note on SUBSCRIBE/PSUBSCRIBE**: These commands put the connection into pub/sub mode
//! and require using `PubSubConnection` instead of `RedisClient`. These tests focus on
//! commands that work with regular connections:
//! - PUBLISH (works on any connection)
//! - PUBSUB CHANNELS/NUMSUB/NUMPAT (introspection commands)
//!
//! For full pub/sub functionality with SUBSCRIBE/PSUBSCRIBE, see examples/pubsub.rs
//! which demonstrates using `PubSubConnection`.
//!
//! Run with: cargo test --test integration_pubsub

use redis_tower::client::RedisClient;
use redis_tower::commands::pubsub::*;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;

/// Helper to create a Redis client connected to a testcontainer
async fn setup_redis() -> RedisClient {
    let container = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis container");

    let host = container.get_host().await.expect("Failed to get host");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get port");

    let client = RedisClient::connect(&format!("{}:{}", host, port))
        .await
        .expect("Failed to connect to Redis");

    // Keep container alive by leaking it (tests are short-lived)
    std::mem::forget(container);

    client
}

#[tokio::test]
async fn test_publish_no_subscribers() {
    let client = setup_redis().await;
    let channel = "empty_channel";

    // Publish to channel with no subscribers
    let receivers: i64 = client
        .call(Publish::new(channel, "Hello, World!"))
        .await
        .unwrap();

    // Should return 0 (no subscribers)
    assert_eq!(receivers, 0);
}

#[tokio::test]
async fn test_publish_with_payload() {
    let client = setup_redis().await;

    // Publish various payloads
    let _: i64 = client
        .call(Publish::new("chan1", "text message"))
        .await
        .unwrap();
    let _: i64 = client
        .call(Publish::new("chan2", b"binary data".to_vec()))
        .await
        .unwrap();
    let _: i64 = client.call(Publish::new("chan3", "12345")).await.unwrap();

    // All return 0 since no subscribers
}

#[tokio::test]
async fn test_pubsub_channels_empty() {
    let client = setup_redis().await;

    // Get all channels (should be empty)
    let channels: Vec<String> = client.call(PubsubChannels::all()).await.unwrap();
    assert_eq!(channels.len(), 0);
}

#[tokio::test]
async fn test_pubsub_channels_with_pattern() {
    let client = setup_redis().await;

    // Query with pattern (no active subscriptions)
    let channels: Vec<String> = client.call(PubsubChannels::pattern("news*")).await.unwrap();

    assert_eq!(channels.len(), 0);
}

#[tokio::test]
async fn test_pubsub_numsub_no_subscribers() {
    let client = setup_redis().await;

    // Query subscriber counts
    let counts: Vec<(String, i64)> = client
        .call(PubsubNumsub::new(&["chan1", "chan2", "chan3"]))
        .await
        .unwrap();

    // Should return entries for all queried channels
    assert_eq!(counts.len(), 3);

    // All should have 0 subscribers
    for (_, count) in &counts {
        assert_eq!(*count, 0);
    }
}

#[tokio::test]
async fn test_pubsub_numpat_no_patterns() {
    let client = setup_redis().await;

    // Query pattern subscription count
    let count: i64 = client.call(PubsubNumpat).await.unwrap();

    // Should be 0 (no pattern subscriptions)
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_publish_multiple_channels() {
    let client = setup_redis().await;

    // Publish to different channels
    let r1: i64 = client.call(Publish::new("sports", "goal!")).await.unwrap();
    let r2: i64 = client.call(Publish::new("news", "breaking")).await.unwrap();
    let r3: i64 = client.call(Publish::new("weather", "sunny")).await.unwrap();

    // All return 0 (no subscribers)
    assert_eq!(r1, 0);
    assert_eq!(r2, 0);
    assert_eq!(r3, 0);
}

#[tokio::test]
async fn test_publish_binary_data() {
    let client = setup_redis().await;

    let binary_payload = vec![0u8, 1, 2, 3, 255, 254, 253];
    let receivers: i64 = client
        .call(Publish::new("binary_channel", binary_payload))
        .await
        .unwrap();

    assert_eq!(receivers, 0);
}

#[tokio::test]
async fn test_publish_empty_payload() {
    let client = setup_redis().await;

    // Publish empty message
    let receivers: i64 = client
        .call(Publish::new("empty_channel", ""))
        .await
        .unwrap();

    assert_eq!(receivers, 0);
}

#[tokio::test]
async fn test_pubsub_numsub_single_channel() {
    let client = setup_redis().await;

    let counts: Vec<(String, i64)> = client
        .call(PubsubNumsub::new(&["single_channel"]))
        .await
        .unwrap();

    assert_eq!(counts.len(), 1);
    assert_eq!(counts[0].0, "single_channel");
    assert_eq!(counts[0].1, 0);
}

#[tokio::test]
async fn test_pubsub_channels_all() {
    let client = setup_redis().await;

    // List all active channels
    let channels: Vec<String> = client.call(PubsubChannels::all()).await.unwrap();

    // No active subscriptions, so empty
    assert!(channels.is_empty());
}

#[tokio::test]
async fn test_publish_large_payload() {
    let client = setup_redis().await;

    // Large payload (10KB)
    let large_payload = "x".repeat(10_000);
    let receivers: i64 = client
        .call(Publish::new("large_channel", large_payload))
        .await
        .unwrap();

    assert_eq!(receivers, 0);
}

#[tokio::test]
async fn test_pubsub_commands_available() {
    let client = setup_redis().await;

    // Verify all PUBSUB commands are callable
    let _: Vec<String> = client.call(PubsubChannels::all()).await.unwrap();
    let _: Vec<(String, i64)> = client.call(PubsubNumsub::new(&["test"])).await.unwrap();
    let _: i64 = client.call(PubsubNumpat).await.unwrap();
    let _: i64 = client.call(Publish::new("test", "data")).await.unwrap();

    // All commands work without errors
}
