//! Integration tests for server management commands
//!
//! Tests server commands like PING, ECHO, INFO, DBSIZE, CONFIG, etc.
//!
//! Run with: cargo test --test integration_server

mod helpers;

use helpers::standalone::setup_redis;
use redis_tower::commands::*;

#[tokio::test]
async fn test_ping() {
    let client = setup_redis().await;

    // PING without message
    let response: String = client.call(Ping::new()).await.unwrap();
    assert_eq!(response, "PONG");

    // PING with message
    let response: String = client.call(Ping::with_message("hello")).await.unwrap();
    assert_eq!(response, "hello");
}

#[tokio::test]
async fn test_echo() {
    let client = setup_redis().await;

    let response: String = client.call(Echo::new("test message")).await.unwrap();
    assert_eq!(response, "test message");
}

#[tokio::test]
async fn test_dbsize() {
    let client = setup_redis().await;

    // Get current size - just verify it returns a non-negative value
    let size: i64 = client.call(DbSize::new()).await.unwrap();
    assert!(size >= 0);
}

#[tokio::test]
async fn test_info() {
    let client = setup_redis().await;

    // Get default server info
    let info: String = client.call(Info::default_info()).await.unwrap();

    // Should contain server information
    assert!(info.contains("redis_version"));
    assert!(info.contains("os:"));

    // Get specific section
    let server_info: String = client.call(Info::section("server")).await.unwrap();
    assert!(server_info.contains("redis_version"));
}

#[tokio::test]
async fn test_config_get() {
    let client = setup_redis().await;

    // Get maxmemory config
    let config: Vec<(String, String)> = client.call(ConfigGet::new("maxmemory")).await.unwrap();

    // Should return key-value pairs
    assert!(!config.is_empty());
    assert_eq!(config[0].0, "maxmemory");
}

#[tokio::test]
async fn test_time() {
    let client = setup_redis().await;

    let (seconds, microseconds): (i64, i64) = client.call(Time::new()).await.unwrap();

    // Should be reasonable Unix timestamp (after 2020)
    assert!(seconds > 1600000000);
    assert!((0..1_000_000).contains(&microseconds));
}

#[tokio::test]
async fn test_lastsave() {
    let client = setup_redis().await;

    let timestamp: i64 = client.call(LastSave::new()).await.unwrap();

    // Should be a valid Unix timestamp
    assert!(timestamp > 0);
}

#[tokio::test]
async fn test_flushdb() {
    let client = setup_redis().await;

    // Add some test keys
    client
        .call(Set::new("flush_test1", "value1"))
        .await
        .unwrap();
    client
        .call(Set::new("flush_test2", "value2"))
        .await
        .unwrap();

    // Flush the database
    client.call(FlushDb::new()).await.unwrap();

    // Keys should be gone
    let exists: i64 = client
        .call(Exists::multiple(vec!["flush_test1", "flush_test2"]))
        .await
        .unwrap();
    assert_eq!(exists, 0);
}
