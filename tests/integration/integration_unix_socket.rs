//! Integration tests for Unix socket connections
//!
//! These tests require Redis to be running with Unix socket support.
//! Configure Redis with: `unixsocket /tmp/redis.sock`

use bytes::Bytes;
use redis_tower::RedisClient;
use redis_tower::commands::Del;
use redis_tower::commands::strings::{Get, Set};

async fn setup_redis_unix() -> RedisClient {
    // Try to connect to Unix socket
    // This will fail if Redis is not configured with Unix socket support
    RedisClient::connect("unix:///tmp/redis.sock")
        .await
        .expect("Failed to connect to Redis via Unix socket. Configure Redis with: unixsocket /tmp/redis.sock")
}

#[tokio::test]
#[ignore] // Requires Redis with Unix socket configured
async fn test_unix_socket_basic_connection() {
    let client = setup_redis_unix().await;

    // Test basic SET/GET
    let key = "test_unix_basic";
    client.call(Set::new(key, "hello")).await.unwrap();

    let value: Option<Bytes> = client.call(Get::new(key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from("hello")));

    // Cleanup
    client.call(Del::new(vec![key.to_string()])).await.unwrap();
}

#[tokio::test]
#[ignore] // Requires Redis with Unix socket configured
async fn test_unix_socket_path_direct() {
    // Connect directly with path (no URL scheme)
    let client = RedisClient::connect("/tmp/redis.sock")
        .await
        .expect("Failed to connect with direct path");

    let key = "test_unix_direct";
    client.call(Set::new(key, "world")).await.unwrap();

    let value: Option<Bytes> = client.call(Get::new(key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from("world")));

    client.call(Del::new(vec![key.to_string()])).await.unwrap();
}

#[tokio::test]
#[ignore] // Requires Redis with Unix socket configured
async fn test_unix_socket_multiple_operations() {
    let client = setup_redis_unix().await;

    // Perform multiple operations
    for i in 0..10 {
        let key = format!("test_unix_multi_{}", i);
        let value = format!("value_{}", i);
        let value_bytes = Bytes::from(value.clone());

        client
            .call(Set::new(&key, value_bytes.clone()))
            .await
            .unwrap();

        let retrieved: Option<Bytes> = client.call(Get::new(&key)).await.unwrap();
        assert_eq!(retrieved, Some(value_bytes));
    }

    // Cleanup
    let keys: Vec<String> = (0..10).map(|i| format!("test_unix_multi_{}", i)).collect();
    client.call(Del::new(keys)).await.unwrap();
}
