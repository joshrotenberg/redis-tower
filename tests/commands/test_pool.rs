//! Integration tests for ConnectionPool
//!
//! These tests require a live Redis server running on localhost:6379

use redis_tower::pool::{ConnectionPool, PoolConfig};
use std::time::Duration;

#[tokio::test]
async fn test_pool_get_creates_connection() {
    let pool = ConnectionPool::new("127.0.0.1:6379".to_string(), 5);

    assert_eq!(pool.size().await, 0);

    // First get should create a connection
    let conn = pool.get().await.expect("Failed to get connection");
    assert_eq!(pool.size().await, 1);

    // Connection should be usable
    use redis_tower::commands::Ping;
    conn.execute(Ping::new()).await.expect("PING failed");
}

#[tokio::test]
async fn test_pool_reuses_connections() {
    let pool = ConnectionPool::new("127.0.0.1:6379".to_string(), 5);

    // Get first connection
    let _conn1 = pool.get().await.expect("Failed to get connection 1");
    assert_eq!(pool.size().await, 1);

    // Get second connection (connections are cloned, so pool still has 1)
    let _conn2 = pool.get().await.expect("Failed to get connection 2");
    assert_eq!(pool.size().await, 1);

    let stats = pool.stats();
    assert_eq!(stats.total_created, 1); // Only created 1 connection
    assert_eq!(stats.total_gets, 2); // But served 2 gets
}

#[tokio::test]
async fn test_pool_respects_max_size() {
    let config = PoolConfig::new(3).with_test_on_checkout(false); // Disable health check for speed

    let pool = ConnectionPool::with_config("127.0.0.1:6379".to_string(), config);

    // Try to grow beyond max_size
    pool.grow(10).await.expect("Failed to grow pool");

    // Should only create up to max_size
    assert_eq!(pool.size().await, 3);

    let stats = pool.stats();
    assert_eq!(stats.total_created, 3);
}

#[tokio::test]
async fn test_pool_health_check_on_checkout() {
    let config = PoolConfig::new(5).with_test_on_checkout(true);

    let pool = ConnectionPool::with_config("127.0.0.1:6379".to_string(), config);

    // Get a connection - should trigger health check
    let conn = pool.get().await.expect("Failed to get connection");

    let stats = pool.stats();
    assert_eq!(stats.total_gets, 1);
    // Health check passed (no failures)
    assert_eq!(stats.health_check_failures, 0);

    // Verify connection works
    use redis_tower::commands::Ping;
    conn.execute(Ping::new()).await.expect("PING failed");
}

#[tokio::test]
async fn test_pool_no_health_check() {
    let config = PoolConfig::new(5).with_test_on_checkout(false);

    let pool = ConnectionPool::with_config("127.0.0.1:6379".to_string(), config);

    let _conn = pool.get().await.expect("Failed to get connection");

    let stats = pool.stats();
    assert_eq!(stats.health_check_failures, 0); // No checks performed
}

#[tokio::test]
async fn test_pool_grow() {
    let config = PoolConfig::new(10).with_test_on_checkout(false);

    let pool = ConnectionPool::with_config("127.0.0.1:6379".to_string(), config);

    assert_eq!(pool.size().await, 0);

    // Grow to 5 connections
    pool.grow(5).await.expect("Failed to grow pool");
    assert_eq!(pool.size().await, 5);

    // Grow to 8 connections
    pool.grow(8).await.expect("Failed to grow pool");
    assert_eq!(pool.size().await, 8);

    let stats = pool.stats();
    assert_eq!(stats.total_created, 8);
}

#[tokio::test]
async fn test_pool_statistics() {
    let config = PoolConfig::new(5).with_test_on_checkout(true);

    let pool = ConnectionPool::with_config("127.0.0.1:6379".to_string(), config);

    // Get some connections
    for _ in 0..10 {
        let _conn = pool.get().await.expect("Failed to get connection");
    }

    let stats = pool.stats();
    assert_eq!(stats.total_gets, 10);
    assert!(stats.total_created > 0);
    assert!(stats.total_created <= 5); // Respects max_size
}

#[tokio::test]
async fn test_pool_concurrent_gets() {
    let config = PoolConfig::new(10).with_test_on_checkout(false);

    let pool = ConnectionPool::with_config("127.0.0.1:6379".to_string(), config);

    // Spawn 20 concurrent get operations
    let mut handles = vec![];
    for i in 0..20 {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            let conn = pool_clone.get().await.expect("Failed to get connection");

            // Use the connection
            use redis_tower::commands::{Del, Get, Set};
            let key = format!("concurrent_test_{}", i);
            conn.execute(Set::new(&key, "value"))
                .await
                .expect("SET failed");
            let value: Option<bytes::Bytes> =
                conn.execute(Get::new(&key)).await.expect("GET failed");
            assert!(value.is_some());
            conn.execute(Del::new(vec![key])).await.expect("DEL failed");
        });
        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        handle.await.expect("Task panicked");
    }

    let stats = pool.stats();
    assert_eq!(stats.total_gets, 20);
    assert!(stats.total_created <= 10); // Should stay within max_size
}

#[tokio::test]
async fn test_pool_min_idle() {
    let config = PoolConfig::new(10)
        .with_min_idle(3)
        .with_test_on_checkout(false);

    let pool = ConnectionPool::with_config("127.0.0.1:6379".to_string(), config);

    // Manually ensure min_idle
    pool.grow(3).await.expect("Failed to grow pool");
    assert_eq!(pool.size().await, 3);

    // Get connections
    for _ in 0..5 {
        let _conn = pool.get().await.expect("Failed to get connection");
    }

    // Pool should maintain connections
    assert!(pool.size().await >= 1);
}

#[tokio::test]
async fn test_pool_config_integration() {
    // Test various configurations work in practice
    let configs = vec![
        PoolConfig::new(1),    // Minimum pool
        PoolConfig::new(100),  // Large pool
        PoolConfig::default(), // Default config
        PoolConfig::new(5).with_min_idle(2),
        PoolConfig::new(10).with_test_on_checkout(false),
        PoolConfig::new(5)
            .with_max_lifetime(Some(Duration::from_secs(3600)))
            .with_idle_timeout(Some(Duration::from_secs(600))),
    ];

    for (i, config) in configs.into_iter().enumerate() {
        let pool = ConnectionPool::with_config("127.0.0.1:6379".to_string(), config);

        // Each config should work
        let conn = pool
            .get()
            .await
            .unwrap_or_else(|_| panic!("Config {} failed to get connection", i));

        use redis_tower::commands::Ping;
        conn.execute(Ping::new())
            .await
            .unwrap_or_else(|_| panic!("Config {} PING failed", i));
    }
}

#[tokio::test]
async fn test_pool_error_handling() {
    // Test with invalid address
    let pool = ConnectionPool::new("127.0.0.1:9999".to_string(), 5);

    let result = pool.get().await;
    assert!(result.is_err(), "Should fail to connect to invalid port");
}
