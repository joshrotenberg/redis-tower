mod common;

use bytes::Bytes;
use redis_tower::RedisConnection;
use redis_tower::commands::*;
use redis_tower::pool::ConnectionPool;

// ---------------------------------------------------------------------------
// ConnectionPool<RedisConnection> integration tests (issue #345)
// ---------------------------------------------------------------------------

/// Basic round-trip through a ConnectionPool<RedisConnection>.
#[tokio::test]
async fn pool_set_get() {
    let addr = common::redis_addr().await.to_string();
    let pool = ConnectionPool::connect(3, || {
        let a = addr.clone();
        async move { RedisConnection::connect(&a).await }
    })
    .await
    .expect("failed to create pool");

    assert_eq!(pool.size(), 3);

    let k = "test:pool:set_get";
    pool.execute(Set::new(k, "hello")).await.unwrap();
    let val: Option<Bytes> = pool.execute(Get::new(k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("hello")));
    pool.execute(Del::new(k)).await.unwrap();
}

/// 100 concurrent tasks against a pool of 5 connections.
/// All tasks must complete and return correct values.
#[tokio::test]
async fn pool_concurrent_100_tasks_5_connections() {
    let addr = common::redis_addr().await.to_string();
    let pool = ConnectionPool::connect(5, || {
        let a = addr.clone();
        async move { RedisConnection::connect(&a).await }
    })
    .await
    .expect("failed to create pool");

    let mut handles = Vec::new();
    for i in 0..100_usize {
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            let k = format!("test:pool:concurrent:{i}");
            p.execute(Set::new(&k, format!("v{i}"))).await.unwrap();
            let v: Option<Bytes> = p.execute(Get::new(&k)).await.unwrap();
            assert_eq!(
                v,
                Some(Bytes::from(format!("v{i}"))),
                "value mismatch for key {k}"
            );
            p.execute(Del::new(&k)).await.unwrap();
        }));
    }

    for h in handles {
        h.await.expect("task panicked");
    }
}

/// A simple PING via the pool verifies the connection is alive.
#[tokio::test]
async fn pool_health_check_ping() {
    let addr = common::redis_addr().await.to_string();
    let pool = ConnectionPool::connect(2, || {
        let a = addr.clone();
        async move { RedisConnection::connect(&a).await }
    })
    .await
    .expect("failed to create pool");

    let pong: String = pool.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}
