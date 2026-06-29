mod common;

use bytes::Bytes;
use redis_tower::RedisConnection;
use redis_tower::commands::*;
use redis_tower::pool::{ConnectionPool, PoolConfig};

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

/// Pool exhaustion under load: drive far more concurrent requests than there
/// are connections and verify every request still completes correctly.
///
/// A pool of 2 connections is saturated by 200 concurrent tasks (each doing
/// SET/GET/DEL). With the acquisition timeout disabled, every request must
/// eventually run and observe its own value; the head-of-line-blocking
/// `try_lock` scan keeps a request from queuing behind a busy connection when
/// the other is free. The pool must drain back to fully idle afterwards.
#[tokio::test]
async fn pool_saturation_under_load() {
    let addr = common::redis_addr().await.to_string();
    let pool = ConnectionPool::connect_with_config(
        PoolConfig::default().size(2).disable_acquisition_timeout(),
        || {
            let a = addr.clone();
            async move { RedisConnection::connect(&a).await }
        },
    )
    .await
    .expect("failed to create pool");

    assert_eq!(pool.size(), 2);

    let tasks = 200_usize;
    let mut handles = Vec::with_capacity(tasks);
    for i in 0..tasks {
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            let k = format!("test:pool:saturation:{i}");
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

    // Once the load drains, every connection is idle again.
    let stats = pool.stats();
    assert_eq!(stats.total_inflight, 0, "pool did not return to idle");
    assert_eq!(stats.idle_count, stats.size);
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
