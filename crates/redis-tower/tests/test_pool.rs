mod common;

use bytes::Bytes;
use redis_server_wrapper::RedisServer;
use redis_tower::RedisConnection;
use redis_tower::commands::*;
use redis_tower::pool::{
    ConnectionPool, HealthProberConfig, PoolConfig, ReplicationLagHealthProbe,
};
use std::time::{Duration, Instant};

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

/// The replication-lag probe reads primary-side replica offsets from a real
/// `INFO replication` response rather than comparing replica-local offsets.
#[tokio::test]
async fn pool_replication_lag_probe_uses_primary_replica_offsets() {
    if std::env::var_os("REDIS_EXTERNAL_SERVICE").is_some() {
        return;
    }

    let first = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let second = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let primary_port = first.local_addr().unwrap().port();
    let replica_port = second.local_addr().unwrap().port();
    drop((first, second));

    let _primary_server = RedisServer::new()
        .port(primary_port)
        .repl_diskless_sync_delay(0)
        .start()
        .await
        .expect("failed to start primary");
    let _replica_server = RedisServer::new()
        .port(replica_port)
        .repl_diskless_sync_delay(0)
        .start()
        .await
        .expect("failed to start replica");
    let primary_addr = format!("127.0.0.1:{primary_port}");
    let replica_addr = format!("127.0.0.1:{replica_port}");
    let mut primary = RedisConnection::connect(&primary_addr).await.unwrap();
    let mut replica = RedisConnection::connect(&replica_addr).await.unwrap();
    replica
        .execute(ReplicaOf::new("127.0.0.1", primary_port))
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let info = primary
            .execute(Info::new().section("replication"))
            .await
            .unwrap();
        if info.contains("connected_slaves:1") && info.contains("state=online") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replica did not become visible in primary INFO replication:\n{info}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    primary
        .execute(Set::new("test:pool:replication-lag", "caught-up"))
        .await
        .unwrap();
    assert_eq!(primary.execute(Wait::new(1, 5_000)).await.unwrap(), 1);

    let pool = ConnectionPool::from_connections(vec![primary], PoolConfig::default()).unwrap();
    let max_expected_lag = 1_000_000;
    let prober = pool.spawn_health_prober_with(
        HealthProberConfig::default().interval(Duration::from_secs(60)),
        ReplicationLagHealthProbe::new(max_expected_lag),
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.stats().unknown_health_count != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the primary-side replication probe should complete");

    let stats = pool.stats();
    assert_eq!(stats.healthy_count, 1, "unexpected probe stats: {stats:?}");
    assert!(
        stats
            .max_replication_lag_bytes
            .is_some_and(|lag| lag <= max_expected_lag),
        "primary-side lag should be parsed from the live replica entry: {stats:?}"
    );
    prober.shutdown().await;
}
