//! Sentinel integration tests.
//!
//! Run with: `cargo test -p redis-tower-sentinel --test sentinel_integration --all-features -- --ignored`

use bytes::Bytes;
use redis_server_wrapper::{RedisSentinel, RedisSentinelHandle};
use redis_tower::pool::ConnectionPool;
use redis_tower_commands::*;
use redis_tower_sentinel::{MultiplexedSentinelClient, SentinelClient, SentinelConnection};
use tokio::sync::OnceCell;

static SENTINEL: OnceCell<RedisSentinelHandle> = OnceCell::const_new();

async fn ensure_sentinel() -> &'static RedisSentinelHandle {
    SENTINEL
        .get_or_init(|| async {
            RedisSentinel::builder()
                .master_port(6390)
                .replica_base_port(6391)
                .sentinel_base_port(26389)
                .replicas(2)
                .sentinels(3)
                .quorum(2)
                .start()
                .await
                .expect("failed to start sentinel topology")
        })
        .await
}

async fn sentinel_addrs() -> Vec<String> {
    ensure_sentinel().await.sentinel_addrs()
}

async fn sentinel_conn() -> SentinelConnection {
    let addrs = sentinel_addrs().await;
    SentinelConnection::connect(&addrs, "mymaster")
        .await
        .expect("failed to connect via sentinel")
}

fn key(test: &str, name: &str) -> String {
    format!("sentinel_test:{test}:{name}")
}

// Generate shared command tests for sentinel topology.
redis_test_harness::command_tests!(sentinel_conn, "sentinel_cmd", ignored);

// -- Sentinel-specific tests --

#[tokio::test]
#[ignore]
async fn sentinel_discovers_master() {
    let addrs = sentinel_addrs().await;
    let addr = redis_tower_sentinel::discovery::discover_master(&addrs, "mymaster")
        .await
        .unwrap();
    assert!(
        addr.contains("6390"),
        "expected master on port 6390, got {addr}"
    );
}

#[tokio::test]
#[ignore]
async fn sentinel_discovers_replicas() {
    let addrs = sentinel_addrs().await;
    let replicas = redis_tower_sentinel::discovery::discover_replicas(&addrs, "mymaster")
        .await
        .unwrap();
    assert_eq!(replicas.len(), 2, "expected 2 replicas, got {replicas:?}");
}

#[tokio::test]
#[ignore]
async fn sentinel_set_and_get() {
    let mut conn = sentinel_conn().await;
    let k = key("set_get", "k");
    conn.execute(Set::new(&k, "hello")).await.unwrap();
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("hello")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn sentinel_client_shared() {
    let addrs = sentinel_addrs().await;
    let client = SentinelClient::connect(&addrs, "mymaster").await.unwrap();
    let k = key("client_shared", "k");
    client.execute(Set::new(&k, "val")).await.unwrap();

    let c = client.clone();
    let k2 = k.clone();
    let handle = tokio::spawn(async move { c.execute(Get::new(&k2)).await.unwrap() });

    let val: Option<Bytes> = handle.await.unwrap();
    assert_eq!(val, Some(Bytes::from("val")));
    client.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn sentinel_rediscover() {
    let mut conn = sentinel_conn().await;
    conn.rediscover().await.unwrap();
    let pong = conn.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

// -- MultiplexedSentinelClient tests --

#[tokio::test]
#[ignore]
async fn multiplexed_sentinel_connect_execute() {
    let addrs = sentinel_addrs().await;
    let client = MultiplexedSentinelClient::connect(&addrs, "mymaster")
        .await
        .unwrap();
    let k = key("multiplexed_connect", "k");
    client.execute(Set::new(&k, "hello")).await.unwrap();
    let val: Option<Bytes> = client.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("hello")));
    client.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn multiplexed_sentinel_clone_across_tasks() {
    let addrs = sentinel_addrs().await;
    let client = MultiplexedSentinelClient::connect(&addrs, "mymaster")
        .await
        .unwrap();
    let k = key("multiplexed_clone", "k");
    client.execute(Set::new(&k, "shared")).await.unwrap();

    let c = client.clone();
    let k2 = k.clone();
    let handle = tokio::spawn(async move { c.execute(Get::new(&k2)).await.unwrap() });
    let val: Option<Bytes> = handle.await.unwrap();
    assert_eq!(val, Some(Bytes::from("shared")));
    client.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn multiplexed_sentinel_concurrent_commands() {
    let addrs = sentinel_addrs().await;
    let client = MultiplexedSentinelClient::connect(&addrs, "mymaster")
        .await
        .unwrap();

    let n = 20usize;
    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        let c = client.clone();
        let k = key("multiplexed_concurrent", &format!("k{i}"));
        let v = format!("val{i}");
        handles.push(tokio::spawn(async move {
            c.execute(Set::new(&k, v)).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Cleanup
    for i in 0..n {
        let k = key("multiplexed_concurrent", &format!("k{i}"));
        client.execute(Del::new(&k)).await.unwrap();
    }
}

#[tokio::test]
#[ignore]
async fn multiplexed_sentinel_connect_with_reconnect() {
    let addrs = sentinel_addrs().await;
    let client = MultiplexedSentinelClient::connect_with_reconnect(&addrs, "mymaster")
        .await
        .unwrap();
    let k = key("multiplexed_reconnect", "k");
    client.execute(Set::new(&k, "resilient")).await.unwrap();
    let val: Option<Bytes> = client.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("resilient")));
    client.execute(Del::new(&k)).await.unwrap();
}

// -- ConnectionPool<SentinelConnection> tests --

#[tokio::test]
#[ignore]
async fn sentinel_pool_set_and_get() {
    let addrs = sentinel_addrs().await;
    let pool = ConnectionPool::connect(3, || {
        let addrs = addrs.clone();
        async move { SentinelConnection::connect(&addrs, "mymaster").await }
    })
    .await
    .expect("failed to create sentinel pool");

    assert_eq!(pool.size(), 3);

    let k = key("pool_set_get", "k");
    pool.execute(Set::new(&k, "hello")).await.unwrap();
    let val: Option<Bytes> = pool.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("hello")));
    pool.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn sentinel_pool_concurrent_tasks() {
    let addrs = sentinel_addrs().await;
    let pool = ConnectionPool::connect(3, || {
        let addrs = addrs.clone();
        async move { SentinelConnection::connect(&addrs, "mymaster").await }
    })
    .await
    .expect("failed to create sentinel pool");

    let mut handles = Vec::new();
    for i in 0..16 {
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            let k = key("pool_concurrent", &format!("k{i}"));
            p.execute(Set::new(&k, format!("v{i}"))).await.unwrap();
            let v: Option<Bytes> = p.execute(Get::new(&k)).await.unwrap();
            assert_eq!(v, Some(Bytes::from(format!("v{i}"))));
            p.execute(Del::new(&k)).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
#[ignore]
async fn sentinel_pool_exhaustion_and_recovery() {
    // Verify that a pool with a single connection serializes concurrent callers
    // rather than failing. Each task should complete successfully even though
    // only one connection is available.
    let addrs = sentinel_addrs().await;
    let pool = ConnectionPool::connect(1, || {
        let addrs = addrs.clone();
        async move { SentinelConnection::connect(&addrs, "mymaster").await }
    })
    .await
    .expect("failed to create sentinel pool");

    let mut handles = Vec::new();
    for i in 0..8 {
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            let k = key("pool_exhaust", &format!("k{i}"));
            p.execute(Set::new(&k, format!("v{i}"))).await.unwrap();
            let v: Option<Bytes> = p.execute(Get::new(&k)).await.unwrap();
            assert_eq!(v, Some(Bytes::from(format!("v{i}"))));
            p.execute(Del::new(&k)).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

// -- Failover simulation --

// This test is destructive: it kills the master process, leaving the topology
// degraded with one fewer Redis node. Other sentinel tests that rely on the
// full 3-sentinel + 2-replica topology may behave differently if run after
// this test. CI runs sentinel tests single-threaded with --ignored and they
// share a static topology via OnceCell, so running this test last is safest.
#[tokio::test]
#[ignore]
async fn sentinel_failover_simulation() {
    let handle = ensure_sentinel().await;

    let addrs = sentinel_addrs().await;
    let client = SentinelClient::connect(&addrs, "mymaster").await.unwrap();

    // Verify initial connectivity.
    let k = key("failover", "k");
    client.execute(Set::new(&k, "before")).await.unwrap();
    let val: Option<Bytes> = client.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("before")));

    // Record the sentinel-reported master address before the failover.
    // handle.master_addr() returns the original static address of the master
    // RedisServerHandle, which does not change after a failover. We use poke()
    // to query the sentinel for the live master address instead.
    let initial_info = handle
        .poke()
        .await
        .expect("sentinel poke failed before kill");
    let initial_master = format!(
        "{}:{}",
        initial_info.get("ip").expect("no ip in sentinel response"),
        initial_info
            .get("port")
            .expect("no port in sentinel response"),
    );

    // Kill the master process (index 0 is always the master).
    let master_pid = handle.pids()[0];
    std::process::Command::new("kill")
        .args(["-9", &master_pid.to_string()])
        .status()
        .expect("kill failed");

    // Wait for sentinel to elect a new master. After the kill, the topology
    // has one fewer replica (the promoted node is now master), so we poll
    // the sentinel directly for a master with flags="master" rather than
    // relying on the full-replica-count health check in wait_for_healthy.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let new_master = loop {
        let info = handle.poke().await;
        if let Ok(info) = info {
            let flags = info.get("flags").map(String::as_str).unwrap_or("");
            if flags == "master" {
                let addr = format!(
                    "{}:{}",
                    info.get("ip").expect("no ip in sentinel response"),
                    info.get("port").expect("no port in sentinel response"),
                );
                if addr != initial_master {
                    break addr;
                }
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sentinel did not elect a new master within 30 seconds"
        );
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };

    // Force the client to rediscover the current master.
    client.rediscover().await.unwrap();

    // Verify commands succeed on the promoted master.
    let k2 = key("failover", "k2");
    client.execute(Set::new(&k2, "after")).await.unwrap();
    let val2: Option<Bytes> = client.execute(Get::new(&k2)).await.unwrap();
    assert_eq!(val2, Some(Bytes::from("after")));

    // The promoted master must be a different node than the original.
    assert_ne!(
        initial_master, new_master,
        "expected a different master after failover"
    );
}
