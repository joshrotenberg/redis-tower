//! Cluster integration tests using redis-test-harness.
//!
//! These tests start a real 3-node Redis Cluster via local redis-server
//! processes. Requires `redis-server` and `redis-cli` on PATH.
//!
//! Run with: `cargo test -p redis-tower-cluster --test cluster_integration -- --ignored`

use std::sync::OnceLock;

use bytes::Bytes;
use redis_test_harness::cluster::{ClusterConfig, RedisCluster};
use redis_tower_cluster::ClusterConnection;
use redis_tower_commands::*;

/// Shared cluster instance -- started once, stopped on Drop.
static CLUSTER: OnceLock<RedisCluster> = OnceLock::new();

fn ensure_cluster() -> &'static RedisCluster {
    CLUSTER.get_or_init(|| {
        let cluster = RedisCluster::new(ClusterConfig {
            masters: 3,
            replicas_per_master: 0,
            ..Default::default()
        });
        cluster.start().expect("failed to start Redis cluster");
        cluster
            .wait_for_healthy(std::time::Duration::from_secs(10))
            .expect("cluster not healthy");
        cluster
    })
}

async fn cluster_conn() -> ClusterConnection {
    let cluster = ensure_cluster();
    let addr = format!("{}:{}", cluster.config().bind, cluster.config().base_port);
    ClusterConnection::connect(&addr)
        .await
        .expect("failed to connect to cluster")
}

fn key(test: &str, name: &str) -> String {
    format!("cluster_test:{test}:{name}")
}

#[tokio::test]
#[ignore]
async fn cluster_set_and_get() {
    let mut cluster = cluster_conn().await;
    let k = key("set_get", "k");
    cluster.execute(Set::new(&k, "hello")).await.unwrap();
    let val = cluster.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("hello")));
    cluster.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn cluster_routes_to_different_nodes() {
    let mut cluster = cluster_conn().await;
    let k1 = key("routing", "foo");
    let k2 = key("routing", "bar");

    cluster.execute(Set::new(&k1, "v1")).await.unwrap();
    cluster.execute(Set::new(&k2, "v2")).await.unwrap();

    let v1 = cluster.execute(Get::new(&k1)).await.unwrap();
    let v2 = cluster.execute(Get::new(&k2)).await.unwrap();
    assert_eq!(v1, Some(Bytes::from("v1")));
    assert_eq!(v2, Some(Bytes::from("v2")));

    cluster.execute(Del::new(&k1)).await.unwrap();
    cluster.execute(Del::new(&k2)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn cluster_hash_tag_same_slot() {
    let mut cluster = cluster_conn().await;
    let k1 = "{user:1}:name";
    let k2 = "{user:1}:email";

    cluster.execute(Set::new(k1, "Alice")).await.unwrap();
    cluster
        .execute(Set::new(k2, "alice@example.com"))
        .await
        .unwrap();

    let v1 = cluster.execute(Get::new(k1)).await.unwrap();
    let v2 = cluster.execute(Get::new(k2)).await.unwrap();
    assert_eq!(v1, Some(Bytes::from("Alice")));
    assert_eq!(v2, Some(Bytes::from("alice@example.com")));

    cluster.execute(Del::new(k1)).await.unwrap();
    cluster.execute(Del::new(k2)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn cluster_ping() {
    let mut cluster = cluster_conn().await;
    let pong = cluster.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

#[tokio::test]
#[ignore]
async fn cluster_incr() {
    let mut cluster = cluster_conn().await;
    let k = key("incr", "counter");
    cluster.execute(Del::new(&k)).await.unwrap();
    let v = cluster.execute(Incr::new(&k)).await.unwrap();
    assert_eq!(v, 1);
    let v = cluster.execute(Incr::new(&k)).await.unwrap();
    assert_eq!(v, 2);
    cluster.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn cluster_topology_has_three_masters() {
    let cluster = cluster_conn().await;
    let topo = cluster.topology();
    assert_eq!(
        topo.master_addrs().len(),
        3,
        "expected 3 master nodes, got {}",
        topo.master_addrs().len()
    );
}

#[tokio::test]
#[ignore]
async fn cluster_hashes() {
    let mut cluster = cluster_conn().await;
    let k = key("hashes", "h");
    cluster
        .execute(HSet::new(&k, "field1", "value1"))
        .await
        .unwrap();
    let val = cluster.execute(HGet::new(&k, "field1")).await.unwrap();
    assert_eq!(val, Some(Bytes::from("value1")));
    cluster.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn cluster_lists() {
    let mut cluster = cluster_conn().await;
    let k = key("lists", "l");
    cluster.execute(Del::new(&k)).await.unwrap();
    cluster.execute(RPush::new(&k, "a")).await.unwrap();
    cluster.execute(RPush::new(&k, "b")).await.unwrap();
    let items = cluster.execute(LRange::new(&k, 0, -1)).await.unwrap();
    assert_eq!(items, vec![Bytes::from("a"), Bytes::from("b")]);
    cluster.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn cluster_sets() {
    let mut cluster = cluster_conn().await;
    let k = key("sets", "s");
    cluster.execute(Del::new(&k)).await.unwrap();
    cluster
        .execute(SAdd::members(&k, ["x", "y", "z"]))
        .await
        .unwrap();
    assert_eq!(cluster.execute(SCard::new(&k)).await.unwrap(), 3);
    cluster.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn cluster_sorted_sets() {
    let mut cluster = cluster_conn().await;
    let k = key("zsets", "z");
    cluster.execute(Del::new(&k)).await.unwrap();
    cluster
        .execute(ZAdd::new(&k).member(1.0, "a").member(2.0, "b"))
        .await
        .unwrap();
    let range = cluster.execute(ZRange::new(&k, 0, -1)).await.unwrap();
    assert_eq!(range, vec![Bytes::from("a"), Bytes::from("b")]);
    cluster.execute(Del::new(&k)).await.unwrap();
}
