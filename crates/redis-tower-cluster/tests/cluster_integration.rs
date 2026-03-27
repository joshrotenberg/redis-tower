//! Cluster integration tests.
//!
//! These tests require a running Redis Cluster. They are ignored by default.
//! Run with: `cargo test -p redis-tower-cluster --test cluster_integration -- --ignored`
//!
//! To set up a cluster manually:
//! ```sh
//! # Using docker-wrapper's RedisClusterTemplate, or manually with redis-cli --cluster create
//! ```
//!
//! Set `REDIS_CLUSTER_ADDR` to the seed node address (default: 127.0.0.1:7000).

use bytes::Bytes;
use redis_tower_cluster::ClusterConnection;
use redis_tower_commands::*;

async fn cluster() -> ClusterConnection {
    let addr = std::env::var("REDIS_CLUSTER_ADDR").unwrap_or_else(|_| "127.0.0.1:7000".to_string());
    // Use host override for Docker-based clusters where nodes announce container IPs.
    ClusterConnection::connect_with_host(&addr, "127.0.0.1")
        .await
        .expect("failed to connect to cluster")
}

fn key(test: &str, name: &str) -> String {
    format!("cluster_test:{test}:{name}")
}

#[tokio::test]
#[ignore]
async fn cluster_set_and_get() {
    let cluster = cluster().await;
    let k = key("set_get", "k");
    cluster.execute(Set::new(&k, "hello")).await.unwrap();
    let val = cluster.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("hello")));
    cluster.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn cluster_routes_to_different_nodes() {
    let cluster = cluster().await;
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
    let cluster = cluster().await;
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
    let cluster = cluster().await;
    let pong = cluster.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

#[tokio::test]
#[ignore]
async fn cluster_incr() {
    let cluster = cluster().await;
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
    let cluster = cluster().await;
    let topo = cluster.topology();
    assert_eq!(
        topo.master_addrs().len(),
        3,
        "expected 3 master nodes, got {}",
        topo.master_addrs().len()
    );
}
