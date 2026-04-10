//! Cluster integration tests.
//!
//! Run with: `cargo test -p redis-tower-cluster --test cluster_integration -- --ignored`

use std::sync::OnceLock;

use bytes::Bytes;
use redis_test_harness::cluster::{ClusterConfig, RedisCluster};
use redis_tower_cluster::{ClusterConnection, MultiplexedClusterClient};
use redis_tower_commands::*;

static CLUSTER: OnceLock<RedisCluster> = OnceLock::new();

fn ensure_cluster() -> &'static RedisCluster {
    CLUSTER.get_or_init(|| {
        let mut cluster = RedisCluster::new(ClusterConfig {
            masters: 3,
            replicas_per_master: 0,
            ..Default::default()
        });
        // Stop any leftover nodes from a previous run.
        let _ = cluster.stop();
        std::thread::sleep(std::time::Duration::from_millis(500));
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

async fn mux_cluster_conn() -> MultiplexedClusterClient {
    let cluster = ensure_cluster();
    let addr = format!("{}:{}", cluster.config().bind, cluster.config().base_port);
    MultiplexedClusterClient::connect(&addr)
        .await
        .expect("failed to connect to multiplexed cluster")
}

// Generate the shared command tests for cluster.
redis_test_harness::command_tests!(cluster_conn, "cluster_cmd", ignored);

// Replay the shared command tests against the multiplexed cluster client.
mod multiplexed {
    use super::*;
    redis_test_harness::command_tests!(mux_cluster_conn, "mux_cluster_cmd", ignored);
}

// -- Cluster-specific tests --

#[tokio::test]
#[ignore]
async fn cluster_topology_has_three_masters() {
    let cluster = cluster_conn().await;
    let topo = cluster.topology();
    assert_eq!(topo.master_addrs().len(), 3);
}

#[tokio::test]
#[ignore]
async fn cluster_routes_to_different_nodes() {
    let mut cluster = cluster_conn().await;
    let k1 = "cluster_routing:foo";
    let k2 = "cluster_routing:bar";

    cluster.execute(Set::new(k1, "v1")).await.unwrap();
    cluster.execute(Set::new(k2, "v2")).await.unwrap();

    let v1 = cluster.execute(Get::new(k1)).await.unwrap();
    let v2 = cluster.execute(Get::new(k2)).await.unwrap();
    assert_eq!(v1, Some(Bytes::from("v1")));
    assert_eq!(v2, Some(Bytes::from("v2")));

    cluster.execute(Del::new(k1)).await.unwrap();
    cluster.execute(Del::new(k2)).await.unwrap();
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

// -- MultiplexedClusterClient-specific tests --

#[tokio::test]
#[ignore]
async fn mux_cluster_topology_has_three_masters() {
    let cluster = mux_cluster_conn().await;
    let topo = cluster.topology().await;
    assert_eq!(topo.master_addrs().len(), 3);
}

#[tokio::test]
#[ignore]
async fn mux_cluster_concurrent_writes_from_many_tasks() {
    // With the multiplexed client, dozens of tasks should share the per-node
    // workers and all make progress. This exercises the "clone the client,
    // spawn many tasks" usage that justified the whole design.
    let cluster = mux_cluster_conn().await;
    let mut handles = Vec::new();
    for i in 0..64 {
        let c = cluster.clone();
        handles.push(tokio::spawn(async move {
            let k = format!("mux_cluster_concurrent:{i}");
            c.execute(Set::new(&k, format!("v{i}"))).await.unwrap();
            let v = c.execute(Get::new(&k)).await.unwrap();
            assert_eq!(v, Some(Bytes::from(format!("v{i}"))));
            c.execute(Del::new(&k)).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
#[ignore]
async fn mux_cluster_refresh_topology() {
    let cluster = mux_cluster_conn().await;
    cluster
        .refresh_topology()
        .await
        .expect("refresh should succeed on a healthy cluster");
    let topo = cluster.topology().await;
    assert_eq!(topo.master_addrs().len(), 3);
}
