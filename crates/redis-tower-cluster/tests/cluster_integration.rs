//! Cluster integration tests.
//!
//! Run with: `cargo test -p redis-tower-cluster --test cluster_integration -- --ignored`

use std::sync::OnceLock;

use bytes::Bytes;
use redis_test_harness::cluster::{ClusterConfig, RedisCluster};
use redis_tower_cluster::ClusterConnection;
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

// Generate the shared command tests for cluster.
redis_test_harness::command_tests!(cluster_conn, "cluster_cmd", ignored);

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
