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
        // Use 17200..17202 instead of the harness default 7000..7002 to
        // avoid conflicts with macOS Control Center, which opportunistically
        // binds port 7000 as "afs3-fileserver" and makes local cluster
        // tests flaky. Kept distinct from cluster-bench (17000..) and the
        // credentials test (7100..) to allow parallel runs.
        let mut cluster = RedisCluster::new(ClusterConfig {
            masters: 3,
            replicas_per_master: 0,
            base_port: 17200,
            work_dir: std::path::PathBuf::from("/tmp/redis-cluster-integration"),
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

#[tokio::test]
#[ignore]
async fn mux_cluster_credentials_authenticate_on_connect() {
    // Dedicated 3-master cluster with `requirepass` set, on its own port
    // range so we don't disturb the shared CLUSTER in other tests.
    use redis_tower::credentials::StaticCredentials;
    use std::collections::HashMap;

    let mut extra = HashMap::new();
    extra.insert("requirepass".to_string(), "cluster-secret".to_string());
    extra.insert("masterauth".to_string(), "cluster-secret".to_string());

    let mut cluster = RedisCluster::new(ClusterConfig {
        masters: 3,
        replicas_per_master: 0,
        base_port: 17300,
        work_dir: std::path::PathBuf::from("/tmp/redis-cluster-auth"),
        extra_config: extra,
        ..Default::default()
    });
    let _ = cluster.stop();
    std::thread::sleep(std::time::Duration::from_millis(500));
    cluster.start().expect("failed to start auth cluster");
    cluster
        .wait_for_healthy(std::time::Duration::from_secs(10))
        .expect("auth cluster not healthy");

    let seed = format!("{}:{}", cluster.config().bind, cluster.config().base_port);

    // Without credentials, connect must fail -- AUTH is required before
    // CLUSTER SLOTS can run.
    let no_auth = MultiplexedClusterClient::connect(&seed).await;
    assert!(
        no_auth.is_err(),
        "connect without credentials should fail on an auth cluster"
    );

    // With credentials via the builder, connect should succeed and commands
    // should work across all three masters.
    let client = MultiplexedClusterClient::builder(&seed)
        .credentials(StaticCredentials::password("cluster-secret"))
        .connect()
        .await
        .expect("connect with credentials should succeed");

    // Spread writes across slots so we actually exercise multiple nodes.
    for i in 0..16 {
        let k = format!("mux_cluster_auth:{i}");
        client.execute(Set::new(&k, format!("v{i}"))).await.unwrap();
        let v = client.execute(Get::new(&k)).await.unwrap();
        assert_eq!(v, Some(Bytes::from(format!("v{i}"))));
        client.execute(Del::new(&k)).await.unwrap();
    }

    let _ = cluster.stop();
}

// -- TLS cluster tests (infrastructure-dependent) --
//
// These are `#[ignore]`d by default because an automatic TLS-enabled test
// cluster would need self-signed cert generation and redis-server TLS
// config in the harness. Point REDIS_TLS_CLUSTER_ADDR at a running
// TLS-enabled cluster and run with `--ignored` to exercise the TLS path.
// Build with a TLS feature, e.g.:
//
//   cargo test -p redis-tower-cluster --features tls-rustls \
//       --test cluster_integration mux_cluster_tls -- --ignored
//
// If the cluster uses a self-signed cert, set REDIS_TLS_ACCEPT_INVALID=1
// to skip certificate verification.

#[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
#[tokio::test]
#[ignore = "requires a TLS-enabled Redis cluster -- set REDIS_TLS_CLUSTER_ADDR"]
async fn mux_cluster_tls_connect_and_roundtrip() {
    use redis_tower_core::tls::TlsConfig;

    // This test lives in the cluster integration binary, which CI runs with
    // `--ignored` (because every cluster test is ignored by default). If
    // REDIS_TLS_CLUSTER_ADDR isn't set we have no TLS cluster to talk to, so
    // skip cleanly instead of failing CI. Set the env var locally or in a
    // dedicated job to actually exercise the TLS path.
    let addr = match std::env::var("REDIS_TLS_CLUSTER_ADDR") {
        Ok(a) => a,
        Err(_) => {
            eprintln!(
                "skipping mux_cluster_tls_connect_and_roundtrip: \
                 REDIS_TLS_CLUSTER_ADDR not set"
            );
            return;
        }
    };
    let accept_invalid = std::env::var("REDIS_TLS_ACCEPT_INVALID")
        .ok()
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));

    #[cfg(feature = "tls-rustls")]
    let mut tls = TlsConfig::default_rustls();
    #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
    let mut tls = TlsConfig::default_native_tls();

    if accept_invalid {
        tls = tls
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true);
    }

    let client = MultiplexedClusterClient::builder(&addr)
        .tls(tls)
        .connect()
        .await
        .expect("TLS connect should succeed");

    let topo = client.topology().await;
    assert!(
        !topo.master_addrs().is_empty(),
        "TLS cluster reported no masters"
    );

    for i in 0..16 {
        let k = format!("mux_cluster_tls:{i}");
        client.execute(Set::new(&k, format!("v{i}"))).await.unwrap();
        let v = client.execute(Get::new(&k)).await.unwrap();
        assert_eq!(v, Some(Bytes::from(format!("v{i}"))));
        client.execute(Del::new(&k)).await.unwrap();
    }
}
