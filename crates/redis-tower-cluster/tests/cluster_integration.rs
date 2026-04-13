//! Cluster integration tests.
//!
//! Run with: `cargo test -p redis-tower-cluster --test cluster_integration -- --ignored`

use bytes::Bytes;
use redis_server_wrapper::{RedisCluster, RedisClusterHandle};
use redis_tower_cluster::{ClusterConnection, MultiplexedClusterClient};
use redis_tower_commands::*;
use tokio::sync::OnceCell;

static CLUSTER: OnceCell<RedisClusterHandle> = OnceCell::const_new();

async fn ensure_cluster() -> &'static RedisClusterHandle {
    CLUSTER
        .get_or_init(|| async {
            // Use 17200..17202 instead of the default 7000..7002 to
            // avoid conflicts with macOS Control Center on port 7000.
            RedisCluster::builder()
                .masters(3)
                .replicas_per_master(0)
                .base_port(17200)
                .start()
                .await
                .expect("failed to start Redis cluster")
        })
        .await
}

async fn cluster_conn() -> ClusterConnection {
    let cluster = ensure_cluster().await;
    ClusterConnection::connect(&cluster.addr())
        .await
        .expect("failed to connect to cluster")
}

async fn mux_cluster_conn() -> MultiplexedClusterClient {
    let cluster = ensure_cluster().await;
    MultiplexedClusterClient::connect(&cluster.addr())
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
    use redis_tower::credentials::StaticCredentials;

    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17300)
        .password("cluster-secret")
        .start()
        .await
        .expect("failed to start auth cluster");

    let seed = cluster.addr();

    // Without credentials, connect must fail.
    let no_auth = MultiplexedClusterClient::connect(&seed).await;
    assert!(
        no_auth.is_err(),
        "connect without credentials should fail on an auth cluster"
    );

    // With credentials via the builder, connect should succeed.
    let client = MultiplexedClusterClient::builder(&seed)
        .credentials(StaticCredentials::password("cluster-secret"))
        .connect()
        .await
        .expect("connect with credentials should succeed");

    for i in 0..16 {
        let k = format!("mux_cluster_auth:{i}");
        client.execute(Set::new(&k, format!("v{i}"))).await.unwrap();
        let v = client.execute(Get::new(&k)).await.unwrap();
        assert_eq!(v, Some(Bytes::from(format!("v{i}"))));
        client.execute(Del::new(&k)).await.unwrap();
    }

    // Shut down nodes via redis-cli and leak the handle to prevent
    // RedisServerHandle::stop() from running kill_by_port, which can
    // SIGKILL the test binary via open sockets (redis-server-wrapper#76).
    drop(client);
    drop(no_auth);
    for port in 17300..17303 {
        let _ = std::process::Command::new("redis-cli")
            .args([
                "-h",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "-a",
                "cluster-secret",
                "SHUTDOWN",
                "NOSAVE",
            ])
            .output();
    }
    std::mem::forget(cluster);
}

// -- TLS cluster tests --
//
// These spin up a TLS-enabled cluster automatically using self-signed
// certificates generated by redis-server-wrapper. No external infrastructure
// or env vars needed.
//
// Run with:
//   cargo test -p redis-tower-cluster --features tls-rustls \
//       --test cluster_integration -- --ignored --test-threads=1

#[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
static TLS_CLUSTER: OnceCell<Option<RedisClusterHandle>> = OnceCell::const_new();

/// Try to start a TLS cluster. Returns `None` if redis-server was not
/// compiled with TLS support (e.g. missing `BUILD_TLS=yes`).
#[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
async fn ensure_tls_cluster() -> Option<&'static RedisClusterHandle> {
    TLS_CLUSTER
        .get_or_init(|| async {
            let certs_dir = std::path::PathBuf::from("/tmp/redis-cluster-tls-integration/certs");
            let certs = match redis_server_wrapper::tls::generate_test_certs(&certs_dir) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("skipping TLS tests: failed to generate certs: {e}");
                    return None;
                }
            };

            match RedisCluster::builder()
                .masters(3)
                .replicas_per_master(0)
                .base_port(17400)
                .tls_port(17400)
                .tls_cert_file(&certs.cert_file)
                .tls_key_file(&certs.key_file)
                .tls_ca_cert_file(&certs.ca_cert_file)
                .tls_auth_clients(false)
                .tls_replication(true)
                .tls_cluster(true)
                .start()
                .await
            {
                Ok(cluster) => Some(cluster),
                Err(e) => {
                    eprintln!("skipping TLS tests: failed to start TLS cluster: {e}");
                    None
                }
            }
        })
        .await
        .as_ref()
}

#[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
fn tls_config_for_test() -> redis_tower_core::tls::TlsConfig {
    #[cfg(feature = "tls-rustls")]
    let tls = redis_tower_core::tls::TlsConfig::default_rustls();
    #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
    let tls = redis_tower_core::tls::TlsConfig::default_native_tls();

    tls.danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
}

#[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
#[tokio::test]
#[ignore = "requires redis-server with TLS support"]
async fn mux_cluster_tls_connect_and_roundtrip() {
    let Some(cluster) = ensure_tls_cluster().await else {
        eprintln!("skipping: redis-server not compiled with TLS support");
        return;
    };
    let addr = cluster.addr();

    let client = MultiplexedClusterClient::builder(&addr)
        .tls(tls_config_for_test())
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

#[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
#[tokio::test]
#[ignore = "requires redis-server with TLS support"]
async fn cluster_connection_tls_connect_and_roundtrip() {
    let Some(cluster) = ensure_tls_cluster().await else {
        eprintln!("skipping: redis-server not compiled with TLS support");
        return;
    };
    let addr = cluster.addr();

    let mut conn = ClusterConnection::builder(&addr)
        .tls(tls_config_for_test())
        .connect()
        .await
        .expect("TLS connect should succeed");

    let topo = conn.topology();
    assert!(
        !topo.master_addrs().is_empty(),
        "TLS cluster reported no masters"
    );

    for i in 0..16 {
        let k = format!("cluster_conn_tls:{i}");
        conn.execute(Set::new(&k, format!("v{i}"))).await.unwrap();
        let v: Option<Bytes> = conn.execute(Get::new(&k)).await.unwrap();
        assert_eq!(v, Some(Bytes::from(format!("v{i}"))));
        conn.execute(Del::new(&k)).await.unwrap();
    }
}
