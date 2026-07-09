//! Cluster integration tests.
//!
//! Run with: `cargo test -p redis-tower-cluster --test cluster_integration -- --ignored`

use bytes::Bytes;
use redis_server_wrapper::{RedisCluster, RedisClusterHandle};
use redis_tower::pool::ConnectionPool;
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
redis_tower_test::command_tests!(cluster_conn, "cluster_cmd", ignored);

// Replay the shared command tests against the multiplexed cluster client.
mod multiplexed {
    use super::*;
    redis_tower_test::command_tests!(mux_cluster_conn, "mux_cluster_cmd", ignored);
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

/// Large values round-trip through the cluster connection (#479). Single-key,
/// so it stays within one slot; exercises the per-node codec at MB scale.
#[tokio::test]
#[ignore]
async fn cluster_large_value_roundtrip() {
    let mut cluster = cluster_conn().await;
    let key = "cluster:large:64mb";
    let _ = cluster.execute(Del::new(key)).await;

    let value = "v".repeat(64 * 1024 * 1024);
    cluster.execute(Set::new(key, value.clone())).await.unwrap();
    let got = cluster
        .execute(Get::new(key))
        .await
        .unwrap()
        .expect("value should be present");
    assert_eq!(got.len(), value.len(), "cluster: 64MB round-trip length");
    assert_eq!(
        got.as_ref(),
        value.as_bytes(),
        "cluster: 64MB round-trip bytes"
    );
    cluster.execute(Del::new(key)).await.unwrap();
}

/// A 1000-member HGETALL through the cluster connection (#479).
#[tokio::test]
#[ignore]
async fn cluster_large_hgetall() {
    let mut cluster = cluster_conn().await;
    let key = "cluster:large:hash";
    let _ = cluster.execute(Del::new(key)).await;

    let fields = (0..1000).map(|i| (format!("f{i}"), format!("v{i}")));
    cluster
        .execute(HSet::from_fields(key, fields))
        .await
        .unwrap();
    let all = cluster.execute(HGetAll::new(key)).await.unwrap();
    assert_eq!(
        all.len(),
        1000,
        "cluster: HGETALL should return 1000 members"
    );
    cluster.execute(Del::new(key)).await.unwrap();
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

/// A clone keeps working after another clone calls `shutdown()`; only the last
/// live clone actually drains the per-node workers.
#[tokio::test]
#[ignore]
async fn mux_cluster_shutdown_drains_and_last_clone_wins() {
    let cluster = mux_cluster_conn().await;
    let clone = cluster.clone();

    // Run a command so the per-node workers are live, then shut down one clone.
    cluster
        .execute(Set::new("mux_cluster_shutdown", "v"))
        .await
        .unwrap();

    // `cluster` is not the last clone, so this returns immediately and leaves
    // the shared workers running for `clone`.
    cluster.shutdown().await;
    let v = clone
        .execute(Get::new("mux_cluster_shutdown"))
        .await
        .unwrap();
    assert_eq!(v, Some(Bytes::from("v")));
    clone
        .execute(Del::new("mux_cluster_shutdown"))
        .await
        .unwrap();

    // The last clone drains the workers cleanly.
    clone.shutdown().await;
}

/// The kill-a-master test a customer evaluation runs first: a master dies and
/// the client must keep serving the rest of the cluster instead of wedging.
///
/// Before this change a per-node worker reconnected to the dead address
/// forever -- nothing triggered a topology refresh -- so the whole client could
/// stall. Now the failure triggers a background self-healing refresh that
/// reconciles the per-node services (replacing dead workers, pruning departed
/// nodes), and commands to the surviving masters keep succeeding.
///
/// Uses a dedicated cluster with `cluster-require-full-coverage no`, so the
/// surviving masters keep serving their own slots after one master dies. That
/// makes the assertion deterministic: we verify the live part of the cluster
/// stays usable through the client, without depending on a replica election
/// (the prune/replace/promote reconciliation itself is unit-tested in
/// `multiplexed::diff_tests`).
#[tokio::test]
#[ignore = "destructive: starts a dedicated cluster and kills a master"]
async fn mux_cluster_survives_master_kill_without_wedging() {
    use redis_server_wrapper::chaos;
    use std::time::Duration;

    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(1)
        .base_port(17500)
        .cluster_node_timeout(2000)
        .cluster_require_full_coverage(false)
        .start()
        .await
        .expect("failed to start cluster");

    let client = MultiplexedClusterClient::connect(&cluster.addr())
        .await
        .expect("failed to connect");

    // Seed keys spread across all three masters; confirm they read back first.
    let keys: Vec<String> = (0..24).map(|i| format!("heal:{i}")).collect();
    for k in &keys {
        client.execute(Set::new(k, "v")).await.expect("initial set");
    }

    // Kill one master. Its ~1/3 of slots become unservable (no election here),
    // but the other two masters keep serving theirs.
    chaos::kill_master_by_key(&cluster, &keys[0])
        .await
        .expect("failed to kill master");

    // A self-healing client keeps the surviving masters usable: after a brief
    // settle, a healthy majority of keys keep reading back, poll after poll. A
    // client that loops on the dead address would serve nothing.
    let mut best = 0usize;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut ok = 0usize;
        for k in &keys {
            if let Ok(Ok(_)) =
                tokio::time::timeout(Duration::from_secs(2), client.execute(Get::new(k))).await
            {
                ok += 1;
            }
        }
        best = best.max(ok);
        // Two of three masters' worth of keys is the success bar.
        if ok >= 14 {
            break;
        }
    }

    assert!(
        best >= 14,
        "client served only {best}/24 keys after a master was killed; it wedged the live cluster"
    );
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
}

/// The plain `ClusterConnection` had no auth path at all, so every
/// password-protected cluster was unreachable. Verify `.credentials()` on the
/// builder and `connect_url` (password-only `redis://:pass@`) both authenticate.
#[tokio::test]
#[ignore]
async fn cluster_connection_credentials_and_connect_url() {
    use redis_tower::credentials::StaticCredentials;

    let cluster = RedisCluster::builder()
        .masters(3)
        .replicas_per_master(0)
        .base_port(17600)
        .password("cluster-secret")
        .start()
        .await
        .expect("failed to start auth cluster");
    let seed = cluster.addr();

    // A bare connect (no credentials) must fail on an auth cluster.
    assert!(
        ClusterConnection::connect(&seed).await.is_err(),
        "bare connect should fail on an auth cluster"
    );

    // Builder .credentials() authenticates every node connection.
    let mut conn = ClusterConnection::builder(&seed)
        .credentials(StaticCredentials::password("cluster-secret"))
        .connect()
        .await
        .expect("connect with credentials should succeed");
    conn.execute(Set::new("cc_auth:k", "v")).await.unwrap();
    assert_eq!(
        conn.execute(Get::new("cc_auth:k")).await.unwrap(),
        Some(Bytes::from("v"))
    );

    // connect_url wires the same auth from a redis:// URL.
    let url = format!("redis://:cluster-secret@{seed}");
    let mut via_url = ClusterConnection::connect_url(&url)
        .await
        .expect("ClusterConnection::connect_url should authenticate");
    assert_eq!(
        via_url.execute(Get::new("cc_auth:k")).await.unwrap(),
        Some(Bytes::from("v"))
    );

    // The multiplexed client's connect_url authenticates too.
    let mux = MultiplexedClusterClient::connect_url(&url)
        .await
        .expect("MultiplexedClusterClient::connect_url should authenticate");
    assert_eq!(
        mux.execute(Get::new("cc_auth:k")).await.unwrap(),
        Some(Bytes::from("v"))
    );

    conn.execute(Del::new("cc_auth:k")).await.unwrap();
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

// -- ConnectionPool<ClusterConnection> tests --

#[tokio::test]
#[ignore]
async fn cluster_pool_set_and_get() {
    let cluster = ensure_cluster().await;
    let addr = cluster.addr();
    let pool = ConnectionPool::connect(3, || {
        let addr = addr.clone();
        async move { ClusterConnection::connect(&addr).await }
    })
    .await
    .expect("failed to create cluster pool");

    assert_eq!(pool.size(), 3);

    let k = "cluster_pool:set_get";
    pool.execute(Set::new(k, "hello")).await.unwrap();
    let val: Option<Bytes> = pool.execute(Get::new(k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("hello")));
    pool.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn cluster_pool_concurrent_tasks() {
    let cluster = ensure_cluster().await;
    let addr = cluster.addr();
    let pool = ConnectionPool::connect(3, || {
        let addr = addr.clone();
        async move { ClusterConnection::connect(&addr).await }
    })
    .await
    .expect("failed to create cluster pool");

    let mut handles = Vec::new();
    for i in 0..16 {
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            let k = format!("cluster_pool_concurrent:{i}");
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
async fn cluster_pool_exhaustion_and_recovery() {
    // Verify that a pool with a single connection serializes concurrent callers
    // rather than failing. Each task should complete successfully even though
    // only one connection is available.
    let cluster = ensure_cluster().await;
    let addr = cluster.addr();
    let pool = ConnectionPool::connect(1, || {
        let addr = addr.clone();
        async move { ClusterConnection::connect(&addr).await }
    })
    .await
    .expect("failed to create cluster pool");

    let mut handles = Vec::new();
    for i in 0..8 {
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            let k = format!("cluster_pool_exhaust:{i}");
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
