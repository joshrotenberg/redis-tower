//! Cluster integration tests.
//!
//! Run with: `cargo test -p redis-tower-cluster --test cluster_integration -- --ignored`

use bytes::Bytes;
use redis_server_wrapper::{RedisCluster, RedisClusterHandle};
use redis_tower::pool::ConnectionPool;
use redis_tower_cluster::{ClusterConnection, MultiplexedClusterClient, slot_for_key};
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

// -- MOVED redirect tests (#327) --

#[tokio::test]
#[ignore]
async fn cluster_moved_redirect_transparent() {
    // "foo", "hello", and "bar" hash to distinct slots. With 3 masters
    // covering ~5461 slots each, ClusterConnection.execute() routes by slot
    // and follows MOVED transparently if any routing decision is stale.
    let mut conn = cluster_conn().await;

    // Write keys known to be on different slots.
    conn.execute(Set::new("foo", "foo_val")).await.unwrap();
    conn.execute(Set::new("hello", "hello_val")).await.unwrap();
    conn.execute(Set::new("bar", "bar_val")).await.unwrap();

    // Read them back -- any routing error returns a MOVED which execute() follows.
    let v1: Option<Bytes> = conn.execute(Get::new("foo")).await.unwrap();
    let v2: Option<Bytes> = conn.execute(Get::new("hello")).await.unwrap();
    let v3: Option<Bytes> = conn.execute(Get::new("bar")).await.unwrap();

    assert_eq!(v1, Some(Bytes::from("foo_val")));
    assert_eq!(v2, Some(Bytes::from("hello_val")));
    assert_eq!(v3, Some(Bytes::from("bar_val")));

    // Verify the three keys land on different slots.
    let s1 = slot_for_key(b"foo");
    let s2 = slot_for_key(b"hello");
    let s3 = slot_for_key(b"bar");
    assert_ne!(s1, s2);
    assert_ne!(s2, s3);
    assert_ne!(s1, s3);

    // Cleanup.
    conn.execute(Del::new("foo")).await.unwrap();
    conn.execute(Del::new("hello")).await.unwrap();
    conn.execute(Del::new("bar")).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn cluster_moved_updates_topology() {
    let mut conn = cluster_conn().await;
    let key = "cluster_topo_update_test";

    // Write via correct routing.
    conn.execute(Set::new(key, "val")).await.unwrap();

    // Find which node currently owns this key's slot.
    let slot = slot_for_key(key.as_bytes());
    let original_master = conn.topology().master_for_slot(slot).unwrap().clone();

    // Find a master that is NOT the current owner of this slot.
    let wrong_master = conn
        .topology()
        .master_addrs()
        .into_iter()
        .find(|addr| **addr != original_master)
        .unwrap()
        .clone();

    // Corrupt the topology: point this slot's range at the wrong master.
    for range in conn.topology_mut().slot_ranges.iter_mut() {
        if slot >= range.start && slot <= range.end {
            range.master = wrong_master.clone();
            break;
        }
    }

    // Topology is now stale (pointing at the wrong node).
    assert_eq!(
        conn.topology().master_for_slot(slot).unwrap(),
        &wrong_master
    );

    // Execute should succeed: the wrong node returns MOVED and execute()
    // follows it, patching the slot map back to the real owner.
    let v: Option<Bytes> = conn.execute(Get::new(key)).await.unwrap();
    assert_eq!(v, Some(Bytes::from("val")));

    // After the redirect, topology should reflect the real owner again.
    let updated_master = conn.topology().master_for_slot(slot).unwrap().clone();
    assert_eq!(updated_master, original_master);

    // Cleanup.
    conn.execute(Del::new(key)).await.unwrap();
}

// -- CROSSSLOT tests (#333) --

#[tokio::test]
#[ignore]
async fn cluster_crossslot_mget_returns_error() {
    let mut conn = cluster_conn().await;
    // "foo" and "hello" land on different slots/shards.
    let result = conn.execute(MGet::new(["foo", "hello"])).await;
    assert!(result.is_err(), "expected CROSSSLOT error, got Ok");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("CROSSSLOT"),
        "expected CROSSSLOT in error, got: {err}"
    );
}

#[tokio::test]
#[ignore]
async fn cluster_crossslot_mset_returns_error() {
    let mut conn = cluster_conn().await;
    let result = conn
        .execute(MSet::new([("foo", "v1"), ("hello", "v2")]))
        .await;
    assert!(result.is_err(), "expected CROSSSLOT error, got Ok");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("CROSSSLOT"),
        "expected CROSSSLOT in error, got: {err}"
    );
}

#[tokio::test]
#[ignore]
async fn cluster_crossslot_del_returns_error() {
    let mut conn = cluster_conn().await;
    // Multi-key Del uses Del::keys (Del::new takes a single key).
    let result = conn.execute(Del::keys(["foo", "hello"])).await;
    assert!(result.is_err(), "expected CROSSSLOT error, got Ok");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("CROSSSLOT"),
        "expected CROSSSLOT in error, got: {err}"
    );
}

#[tokio::test]
#[ignore]
async fn cluster_hash_tag_mget_same_slot_succeeds() {
    let mut conn = cluster_conn().await;
    let k1 = "{crossslot_tag}:key1";
    let k2 = "{crossslot_tag}:key2";

    conn.execute(Set::new(k1, "v1")).await.unwrap();
    conn.execute(Set::new(k2, "v2")).await.unwrap();

    let result = conn.execute(MGet::new([k1, k2])).await;
    assert!(
        result.is_ok(),
        "hash-tag MGet should succeed: {:?}",
        result.err()
    );
    let vals = result.unwrap();
    assert_eq!(vals[0], Some(Bytes::from("v1")));
    assert_eq!(vals[1], Some(Bytes::from("v2")));

    conn.execute(Del::new(k1)).await.unwrap();
    conn.execute(Del::new(k2)).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn cluster_hash_tag_mset_same_slot_succeeds() {
    let mut conn = cluster_conn().await;
    let k1 = "{mset_tag}:a";
    let k2 = "{mset_tag}:b";

    let result = conn
        .execute(MSet::new([(k1, "hello"), (k2, "world")]))
        .await;
    assert!(
        result.is_ok(),
        "hash-tag MSet should succeed: {:?}",
        result.err()
    );

    let v1: Option<Bytes> = conn.execute(Get::new(k1)).await.unwrap();
    let v2: Option<Bytes> = conn.execute(Get::new(k2)).await.unwrap();
    assert_eq!(v1, Some(Bytes::from("hello")));
    assert_eq!(v2, Some(Bytes::from("world")));

    conn.execute(Del::new(k1)).await.unwrap();
    conn.execute(Del::new(k2)).await.unwrap();
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
