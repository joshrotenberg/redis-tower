//! Cluster integration tests.
//!
//! Run with: `cargo test -p redis-tower-cluster --test cluster_integration -- --ignored --test-threads=1`

use bytes::Bytes;
use futures::StreamExt;
use redis_server_wrapper::{RedisCluster, RedisClusterHandle};
use redis_tower::metrics_layer::{
    ClusterRedirectKind, ClusterTopologyRefreshOutcome, ErrorKind, MetricsRecorder,
};
use redis_tower::pool::ConnectionPool;
use redis_tower_cluster::{
    ClusterConnection, ClusterScan, ClusterScanItem, MultiplexedClusterClient, ScanClusterStream,
};
use redis_tower_commands::*;
use redis_tower_test::cluster::{ClusterFixture, ClusterNodeRole, key_for_slot};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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

#[derive(Default)]
struct RefreshMetrics {
    outcomes: Mutex<Vec<ClusterTopologyRefreshOutcome>>,
    redirects: Mutex<Vec<ClusterRedirectKind>>,
}

impl MetricsRecorder for RefreshMetrics {
    fn command_completed(&self, _: &str, _: Duration, _: Option<ErrorKind>) {}

    fn cluster_redirected(&self, kind: ClusterRedirectKind) {
        self.redirects.lock().unwrap().push(kind);
    }

    fn cluster_topology_refresh_completed(
        &self,
        _: Duration,
        outcome: ClusterTopologyRefreshOutcome,
    ) {
        self.outcomes.lock().unwrap().push(outcome);
    }
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
    let fixture = ensure_cluster().await;
    let metrics = Arc::new(RefreshMetrics::default());
    let cluster = MultiplexedClusterClient::builder(fixture.addr())
        .metrics_recorder(metrics.clone())
        .connect()
        .await
        .expect("failed to connect to multiplexed cluster");
    cluster
        .refresh_topology()
        .await
        .expect("refresh should succeed on a healthy cluster");
    let topo = cluster.topology().await;
    assert_eq!(topo.master_addrs().len(), 3);
    assert_eq!(
        *metrics.outcomes.lock().unwrap(),
        vec![ClusterTopologyRefreshOutcome::Success]
    );
}

/// A cluster-wide SCAN reaches every master, where a plain keyless `SCAN`
/// reaches only the node it happened to route to (#456).
#[tokio::test]
#[ignore]
async fn mux_cluster_scan_stream_covers_the_whole_keyspace() {
    let cluster = mux_cluster_conn().await;

    // Distinct hash tags so the keys land on different slots, and therefore --
    // on a three-master cluster with an even slot split -- on different nodes.
    let keys: Vec<String> = (0..60).map(|i| format!("scan456:{{{i}}}:k")).collect();
    for key in &keys {
        cluster.execute(Set::new(key, "v")).await.unwrap();
    }

    let items: Vec<ClusterScanItem> =
        std::pin::pin!(ScanClusterStream::scan(&cluster, "scan456:*"))
            .map(|r| r.expect("cluster scan should succeed"))
            .collect()
            .await;

    let found: HashSet<String> = items
        .iter()
        .map(|i| String::from_utf8_lossy(&i.key).into_owned())
        .collect();
    for key in &keys {
        assert!(found.contains(key), "cluster scan missed {key}");
    }

    let nodes: HashSet<&str> = items.iter().map(|i| i.node.as_str()).collect();
    assert!(
        nodes.len() > 1,
        "60 keys across 60 slots should span more than one master, saw {nodes:?}"
    );

    // The contrast: a keyless SCAN routes to one node and silently returns a
    // fraction of the keyspace.
    let single = cluster
        .execute(Scan::new().match_pattern("scan456:*").count(1000))
        .await
        .unwrap();
    assert!(
        single.results.len() < keys.len(),
        "a plain SCAN should not have covered the cluster"
    );

    for key in &keys {
        cluster.execute(Del::new(key)).await.unwrap();
    }
}

/// Paging several masters at once finds the same keys as the sequential
/// traversal. Coverage is what the fan-out must not change; the ordering it does
/// change is asserted in the unit tests, which can pin the visit order exactly.
#[tokio::test]
#[ignore]
async fn mux_cluster_scan_stream_covers_the_keyspace_concurrently() {
    let cluster = mux_cluster_conn().await;

    let keys: Vec<String> = (0..60).map(|i| format!("scan456c:{{{i}}}:k")).collect();
    for key in &keys {
        cluster.execute(Set::new(key, "v")).await.unwrap();
    }

    let collect = |concurrency: usize| {
        let cluster = cluster.clone();
        async move {
            let items: Vec<ClusterScanItem> = std::pin::pin!(
                ClusterScan::new("scan456c:*")
                    .count(10)
                    .concurrency(concurrency)
                    .run(&cluster)
            )
            .map(|r| r.expect("cluster scan should succeed"))
            .collect()
            .await;
            items
        }
    };

    let concurrent = collect(4).await;
    let sequential = collect(1).await;

    let found: HashSet<String> = concurrent
        .iter()
        .map(|i| String::from_utf8_lossy(&i.key).into_owned())
        .collect();
    for key in &keys {
        assert!(found.contains(key), "concurrent cluster scan missed {key}");
    }

    let sequential_keys: HashSet<String> = sequential
        .iter()
        .map(|i| String::from_utf8_lossy(&i.key).into_owned())
        .collect();
    assert_eq!(
        found, sequential_keys,
        "concurrency must not change which keys a scan finds"
    );

    // Each key's node tag must still be the master that actually holds it, which
    // is what the sequential traversal reported for the same key.
    let sequential_nodes: std::collections::HashMap<String, String> = sequential
        .iter()
        .map(|i| (String::from_utf8_lossy(&i.key).into_owned(), i.node.clone()))
        .collect();
    for item in &concurrent {
        let key = String::from_utf8_lossy(&item.key).into_owned();
        assert_eq!(
            sequential_nodes.get(&key),
            Some(&item.node),
            "{key} was tagged with a different master than the sequential scan saw"
        );
    }

    let nodes: HashSet<&str> = concurrent.iter().map(|i| i.node.as_str()).collect();
    assert!(
        nodes.len() > 1,
        "60 keys across 60 slots should span more than one master, saw {nodes:?}"
    );

    for key in &keys {
        cluster.execute(Del::new(key)).await.unwrap();
    }
}

/// Refreshing membership between rounds finds the same keys on a cluster that is
/// not resharding, and really does re-run discovery -- the topology it comes back
/// with still holds every master the scan reached.
///
/// The case it exists for, a slot migrating mid-scan, needs a reshard this fixture
/// cannot drive; the unit tests cover that with a fake cluster whose membership a
/// test can change between two polls of the stream.
#[tokio::test]
#[ignore]
async fn mux_cluster_scan_stream_refreshing_membership_covers_the_keyspace() {
    let cluster = mux_cluster_conn().await;

    let keys: Vec<String> = (0..60).map(|i| format!("scan456m:{{{i}}}:k")).collect();
    for key in &keys {
        cluster.execute(Set::new(key, "v")).await.unwrap();
    }

    let refreshing: Vec<ClusterScanItem> = std::pin::pin!(
        ClusterScan::new("scan456m:*")
            .count(10)
            .refresh_membership(true)
            .run(&cluster)
    )
    .map(|r| r.expect("refreshing cluster scan should succeed"))
    .collect()
    .await;

    let found: HashSet<String> = refreshing
        .iter()
        .map(|i| String::from_utf8_lossy(&i.key).into_owned())
        .collect();
    for key in &keys {
        assert!(found.contains(key), "refreshing cluster scan missed {key}");
    }

    // The refreshes reconcile the client's own services, so a scan that finished
    // must have left the masters it scanned in place.
    let masters: HashSet<String> = cluster
        .topology()
        .await
        .master_addrs()
        .iter()
        .map(|a| a.addr_string())
        .collect();
    for item in &refreshing {
        assert!(
            masters.contains(&item.node),
            "{} was scanned but is not a master after the refreshes",
            item.node
        );
    }

    for key in &keys {
        cluster.execute(Del::new(key)).await.unwrap();
    }
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

/// A real held reshard exposes both redirect modes in their protocol order.
///
/// Moving the key without handing off its slot makes the stale owner return
/// ASK. Completing the handoff while the client still has the old slot map
/// makes that owner return MOVED. Successful reads alone would not prove either
/// path ran, so the test also checks the bounded redirect metric hook.
#[tokio::test]
#[ignore = "live: starts a dedicated 3-master/3-replica cluster and reshards a slot"]
async fn mux_cluster_handles_ask_then_moved_during_live_reshard() {
    let fixture = ClusterFixture::builder()
        .base_port(17700)
        .start()
        .await
        .expect("failed to start reshard fixture");
    let before = fixture
        .topology()
        .await
        .expect("failed to inspect initial topology");
    let slot = 42;
    let source = before
        .owner_of_slot(slot)
        .expect("slot should have an owner")
        .clone();
    let target = before
        .nodes()
        .iter()
        .find(|node| matches!(&node.role, ClusterNodeRole::Master) && node.id != source.id)
        .expect("fixture should have another master")
        .clone();
    let key = key_for_slot(slot);
    let metrics = Arc::new(RefreshMetrics::default());
    let client = MultiplexedClusterClient::builder(fixture.seed_addr())
        .metrics_recorder(metrics.clone())
        .connect()
        .await
        .expect("failed to connect to reshard fixture");

    client
        .execute(redis_tower::WithDeadline::after(
            Set::new(&key, "before-reshard"),
            Duration::from_secs(5),
        ))
        .await
        .expect("failed to seed reshard key");

    let guard = fixture
        .begin_reshard(slot, target.index)
        .await
        .expect("failed to open reshard window");
    let moved = tokio::time::timeout(Duration::from_secs(5), guard.migrate_keys())
        .await
        .expect("timed out moving the held slot's keys")
        .expect("failed to move key while keeping ownership on source");
    assert_eq!(moved, 1, "the held migration should move the seeded key");

    // Preserve cleanup if the client regresses: finish the handoff before
    // asserting on the result captured from the ASK window.
    let ask_result = client
        .execute(redis_tower::WithDeadline::after(
            Get::new(&key),
            Duration::from_secs(5),
        ))
        .await;
    let redirects_after_ask = metrics.redirects.lock().unwrap().clone();
    tokio::time::timeout(Duration::from_secs(5), guard.complete())
        .await
        .expect("timed out completing slot handoff")
        .expect("failed to complete slot handoff");
    fixture
        .wait_for_slot_owner(slot, &target.id, Duration::from_secs(5))
        .await
        .expect("target never became the advertised slot owner");

    assert_eq!(
        ask_result.expect("client should follow ASK with atomic ASKING + GET"),
        Some(Bytes::from("before-reshard"))
    );
    assert_eq!(redirects_after_ask, vec![ClusterRedirectKind::Ask]);

    let moved_result = client
        .execute(redis_tower::WithDeadline::after(
            Get::new(&key),
            Duration::from_secs(5),
        ))
        .await
        .expect("client should follow MOVED after the completed handoff");
    assert_eq!(moved_result, Some(Bytes::from("before-reshard")));
    assert_eq!(
        *metrics.redirects.lock().unwrap(),
        vec![ClusterRedirectKind::Ask, ClusterRedirectKind::Moved]
    );
    assert_eq!(
        client
            .topology()
            .await
            .master_for_slot(slot)
            .expect("client topology lost the migrated slot")
            .addr_string(),
        target.addr,
        "MOVED should patch the migrated slot to the new owner"
    );

    client
        .execute(redis_tower::WithDeadline::after(
            Del::new(&key),
            Duration::from_secs(5),
        ))
        .await
        .ok();
    tokio::time::timeout(Duration::from_secs(5), client.shutdown())
        .await
        .expect("timed out shutting down reshard client");
}

/// Killing a master in a real 3x3 topology and issuing CLUSTER FAILOVER on its
/// replica must lead to promotion and automatic replacement of the dead
/// per-node worker in the client's slot map. The first successful post-kill
/// read is bounded and timed so this cannot pass by wedging until the test
/// runner's global timeout.
#[tokio::test]
#[ignore = "destructive: starts a dedicated 3-master/3-replica cluster and kills a master"]
async fn mux_cluster_replaces_killed_master_after_replica_promotion() {
    let fixture = ClusterFixture::builder()
        .base_port(17500)
        .cluster_node_timeout(2000)
        .start()
        .await
        .expect("failed to start failover fixture");
    let before = fixture
        .topology()
        .await
        .expect("failed to inspect initial topology");
    let slot = 42;
    let old_master = before
        .owner_of_slot(slot)
        .expect("slot should have a master")
        .clone();
    let promoted = before
        .replicas_of(&old_master.id)
        .into_iter()
        .next()
        .expect("3x3 fixture should have one replica for the slot owner")
        .clone();
    let key = key_for_slot(slot);

    let client = MultiplexedClusterClient::connect(&fixture.seed_addr())
        .await
        .expect("failed to connect to failover fixture");

    // Seed through a direct connection so WAIT observes this write on the same
    // socket. That proves the only replica acknowledged the value before its
    // master is killed, avoiding a timing-only replication assumption.
    let mut seeder = redis_tower::RedisConnection::connect(&old_master.addr)
        .await
        .expect("failed to connect directly to slot owner");
    seeder
        .execute(Set::new(&key, "survives-failover"))
        .await
        .expect("failed to seed failover key");
    let acknowledgements = seeder
        .execute(Wait::new(1, 5000))
        .await
        .expect("WAIT failed before failover");
    assert_eq!(
        acknowledgements, 1,
        "replica did not acknowledge seed write"
    );
    drop(seeder);
    assert_eq!(
        client.execute(Get::new(&key)).await.unwrap(),
        Some(Bytes::from("survives-failover"))
    );

    let started = Instant::now();
    let killed = fixture
        .kill_slot_owner(slot)
        .await
        .expect("failed to kill slot owner");
    assert_eq!(killed.id, old_master.id);
    fixture
        .promote_replica(promoted.index)
        .await
        .expect("CLUSTER FAILOVER failed on the surviving replica");

    let recovery_budget = Duration::from_secs(30);
    let mut attempts = 0usize;
    let mut failed_attempts = 0usize;
    let mut last_failure = None;
    let recovered = loop {
        attempts += 1;
        let command = redis_tower::WithDeadline::after(Get::new(&key), Duration::from_secs(2));
        match client.execute(command).await {
            Ok(Some(value)) => break value,
            Ok(None) => {
                failed_attempts += 1;
                last_failure = Some("successful GET returned no value".to_string());
            }
            Err(error) => {
                failed_attempts += 1;
                last_failure = Some(error.to_string());
            }
        }
        assert!(
            started.elapsed() < recovery_budget,
            "no successful read within {recovery_budget:?} after killing {}; \
             attempts={attempts}, last_failure={last_failure:?}",
            old_master.addr,
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let time_to_first_success = started.elapsed();
    assert!(
        time_to_first_success < recovery_budget,
        "first successful read took {time_to_first_success:?}, exceeding {recovery_budget:?}"
    );
    assert_eq!(recovered, Bytes::from("survives-failover"));

    let replacement = fixture
        .wait_for_slot_owner_change(slot, &old_master.id, recovery_budget)
        .await
        .expect("the killed master was not replaced");
    assert_eq!(
        replacement.id, promoted.id,
        "the slot owner's only replica should win the election"
    );
    let after = fixture
        .topology()
        .await
        .expect("failed to inspect promoted topology");
    assert_eq!(
        after.owner_of_slot(slot).map(|node| node.id.as_str()),
        Some(promoted.id.as_str()),
        "Redis did not reassign the slot to its only replica"
    );
    assert_eq!(
        client
            .topology()
            .await
            .master_for_slot(slot)
            .expect("client topology lost the failed-over slot")
            .addr_string(),
        promoted.addr,
        "client retained the killed master after a successful read"
    );
    assert_ne!(promoted.addr, old_master.addr);
    eprintln!(
        "cluster failover time-to-first-success: {time_to_first_success:?} \
         (budget {recovery_budget:?}, attempts={attempts}, failed_attempts={failed_attempts}, \
         last_failure={last_failure:?})"
    );

    client
        .execute(redis_tower::WithDeadline::after(
            Del::new(&key),
            Duration::from_secs(5),
        ))
        .await
        .ok();
    tokio::time::timeout(Duration::from_secs(5), client.shutdown())
        .await
        .expect("timed out shutting down failover client");
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
