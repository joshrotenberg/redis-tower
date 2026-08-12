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
use redis_tower::{CacheTrackingMode, CachedClientConfig, Transaction};
use redis_tower_cluster::{
    CachedMultiplexedClusterClient, ClusterClient, ClusterConnection, ClusterPipeline, ClusterScan,
    ClusterScanItem, MultiplexedClusterClient, ScanClusterStream, slot_for_key,
};
use redis_tower_commands::*;
use redis_tower_test::cluster::{ClusterFixture, ClusterNodeRole, key_for_slot};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
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

fn commandstat_calls(info: &str, command: &str) -> u64 {
    let prefix = format!("cmdstat_{command}:");
    info.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|fields| {
            fields
                .split(',')
                .find_map(|field| field.strip_prefix("calls="))
        })
        .and_then(|calls| calls.parse().ok())
        .unwrap_or(0)
}

async fn master_commandstat_calls(fixture: &ClusterFixture, command: &str) -> Vec<(usize, u64)> {
    let topology = fixture
        .topology()
        .await
        .expect("failed to inspect commandstats topology");
    let mut calls = Vec::new();
    for master in topology.masters() {
        let info = fixture
            .run_node(master.index, &["INFO", "commandstats"])
            .await
            .expect("failed to read master commandstats");
        calls.push((master.index, commandstat_calls(&info, command)));
    }
    calls
}

fn assert_commandstat_incremented_only_on(
    before: &[(usize, u64)],
    after: &[(usize, u64)],
    owner_index: usize,
    command: &str,
) {
    assert_eq!(after.len(), before.len(), "master set changed during test");
    for (index, before_calls) in before {
        let after_calls = after
            .iter()
            .find_map(|(candidate, calls)| (candidate == index).then_some(*calls))
            .unwrap_or_else(|| panic!("master {index} disappeared during test"));
        let expected = before_calls + if *index == owner_index { 1 } else { 0 };
        assert_eq!(
            after_calls, expected,
            "{command} reached unexpected master {index}; owner is {owner_index}"
        );
    }
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

// -- CachedMultiplexedClusterClient-specific tests --

async fn wait_for_cached_value(
    client: &CachedMultiplexedClusterClient,
    key: &str,
    expected: &[u8],
    timeout: Duration,
) -> Option<Bytes> {
    let deadline = Instant::now() + timeout;
    loop {
        match client.execute(Get::new(key)).await {
            Ok(Some(value)) if value.as_ref() == expected => return Some(value),
            Ok(_) | Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(value) => return value,
            Err(error) => {
                panic!("cached GET for {key} did not recover before {timeout:?}: {error}")
            }
        }
    }
}

#[tokio::test]
#[ignore]
async fn cached_cluster_clones_share_hits_and_observe_external_invalidation() {
    let cluster = ensure_cluster().await;
    let writer = MultiplexedClusterClient::connect(&cluster.addr())
        .await
        .expect("failed to connect external cluster writer");
    let key = "cached-cluster:shared-clone";
    writer.execute(Del::new(key)).await.ok();
    writer
        .execute(Set::new(key, "before"))
        .await
        .expect("failed to seed cached-cluster key");

    let cached = CachedMultiplexedClusterClient::connect(&cluster.addr())
        .await
        .expect("failed to connect cached cluster client");
    assert!(cached.is_caching_healthy().await);
    assert_eq!(
        cached.execute(Get::new(key)).await.unwrap(),
        Some(Bytes::from_static(b"before"))
    );
    let clone = cached.clone();
    assert_eq!(
        clone.execute(Get::new(key)).await.unwrap(),
        Some(Bytes::from_static(b"before"))
    );
    let after_hit = cached.cache_statistics().await;
    assert!(after_hit.misses >= 1, "first read should miss locally");
    assert!(
        after_hit.hits >= 1,
        "a clone should share the populated entry"
    );

    writer
        .execute(Set::new(key, "after"))
        .await
        .expect("failed to mutate key outside cached client");
    assert_eq!(
        wait_for_cached_value(&clone, key, b"after", Duration::from_secs(5)).await,
        Some(Bytes::from_static(b"after")),
        "external write must invalidate the shared entry"
    );
    assert!(
        cached.cache_statistics().await.invalidations > after_hit.invalidations,
        "receiver should record the external invalidation"
    );

    writer.execute(Del::new(key)).await.ok();
    drop(clone);
    tokio::time::timeout(Duration::from_secs(5), cached.shutdown())
        .await
        .expect("timed out shutting down cached cluster client");
    tokio::time::timeout(Duration::from_secs(5), writer.shutdown())
        .await
        .expect("timed out shutting down external cluster writer");
}

#[tokio::test]
#[ignore]
async fn cached_cluster_opt_in_keeps_setup_atomic_under_concurrency() {
    let cluster = ensure_cluster().await;
    let writer = MultiplexedClusterClient::connect(&cluster.addr())
        .await
        .expect("failed to connect opt-in writer");
    let keys = (0..24)
        .map(|index| format!("cached-cluster:optin:{{{index}}}"))
        .collect::<Vec<_>>();
    for key in &keys {
        writer.execute(Del::new(key)).await.ok();
        writer
            .execute(Set::new(key, "v1"))
            .await
            .expect("failed to seed opt-in key");
    }

    let config = CachedClientConfig::new()
        .tracking_mode(CacheTrackingMode::OptIn)
        .client_ttl(Some(Duration::from_secs(300)));
    let cached = CachedMultiplexedClusterClient::builder(cluster.addr())
        .cache_config(config)
        .connect()
        .await
        .expect("failed to connect opt-in cached cluster client");
    let barrier = Arc::new(tokio::sync::Barrier::new(keys.len() + 1));
    let reads = keys
        .iter()
        .cloned()
        .map(|key| {
            let client = cached.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                let value = client.execute(Get::new(&key)).await.unwrap();
                (key, value)
            })
        })
        .collect::<Vec<_>>();
    barrier.wait().await;
    for read in reads {
        let (key, value) = read.await.unwrap();
        assert_eq!(
            value,
            Some(Bytes::from_static(b"v1")),
            "wrong value for {key}"
        );
    }
    assert_eq!(cached.cache_size().await, keys.len());

    // Every second read is local, proving all concurrently configured reads
    // populated the one shared cache.
    for key in &keys {
        assert_eq!(
            cached.execute(Get::new(key)).await.unwrap(),
            Some(Bytes::from_static(b"v1"))
        );
    }
    assert!(cached.cache_statistics().await.hits >= keys.len() as u64);

    // If CLIENT CACHING YES interleaves with another caller's command, at
    // least one populated key will not be tracked and will remain stale here
    // throughout this test's five-second assertion window; its five-minute
    // TTL is only the required Cluster safety backstop.
    for key in &keys {
        writer
            .execute(Set::new(key, "v2"))
            .await
            .expect("failed to externally update opt-in key");
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut all_fresh = true;
        for key in &keys {
            match cached.execute(Get::new(key)).await {
                Ok(Some(value)) if value.as_ref() == b"v2" => {}
                Ok(_) | Err(_) => all_fresh = false,
            }
        }
        if all_fresh {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "an opt-in cache entry stayed stale; CLIENT CACHING YES may have interleaved"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    for key in &keys {
        writer.execute(Del::new(key)).await.ok();
    }
    tokio::time::timeout(Duration::from_secs(5), cached.shutdown())
        .await
        .expect("timed out shutting down opt-in cached cluster client");
    tokio::time::timeout(Duration::from_secs(5), writer.shutdown())
        .await
        .expect("timed out shutting down opt-in writer");
}

/// Redis clears both `ASKING` and `CLIENT CACHING YES` after the next command,
/// so the two one-shot flags cannot prefix the same migrated read. The cached
/// router must close its safety gate and follow ASK with only `ASKING` + GET.
#[tokio::test]
#[ignore = "live: starts a dedicated cluster and holds a slot migration open"]
async fn cached_cluster_opt_in_follows_live_ask_without_conflicting_one_shot_flags() {
    let fixture = ClusterFixture::builder()
        .base_port(17800)
        .start()
        .await
        .expect("failed to start cached reshard fixture");
    let before = fixture
        .topology()
        .await
        .expect("failed to inspect cached reshard topology");
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
    let config = CachedClientConfig::new()
        .tracking_mode(CacheTrackingMode::OptIn)
        .client_ttl(Some(Duration::from_secs(300)));
    let cached = CachedMultiplexedClusterClient::builder(fixture.seed_addr())
        .cache_config(config)
        .metrics_recorder(metrics.clone())
        .connect()
        .await
        .expect("failed to connect cached reshard client");

    cached
        .execute(redis_tower::WithDeadline::after(
            Set::new(&key, "during-reshard"),
            Duration::from_secs(5),
        ))
        .await
        .expect("failed to seed cached reshard key");
    cached.clear_cache().await;

    let guard = fixture
        .begin_reshard(slot, target.index)
        .await
        .expect("failed to open cached reshard window");
    let moved = tokio::time::timeout(Duration::from_secs(5), guard.migrate_keys())
        .await
        .expect("timed out moving cached reshard key")
        .expect("failed to move cached reshard key");
    assert_eq!(moved, 1, "the held migration should move the seeded key");

    // Complete the migration before asserting so fixture cleanup remains
    // deterministic even if the ASK retry regresses.
    let ask_result = cached
        .execute(redis_tower::WithDeadline::after(
            Get::new(&key),
            Duration::from_secs(5),
        ))
        .await;
    let redirects_after_ask = metrics.redirects.lock().unwrap().clone();
    let cache_size_after_ask = cached.cache_size().await;
    tokio::time::timeout(Duration::from_secs(5), guard.complete())
        .await
        .expect("timed out completing cached slot handoff")
        .expect("failed to complete cached slot handoff");

    assert_eq!(
        ask_result.expect("cached OptIn client should follow ASK with ASKING + GET"),
        Some(Bytes::from_static(b"during-reshard"))
    );
    assert_eq!(redirects_after_ask, vec![ClusterRedirectKind::Ask]);
    assert_eq!(
        cache_size_after_ask, 0,
        "an ASK response must not populate the local cache"
    );

    cached
        .execute(redis_tower::WithDeadline::after(
            Del::new(&key),
            Duration::from_secs(5),
        ))
        .await
        .ok();
    tokio::time::timeout(Duration::from_secs(5), cached.shutdown())
        .await
        .expect("timed out shutting down cached reshard client");
}

/// A missing key tracked on the old owner is not tracked on the new owner.
/// This leaves a cached nil that ordinary server-default invalidations cannot
/// remove after an empty-slot handoff, so a separate same-slot EXISTS must
/// follow MOVED and synchronously make that old slot entry unusable.
#[tokio::test]
#[ignore = "live: starts a dedicated cluster and moves an empty slot"]
async fn cached_cluster_moved_invalidates_a_preexisting_slot_entry() {
    let fixture = ClusterFixture::builder()
        .base_port(17900)
        .start()
        .await
        .expect("failed to start cached MOVED fixture");
    let before = fixture
        .topology()
        .await
        .expect("failed to inspect cached MOVED topology");
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
    let config = CachedClientConfig::new()
        .tracking_mode(CacheTrackingMode::ServerDefault)
        // Keep the entry alive well beyond the test deadline so MOVED, rather
        // than TTL expiry, remains the mechanism under test.
        .client_ttl(Some(Duration::from_secs(300)));
    let cached = CachedMultiplexedClusterClient::builder(fixture.seed_addr())
        .cache_config(config)
        .metrics_recorder(metrics.clone())
        .connect()
        .await
        .expect("failed to connect cached MOVED client");

    assert_eq!(
        cached.execute(Get::new(&key)).await.unwrap(),
        None,
        "the empty slot should begin with a missing key"
    );
    assert_eq!(
        cached.cache_size().await,
        1,
        "nil response should be cached"
    );

    let moved = fixture
        .reshard_slot(slot, target.index)
        .await
        .expect("failed to move empty cached slot");
    assert_eq!(moved, 0, "the slot must stay empty during the handoff");
    fixture
        .wait_for_slot_owner(slot, &target.id, Duration::from_secs(5))
        .await
        .expect("target never became the advertised slot owner");
    fixture
        .run_node(target.index, &["SET", &key, "after-moved"])
        .await
        .expect("failed to create key on the new owner");
    assert_eq!(
        cached.cache_size().await,
        1,
        "server-default tracking on the old owner should leave the nil cached"
    );

    assert_eq!(
        cached
            .execute(redis_tower::WithDeadline::after(
                Exists::new(&key),
                Duration::from_secs(5),
            ))
            .await
            .expect("same-slot EXISTS should follow MOVED"),
        1
    );
    assert_eq!(
        *metrics.redirects.lock().unwrap(),
        vec![ClusterRedirectKind::Moved]
    );
    assert_eq!(
        cached
            .execute(redis_tower::WithDeadline::after(
                Get::new(&key),
                Duration::from_secs(5),
            ))
            .await
            .expect("GET after MOVED must bypass the old slot entry"),
        Some(Bytes::from_static(b"after-moved"))
    );

    cached.execute(Del::new(&key)).await.ok();
    tokio::time::timeout(Duration::from_secs(5), cached.shutdown())
        .await
        .expect("timed out shutting down cached MOVED client");
}

#[tokio::test]
#[ignore = "live: kills cached data and invalidation connections on one master"]
async fn cached_cluster_fails_closed_and_recovers_after_master_connections_are_killed() {
    let cluster = ensure_cluster().await;
    let writer = MultiplexedClusterClient::connect(&cluster.addr())
        .await
        .expect("failed to connect failure-path writer");
    let key = "cached-cluster:receiver-loss";
    writer.execute(Del::new(key)).await.ok();
    writer.execute(Set::new(key, "before")).await.unwrap();

    let cached = CachedMultiplexedClusterClient::connect(&cluster.addr())
        .await
        .expect("failed to connect failure-path cached client");
    assert_eq!(
        cached.execute(Get::new(key)).await.unwrap(),
        Some(Bytes::from_static(b"before"))
    );
    assert_eq!(
        cached.execute(Get::new(key)).await.unwrap(),
        Some(Bytes::from_static(b"before"))
    );

    let owner = cached
        .topology()
        .await
        .master_for_slot(slot_for_key(key.as_bytes()))
        .expect("test key slot should have a master")
        .addr_string();
    let owner_port = owner
        .rsplit_once(':')
        .map(|(_, port)| port)
        .expect("master address should contain a port");
    let owner_index = cluster
        .node_addrs()
        .iter()
        .position(|addr| {
            addr.rsplit_once(':')
                .is_some_and(|(_, port)| port == owner_port)
        })
        .expect("cached topology master should belong to fixture");

    // Repeating the deterministic server-side kill closes any connection that
    // races a worker reconnect. Stop as soon as the cache safety gate reports
    // the coverage loss.
    let unhealthy_deadline = Instant::now() + Duration::from_secs(3);
    while cached.is_caching_healthy().await {
        cluster
            .node(owner_index)
            .run(&["CLIENT", "KILL", "TYPE", "NORMAL"])
            .await
            .expect("failed to kill normal clients on cached master");
        assert!(
            Instant::now() < unhealthy_deadline,
            "cached client never failed closed after its data/receiver loss"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let clear_deadline = Instant::now() + Duration::from_secs(3);
    while cached.cache_size().await != 0 {
        assert!(
            Instant::now() < clear_deadline,
            "coverage loss did not clear the shared cache"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    writer
        .execute(Set::new(key, "after"))
        .await
        .expect("failed to write while cached coverage was recovering");
    assert_eq!(
        wait_for_cached_value(&cached, key, b"after", Duration::from_secs(10)).await,
        Some(Bytes::from_static(b"after")),
        "a successful read after coverage loss must never return the old entry"
    );
    let healthy_deadline = Instant::now() + Duration::from_secs(10);
    while !cached.is_caching_healthy().await {
        assert!(
            Instant::now() < healthy_deadline,
            "per-master tracking coverage did not recover"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    writer.execute(Del::new(key)).await.ok();
    tokio::time::timeout(Duration::from_secs(5), cached.shutdown())
        .await
        .expect("timed out shutting down recovered cached client");
    tokio::time::timeout(Duration::from_secs(5), writer.shutdown())
        .await
        .expect("timed out shutting down failure-path writer");
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

/// Explicit cluster pipelines group by concrete node while preserving the
/// caller's typed result order. The split helpers intentionally relax Redis's
/// single-slot atomicity rule, so exercise their duplicate, missing-key, and
/// input-order contracts across three exact slots.
#[tokio::test]
#[ignore = "live: starts a dedicated 3-master/3-replica cluster for cross-slot batches"]
async fn mux_cluster_pipeline_and_split_helpers_preserve_order() {
    let fixture = tokio::time::timeout(Duration::from_secs(60), ClusterFixture::start())
        .await
        .expect("timed out starting cluster batch fixture")
        .expect("failed to start cluster batch fixture");
    let client = tokio::time::timeout(
        Duration::from_secs(10),
        MultiplexedClusterClient::connect(&fixture.seed_addr()),
    )
    .await
    .expect("timed out connecting cluster batch client")
    .expect("failed to connect cluster batch client");

    let low_slot = key_for_slot(42);
    let middle_slot = key_for_slot(6_000);
    let high_slot = key_for_slot(12_000);
    let missing_slot = key_for_slot(9_000);
    assert_ne!(
        redis_tower_cluster::slot_for_key(low_slot.as_bytes()),
        redis_tower_cluster::slot_for_key(middle_slot.as_bytes())
    );
    assert_ne!(
        redis_tower_cluster::slot_for_key(middle_slot.as_bytes()),
        redis_tower_cluster::slot_for_key(high_slot.as_bytes())
    );
    let topology = fixture
        .topology()
        .await
        .expect("failed to inspect cluster batch topology");
    let owners: HashSet<&str> = [42, 6_000, 12_000]
        .into_iter()
        .map(|slot| {
            topology
                .owner_of_slot(slot)
                .expect("exact test slot should have an owner")
                .id
                .as_str()
        })
        .collect();
    assert_eq!(owners.len(), 3, "pipeline keys must span all three masters");

    tokio::time::timeout(
        Duration::from_secs(5),
        client.del_split([
            low_slot.as_bytes(),
            middle_slot.as_bytes(),
            high_slot.as_bytes(),
            missing_slot.as_bytes(),
        ]),
    )
    .await
    .expect("timed out cleaning cluster batch keys")
    .expect("failed to clean cluster batch keys");
    tokio::time::timeout(
        Duration::from_secs(5),
        client.mset_split([
            (low_slot.as_bytes(), b"40".as_slice()),
            (middle_slot.as_bytes(), b"middle".as_slice()),
            (high_slot.as_bytes(), b"tail".as_slice()),
        ]),
    )
    .await
    .expect("timed out seeding mixed-slot pipeline keys")
    .expect("failed to seed mixed-slot pipeline keys");

    let pipeline_results = tokio::time::timeout(
        Duration::from_secs(5),
        ClusterPipeline::new()
            .push(Get::new(&high_slot))
            .push(StrLen::new(&middle_slot))
            .push(Incr::new(&low_slot))
            .push(Get::new(&low_slot))
            .execute(&client),
    )
    .await
    .expect("timed out executing mixed-slot typed pipeline")
    .expect("mixed-slot typed pipeline failed");
    assert_eq!(
        pipeline_results.get::<Option<Bytes>>(0).unwrap().as_deref(),
        Some(&b"tail"[..])
    );
    assert_eq!(*pipeline_results.get::<i64>(1).unwrap(), 6);
    assert_eq!(*pipeline_results.get::<i64>(2).unwrap(), 41);
    assert_eq!(
        pipeline_results.get::<Option<Bytes>>(3).unwrap().as_deref(),
        Some(&b"41"[..])
    );

    // A duplicate MSET key remains in its slot-local input order, so the last
    // value wins exactly as it does for an ordinary same-slot MSET.
    tokio::time::timeout(
        Duration::from_secs(5),
        client.mset_split([
            (middle_slot.as_bytes(), b"middle-first".as_slice()),
            (low_slot.as_bytes(), b"low".as_slice()),
            (high_slot.as_bytes(), b"high".as_slice()),
            (middle_slot.as_bytes(), b"middle-last".as_slice()),
        ]),
    )
    .await
    .expect("timed out executing split MSET")
    .expect("split MSET failed");

    let values = tokio::time::timeout(
        Duration::from_secs(5),
        client.mget_split([
            high_slot.as_bytes(),
            middle_slot.as_bytes(),
            missing_slot.as_bytes(),
            low_slot.as_bytes(),
            middle_slot.as_bytes(),
        ]),
    )
    .await
    .expect("timed out executing split MGET")
    .expect("split MGET failed");
    assert_eq!(
        values,
        vec![
            Some(Bytes::from_static(b"high")),
            Some(Bytes::from_static(b"middle-last")),
            None,
            Some(Bytes::from_static(b"low")),
            Some(Bytes::from_static(b"middle-last")),
        ],
        "split MGET must restore duplicate and missing values to input order"
    );

    let deleted = tokio::time::timeout(
        Duration::from_secs(5),
        client.del_split([
            middle_slot.as_bytes(),
            low_slot.as_bytes(),
            middle_slot.as_bytes(),
            missing_slot.as_bytes(),
            high_slot.as_bytes(),
        ]),
    )
    .await
    .expect("timed out executing split DEL")
    .expect("split DEL failed");
    assert_eq!(deleted, 3, "duplicate and missing DEL keys count once/zero");
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(5),
            client.mget_split([
                low_slot.as_bytes(),
                middle_slot.as_bytes(),
                high_slot.as_bytes(),
            ]),
        )
        .await
        .expect("timed out verifying split DEL cleanup")
        .expect("failed to verify split DEL cleanup"),
        vec![None, None, None]
    );

    tokio::time::timeout(Duration::from_secs(5), client.shutdown())
        .await
        .expect("timed out shutting down cluster batch client");
}

/// Cluster transactions must pin every frame to one master before touching the
/// wire. Exercise both shared and exclusive clients, prove a cross-slot body is
/// rejected without partial writes, and exercise WATCH through the direct
/// transaction surface that submits one preflighted, pinned exchange.
#[tokio::test]
#[ignore = "live: starts a dedicated 3-master/3-replica cluster for transactions"]
async fn cluster_transactions_are_slot_pinned_and_watch_safe() {
    let fixture = tokio::time::timeout(Duration::from_secs(60), ClusterFixture::start())
        .await
        .expect("timed out starting transaction fixture")
        .expect("failed to start transaction fixture");
    let seed = fixture.seed_addr();

    tokio::time::timeout(Duration::from_secs(20), async {
        // Appending outside the generated hash tag creates distinct keys in
        // exactly the same slot.
        let transaction_slot = 12_000;
        let topology = fixture
            .topology()
            .await
            .expect("failed to inspect transaction topology");
        let transaction_owner = topology
            .owner_of_slot(transaction_slot)
            .expect("transaction slot should have an owner")
            .index;
        assert_ne!(
            transaction_owner,
            topology
                .owner_of_slot(0)
                .expect("default slot should have an owner")
                .index,
            "transaction test must target a non-default master"
        );
        let same_slot_base = key_for_slot(transaction_slot);
        let counter = format!("{same_slot_base}:counter");
        let marker = format!("{same_slot_base}:marker");
        let mut shared = ClusterClient::connect(&seed)
            .await
            .expect("failed to connect shared cluster client");
        shared
            .execute(Del::keys([counter.clone(), marker.clone()]))
            .await
            .expect("failed to clean same-slot transaction keys");

        let watch_calls_before = master_commandstat_calls(&fixture, "watch").await;
        let transaction_multi_calls_before = master_commandstat_calls(&fixture, "multi").await;

        let committed = Transaction::new()
            .watch([counter.clone()])
            .push(Set::new(&counter, "40"))
            .push(Incr::new(&counter))
            .push(Set::new(&marker, "committed"))
            .push(Get::new(&counter))
            .execute(&mut shared)
            .await
            .expect("same-slot ClusterClient transaction should execute")
            .unwrap();
        assert_eq!(*committed.get::<i64>(1).unwrap(), 41);
        assert_eq!(
            committed.get::<Option<Bytes>>(3).unwrap().as_deref(),
            Some(&b"41"[..])
        );
        assert_eq!(
            shared.execute(Get::new(&marker)).await.unwrap(),
            Some(Bytes::from_static(b"committed"))
        );
        assert_commandstat_incremented_only_on(
            &watch_calls_before,
            &master_commandstat_calls(&fixture, "watch").await,
            transaction_owner,
            "WATCH",
        );
        assert_commandstat_incremented_only_on(
            &transaction_multi_calls_before,
            &master_commandstat_calls(&fixture, "multi").await,
            transaction_owner,
            "MULTI",
        );

        let mut exclusive = ClusterConnection::connect(&seed)
            .await
            .expect("failed to connect exclusive cluster connection");
        let cross_slot_a = key_for_slot(42);
        let cross_slot_b = key_for_slot(12_000);
        exclusive.execute(Del::new(&cross_slot_a)).await.ok();
        exclusive.execute(Del::new(&cross_slot_b)).await.ok();
        let multi_calls_before = master_commandstat_calls(&fixture, "multi").await;
        assert!(
            multi_calls_before.iter().any(|(_, calls)| *calls > 0),
            "the committed transaction should make MULTI commandstats observable"
        );
        let cross_slot_error = match Transaction::new()
            .push(Set::new(&cross_slot_a, "must-not-land-a"))
            .push(Set::new(&cross_slot_b, "must-not-land-b"))
            .execute(&mut exclusive)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("mixed-slot transaction unexpectedly reached Redis"),
        };
        assert!(
            cross_slot_error.to_string().contains("CROSSSLOT"),
            "unexpected mixed-slot error: {cross_slot_error}"
        );
        assert_eq!(
            master_commandstat_calls(&fixture, "multi").await,
            multi_calls_before,
            "mixed-slot preflight must reject before MULTI reaches any master"
        );
        assert_eq!(
            exclusive.execute(Get::new(&cross_slot_a)).await.unwrap(),
            None
        );
        assert_eq!(
            exclusive.execute(Get::new(&cross_slot_b)).await.unwrap(),
            None
        );

        let watch_calls_before = master_commandstat_calls(&fixture, "watch").await;
        let helper_multi_calls_before = master_commandstat_calls(&fixture, "multi").await;
        let build_called = Arc::new(AtomicBool::new(false));
        let build_flag = Arc::clone(&build_called);
        let helper_result = redis_tower::transaction_with_retries(
            &mut exclusive,
            [counter.clone()],
            0,
            async move |_connection: &mut ClusterConnection| {
                build_flag.store(true, Ordering::SeqCst);
                Ok(Transaction::new())
            },
        )
        .await;
        let helper_error = match helper_result {
            Err(error) => error,
            Ok(_) => panic!("cluster accepted the closure-based transaction helper"),
        };
        assert!(
            helper_error
                .to_string()
                .contains("transaction_with_retries()")
        );
        assert!(helper_error.to_string().contains("Transaction::watch()"));
        assert!(
            !build_called.load(Ordering::SeqCst),
            "unsupported helper invoked its build closure"
        );
        assert_eq!(
            master_commandstat_calls(&fixture, "watch").await,
            watch_calls_before,
            "unsupported helper must reject before WATCH"
        );
        assert_eq!(
            master_commandstat_calls(&fixture, "multi").await,
            helper_multi_calls_before,
            "unsupported helper must reject before MULTI"
        );

        shared
            .execute(Del::keys([counter, marker]))
            .await
            .expect("failed to clean same-slot transaction keys");
        exclusive.execute(Del::new(cross_slot_a)).await.ok();
        exclusive.execute(Del::new(cross_slot_b)).await.ok();
    })
    .await
    .expect("cluster transaction coverage exceeded its hard timeout");
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

// -- Cluster Pub/Sub tests --

mod cluster_pubsub {
    use super::*;
    use redis_tower::{
        Command, MessageKind, NodeAddr, PubSubConnection, PubSubMessage, RedisConnection,
    };
    use std::future::Future;

    const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
    const OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
    const RECOVERY_TIMEOUT: Duration = Duration::from_secs(20);

    async fn bounded<T>(operation: &str, duration: Duration, future: impl Future<Output = T>) -> T {
        tokio::time::timeout(duration, future)
            .await
            .unwrap_or_else(|_| panic!("timed out after {duration:?} while {operation}"))
    }

    async fn start_fixture() -> ClusterFixture {
        bounded(
            "starting cluster Pub/Sub fixture",
            STARTUP_TIMEOUT,
            ClusterFixture::start(),
        )
        .await
        .expect("failed to start cluster Pub/Sub fixture")
    }

    async fn node_commandstat_calls(
        fixture: &ClusterFixture,
        node_index: usize,
        command: &str,
    ) -> u64 {
        let info = bounded(
            "reading Pub/Sub commandstats",
            OPERATION_TIMEOUT,
            fixture.run_node(node_index, &["INFO", "commandstats"]),
        )
        .await
        .expect("failed to read Pub/Sub commandstats");
        commandstat_calls(&info, command)
    }

    async fn wait_for_commandstat(
        fixture: &ClusterFixture,
        node_index: usize,
        command: &str,
        minimum: u64,
    ) {
        bounded(
            "waiting for a replayed Pub/Sub subscription",
            RECOVERY_TIMEOUT,
            async {
                loop {
                    if node_commandstat_calls(fixture, node_index, command).await >= minimum {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
        )
        .await;
    }

    async fn wait_for_shard_subscribers(
        fixture: &ClusterFixture,
        node_index: usize,
        channel: &str,
        minimum: u64,
    ) {
        bounded(
            "waiting for a confirmed sharded Pub/Sub subscription",
            RECOVERY_TIMEOUT,
            async {
                loop {
                    let output = fixture
                        .run_node(node_index, &["PUBSUB", "SHARDNUMSUB", channel])
                        .await
                        .expect("failed to read sharded Pub/Sub subscriber count");
                    let count = output
                        .lines()
                        .rev()
                        .find_map(|line| line.trim().parse::<u64>().ok())
                        .unwrap_or(0);
                    if count >= minimum {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
        )
        .await;
    }

    async fn shard_subscriber_client_id(fixture: &ClusterFixture, node_index: usize) -> u64 {
        let clients = bounded(
            "listing sharded Pub/Sub clients",
            OPERATION_TIMEOUT,
            fixture.run_node(node_index, &["CLIENT", "LIST", "TYPE", "PUBSUB"]),
        )
        .await
        .expect("failed to list sharded Pub/Sub clients");

        let ids: Vec<u64> = clients
            .lines()
            .filter(|line| {
                line.split_whitespace().any(|field| {
                    field
                        .strip_prefix("ssub=")
                        .and_then(|count| count.parse::<u64>().ok())
                        .is_some_and(|count| count > 0)
                })
            })
            .filter_map(|line| {
                line.split_whitespace().find_map(|field| {
                    field
                        .strip_prefix("id=")
                        .and_then(|id| id.parse::<u64>().ok())
                })
            })
            .collect();
        assert_eq!(
            ids.len(),
            1,
            "expected exactly one sharded Pub/Sub client on node {node_index}, got {clients:?}"
        );
        ids[0]
    }

    fn node_addr(addr: &str) -> NodeAddr {
        NodeAddr::parse(addr).unwrap_or_else(|| panic!("invalid fixture node address {addr}"))
    }

    fn assert_message(message: &PubSubMessage, kind: MessageKind, channel: &str, payload: &[u8]) {
        assert_eq!(message.kind, kind);
        assert_eq!(message.channel, channel);
        assert_eq!(message.payload.as_ref(), payload);
    }

    /// Exercise both cluster Pub/Sub modes in one dedicated fixture. Regular
    /// subscriptions must stay on the designated node, while shard channels
    /// and SPUBLISH must route to their hash-slot owner. Mixed-slot shard
    /// subscriptions are rejected without sending SSUBSCRIBE to any master.
    #[tokio::test]
    #[ignore = "live: starts a dedicated 3-master/3-replica cluster for Pub/Sub routing"]
    async fn routes_regular_and_sharded_pubsub_and_rejects_cross_slot() {
        let extracted_channel = "{redis-tower:cluster-pubsub}:extractor";
        let spublish_frame = SPublish::new(extracted_channel, "payload").to_frame();
        assert_eq!(
            redis_tower_cluster::key_extractor::extract_key(&spublish_frame),
            Some(extracted_channel.as_bytes())
        );
        assert_eq!(
            redis_tower_cluster::key_extractor::pipeline_routing_slot(&spublish_frame)
                .expect("SPUBLISH should have one valid routing slot"),
            Some(slot_for_key(extracted_channel.as_bytes()))
        );

        let fixture = start_fixture().await;
        let topology = bounded(
            "reading cluster Pub/Sub topology",
            OPERATION_TIMEOUT,
            fixture.topology(),
        )
        .await
        .expect("failed to read cluster Pub/Sub topology");
        let seed = fixture.seed_addr();
        let designated = topology
            .masters()
            .find(|node| node.addr != seed)
            .expect("fixture should have a non-seed master")
            .clone();
        let client = bounded(
            "connecting cluster Pub/Sub client",
            OPERATION_TIMEOUT,
            MultiplexedClusterClient::connect(&seed),
        )
        .await
        .expect("failed to connect cluster Pub/Sub client");

        let subscribe_before = bounded(
            "capturing regular subscription commandstats",
            RECOVERY_TIMEOUT,
            master_commandstat_calls(&fixture, "subscribe"),
        )
        .await;
        let mut regular = bounded(
            "opening designated-node Pub/Sub connection",
            OPERATION_TIMEOUT,
            client.pubsub_on(node_addr(&designated.addr)),
        )
        .await
        .expect("failed to open designated-node Pub/Sub connection");
        assert_eq!(regular.current_node().addr_string(), designated.addr);
        let regular_channel = "redis-tower:cluster-pubsub:regular";
        bounded(
            "subscribing on designated cluster node",
            OPERATION_TIMEOUT,
            regular.subscribe(&[regular_channel]),
        )
        .await
        .expect("regular cluster subscription failed");
        let subscribe_after = bounded(
            "checking regular subscription commandstats",
            RECOVERY_TIMEOUT,
            master_commandstat_calls(&fixture, "subscribe"),
        )
        .await;
        assert_commandstat_incremented_only_on(
            &subscribe_before,
            &subscribe_after,
            designated.index,
            "SUBSCRIBE",
        );
        assert!(regular.subscriptions().channels.contains(regular_channel));

        bounded(
            "publishing regular cluster message",
            OPERATION_TIMEOUT,
            client.execute(Publish::new(regular_channel, "regular-payload")),
        )
        .await
        .expect("regular cluster publish failed");
        let message = bounded(
            "receiving regular cluster message",
            OPERATION_TIMEOUT,
            regular.next_message(),
        )
        .await
        .expect("regular cluster subscription ended");
        assert_message(
            &message,
            MessageKind::Message,
            regular_channel,
            b"regular-payload",
        );

        let shard_slot = [42_u16, 6_000, 12_000]
            .into_iter()
            .find(|slot| {
                topology
                    .owner_of_slot(*slot)
                    .is_some_and(|owner| owner.addr != seed)
            })
            .expect("fixture should assign a test slot to a non-seed master");
        let shard_owner = topology
            .owner_of_slot(shard_slot)
            .expect("shard slot should have an owner")
            .clone();
        let shard_tag = key_for_slot(shard_slot);
        let shard_channel_a = format!("{shard_tag}:a");
        let shard_channel_b = format!("{shard_tag}:b");
        assert_eq!(slot_for_key(shard_channel_a.as_bytes()), shard_slot);
        assert_eq!(
            slot_for_key(shard_channel_a.as_bytes()),
            slot_for_key(shard_channel_b.as_bytes())
        );

        let mut sharded = bounded(
            "opening same-slot sharded Pub/Sub connection",
            OPERATION_TIMEOUT,
            client.sharded_pubsub(&[&shard_channel_a, &shard_channel_b]),
        )
        .await
        .expect("same-slot sharded subscription failed");
        assert_eq!(sharded.slot(), shard_slot);
        assert_eq!(sharded.current_node().addr_string(), shard_owner.addr);
        assert!(
            sharded
                .subscriptions()
                .shard_channels
                .contains(&shard_channel_a)
        );
        assert!(
            sharded
                .subscriptions()
                .shard_channels
                .contains(&shard_channel_b)
        );

        let spublish_before = bounded(
            "capturing SPUBLISH routing commandstats",
            RECOVERY_TIMEOUT,
            master_commandstat_calls(&fixture, "spublish"),
        )
        .await;
        let shard_receivers = bounded(
            "publishing first sharded cluster message",
            OPERATION_TIMEOUT,
            client.execute(SPublish::new(&shard_channel_a, "shard-a")),
        )
        .await
        .expect("first sharded cluster publish failed");
        assert_eq!(shard_receivers, 1);
        let spublish_after = bounded(
            "checking SPUBLISH routing commandstats",
            RECOVERY_TIMEOUT,
            master_commandstat_calls(&fixture, "spublish"),
        )
        .await;
        assert_commandstat_incremented_only_on(
            &spublish_before,
            &spublish_after,
            shard_owner.index,
            "SPUBLISH",
        );
        let message = bounded(
            "receiving first sharded cluster message",
            OPERATION_TIMEOUT,
            sharded.next_message(),
        )
        .await
        .expect("first sharded cluster subscription ended");
        assert_message(
            &message,
            MessageKind::SMessage,
            &shard_channel_a,
            b"shard-a",
        );

        let shard_receivers = bounded(
            "publishing second sharded cluster message",
            OPERATION_TIMEOUT,
            client.execute(SPublish::new(&shard_channel_b, "shard-b")),
        )
        .await
        .expect("second sharded cluster publish failed");
        assert_eq!(shard_receivers, 1);
        let message = bounded(
            "receiving second sharded cluster message",
            OPERATION_TIMEOUT,
            sharded.next_message(),
        )
        .await
        .expect("second sharded cluster subscription ended");
        assert_message(
            &message,
            MessageKind::SMessage,
            &shard_channel_b,
            b"shard-b",
        );

        let cross_slot_a = key_for_slot(42);
        let cross_slot_b = key_for_slot(12_000);
        let ssubscribe_before = bounded(
            "capturing cross-slot SSUBSCRIBE commandstats",
            RECOVERY_TIMEOUT,
            master_commandstat_calls(&fixture, "ssubscribe"),
        )
        .await;
        let error = match bounded(
            "rejecting cross-slot sharded subscription",
            OPERATION_TIMEOUT,
            client.sharded_pubsub(&[&cross_slot_a, &cross_slot_b]),
        )
        .await
        {
            Ok(_) => panic!("cross-slot sharded subscription unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("CROSSSLOT"),
            "unexpected cross-slot sharded subscription error: {error}"
        );
        let ssubscribe_after = bounded(
            "checking cross-slot SSUBSCRIBE commandstats",
            RECOVERY_TIMEOUT,
            master_commandstat_calls(&fixture, "ssubscribe"),
        )
        .await;
        assert_eq!(
            ssubscribe_after, ssubscribe_before,
            "cross-slot validation must reject before SSUBSCRIBE reaches Redis"
        );

        drop(sharded);
        drop(regular);
        bounded(
            "shutting down cluster Pub/Sub client",
            OPERATION_TIMEOUT,
            client.shutdown(),
        )
        .await;
    }

    /// A fixed-node regular subscription must reconnect to that exact node and
    /// replay its tracked channels. Waiting for the second SUBSCRIBE command
    /// before publishing makes the at-most-once reconnect gap deterministic.
    #[tokio::test]
    #[ignore = "live: kills a designated-node Pub/Sub connection"]
    async fn regular_pubsub_reconnects_to_designated_node_and_resubscribes() {
        let fixture = start_fixture().await;
        let topology = bounded(
            "reading reconnect Pub/Sub topology",
            OPERATION_TIMEOUT,
            fixture.topology(),
        )
        .await
        .expect("failed to read reconnect Pub/Sub topology");
        let designated = topology
            .masters()
            .nth(1)
            .expect("fixture should have a second master")
            .clone();
        let client = bounded(
            "connecting reconnect Pub/Sub client",
            OPERATION_TIMEOUT,
            MultiplexedClusterClient::connect(&fixture.seed_addr()),
        )
        .await
        .expect("failed to connect reconnect Pub/Sub client");
        let mut subscriber = bounded(
            "opening reconnecting designated-node subscription",
            OPERATION_TIMEOUT,
            client.pubsub_on(node_addr(&designated.addr)),
        )
        .await
        .expect("failed to open reconnecting designated-node subscription");
        let channel = "redis-tower:cluster-pubsub:reconnect";
        bounded(
            "establishing regular subscription before reconnect",
            OPERATION_TIMEOUT,
            subscriber.subscribe(&[channel]),
        )
        .await
        .expect("failed to establish regular subscription before reconnect");
        let subscribe_calls = node_commandstat_calls(&fixture, designated.index, "subscribe").await;

        bounded(
            "killing designated-node Pub/Sub connection",
            OPERATION_TIMEOUT,
            fixture.run_node(designated.index, &["CLIENT", "KILL", "TYPE", "PUBSUB"]),
        )
        .await
        .expect("failed to kill designated-node Pub/Sub connection");

        let (message, ()) = bounded(
            "reconnecting, replaying, and receiving regular Pub/Sub message",
            Duration::from_secs(30),
            async {
                tokio::join!(
                    bounded(
                        "reconnecting and receiving regular Pub/Sub message",
                        RECOVERY_TIMEOUT,
                        subscriber.next_message(),
                    ),
                    async {
                        wait_for_commandstat(
                            &fixture,
                            designated.index,
                            "subscribe",
                            subscribe_calls + 1,
                        )
                        .await;
                        bounded(
                            "publishing after regular Pub/Sub reconnect",
                            OPERATION_TIMEOUT,
                            client.execute(Publish::new(channel, "after-reconnect")),
                        )
                        .await
                        .expect("publish after regular Pub/Sub reconnect failed");
                    }
                )
            },
        )
        .await;
        let message = message.expect("regular Pub/Sub reconnect did not yield a message");
        assert_message(&message, MessageKind::Message, channel, b"after-reconnect");
        assert_eq!(subscriber.current_node().addr_string(), designated.addr);
        assert!(subscriber.subscriptions().channels.contains(channel));

        drop(subscriber);
        bounded(
            "shutting down regular reconnect client",
            OPERATION_TIMEOUT,
            client.shutdown(),
        )
        .await;
    }

    /// Moving a subscribed shard channel's slot must move the Pub/Sub socket
    /// and replay SSUBSCRIBE on the new owner. The message is published only
    /// after commandstats prove the replay completed, avoiding a race with the
    /// documented at-most-once gap during relocation.
    #[tokio::test]
    #[ignore = "live: starts a dedicated cluster and moves a subscribed shard slot"]
    async fn sharded_pubsub_follows_topology_owner_change_and_resubscribes() {
        let fixture = start_fixture().await;
        let before = bounded(
            "reading pre-reshard Pub/Sub topology",
            OPERATION_TIMEOUT,
            fixture.topology(),
        )
        .await
        .expect("failed to read pre-reshard Pub/Sub topology");
        let slot = 42;
        let source = before
            .owner_of_slot(slot)
            .expect("shard slot should have a source owner")
            .clone();
        let target = before
            .masters()
            .find(|node| node.id != source.id)
            .expect("fixture should have another shard master")
            .clone();
        let channel = format!("{}:topology", key_for_slot(slot));
        let client = bounded(
            "connecting topology-aware Pub/Sub client",
            OPERATION_TIMEOUT,
            MultiplexedClusterClient::connect(&fixture.seed_addr()),
        )
        .await
        .expect("failed to connect topology-aware Pub/Sub client");
        let mut subscriber = bounded(
            "opening topology-aware sharded subscription",
            OPERATION_TIMEOUT,
            client.sharded_pubsub(&[&channel]),
        )
        .await
        .expect("failed to open topology-aware sharded subscription");
        assert_eq!(subscriber.current_node().addr_string(), source.addr);
        assert_eq!(subscriber.slot(), slot);

        let receivers = bounded(
            "publishing baseline sharded message",
            OPERATION_TIMEOUT,
            client.execute(SPublish::new(&channel, "before-reshard")),
        )
        .await
        .expect("baseline sharded publish failed");
        assert_eq!(receivers, 1);
        let message = bounded(
            "receiving baseline sharded message",
            OPERATION_TIMEOUT,
            subscriber.next_message(),
        )
        .await
        .expect("baseline sharded subscription ended");
        assert_message(&message, MessageKind::SMessage, &channel, b"before-reshard");

        let target_subscribe_calls =
            node_commandstat_calls(&fixture, target.index, "ssubscribe").await;
        let (message, ()) = bounded(
            "relocating, replaying, and receiving sharded Pub/Sub message",
            Duration::from_secs(45),
            async {
                tokio::join!(
                    bounded(
                        "relocating and receiving sharded Pub/Sub message",
                        RECOVERY_TIMEOUT,
                        subscriber.next_message(),
                    ),
                    async {
                        bounded(
                            "moving subscribed shard slot",
                            OPERATION_TIMEOUT,
                            fixture.reshard_slot(slot, target.index),
                        )
                        .await
                        .expect("failed to move subscribed shard slot");
                        bounded(
                            "waiting for subscribed shard slot owner",
                            OPERATION_TIMEOUT,
                            fixture.wait_for_slot_owner(slot, &target.id, OPERATION_TIMEOUT),
                        )
                        .await
                        .expect("new shard owner was not advertised");
                        bounded(
                            "refreshing topology after subscribed shard moved",
                            OPERATION_TIMEOUT,
                            client.refresh_topology(),
                        )
                        .await
                        .expect("cluster client failed to refresh moved shard topology");
                        wait_for_commandstat(
                            &fixture,
                            target.index,
                            "ssubscribe",
                            target_subscribe_calls + 1,
                        )
                        .await;

                        let receivers = bounded(
                            "publishing after sharded Pub/Sub relocation",
                            OPERATION_TIMEOUT,
                            client.execute(SPublish::new(&channel, "after-reshard")),
                        )
                        .await
                        .expect("sharded publish after relocation failed");
                        assert_eq!(receivers, 1);
                    }
                )
            },
        )
        .await;
        let message = message.expect("relocated sharded Pub/Sub did not yield a message");
        assert_message(&message, MessageKind::SMessage, &channel, b"after-reshard");
        assert_eq!(subscriber.current_node().addr_string(), target.addr);
        assert_eq!(subscriber.slot(), slot);
        assert!(subscriber.subscriptions().shard_channels.contains(&channel));
        let target_subscriber_id = shard_subscriber_client_id(&fixture, target.index).await;

        // Move the slot back without refreshing the client, then break the
        // active Pub/Sub socket. The reconnect first reaches the stale owner;
        // its replayed SSUBSCRIBE returns MOVED, which must update shared
        // routing and continue the same reconnect campaign on the new owner.
        let source_subscribe_calls =
            node_commandstat_calls(&fixture, source.index, "ssubscribe").await;
        let (message, ()) = bounded(
            "following a replay-time Pub/Sub MOVED",
            Duration::from_secs(45),
            async {
                tokio::join!(
                    bounded(
                        "receiving after replay-time Pub/Sub MOVED",
                        RECOVERY_TIMEOUT,
                        subscriber.next_message(),
                    ),
                    async {
                        bounded(
                            "moving subscribed shard slot back",
                            OPERATION_TIMEOUT,
                            fixture.reshard_slot(slot, source.index),
                        )
                        .await
                        .expect("failed to move subscribed shard slot back");
                        bounded(
                            "waiting for restored shard slot owner",
                            OPERATION_TIMEOUT,
                            fixture.wait_for_slot_owner(slot, &source.id, OPERATION_TIMEOUT),
                        )
                        .await
                        .expect("restored shard owner was not advertised");
                        let stale = RedisConnection::connect(&target.addr)
                            .await
                            .expect("failed to connect to stale shard owner");
                        let mut stale = PubSubConnection::from_connection(stale)
                            .expect("failed to open stale-owner Pub/Sub connection");
                        let stale_error = bounded(
                            "probing stale-owner SSUBSCRIBE redirect",
                            OPERATION_TIMEOUT,
                            stale.ssubscribe(&[&channel]),
                        )
                        .await
                        .expect_err("stale shard owner unexpectedly accepted SSUBSCRIBE");
                        assert!(
                            stale_error.is_moved(),
                            "stale shard owner returned {stale_error} instead of MOVED"
                        );
                        let target_subscriber_id = target_subscriber_id.to_string();
                        let killed = bounded(
                            "breaking the stale-owner Pub/Sub socket",
                            OPERATION_TIMEOUT,
                            fixture.run_node(
                                target.index,
                                &["CLIENT", "KILL", "ID", &target_subscriber_id],
                            ),
                        )
                        .await
                        .expect("failed to break stale-owner Pub/Sub socket");
                        assert_eq!(
                            killed.trim(),
                            "1",
                            "exact stale-owner Pub/Sub socket was not killed"
                        );
                        wait_for_commandstat(
                            &fixture,
                            source.index,
                            "ssubscribe",
                            source_subscribe_calls + 1,
                        )
                        .await;
                        wait_for_shard_subscribers(&fixture, source.index, &channel, 1).await;

                        let receivers = bounded(
                            "publishing after replay-time Pub/Sub MOVED",
                            OPERATION_TIMEOUT,
                            client.execute(SPublish::new(&channel, "after-replay-moved")),
                        )
                        .await
                        .expect("sharded publish after replay-time MOVED failed");
                        assert_eq!(receivers, 1);
                    }
                )
            },
        )
        .await;
        let message = message.expect("replay-time MOVED did not yield a message");
        assert_message(
            &message,
            MessageKind::SMessage,
            &channel,
            b"after-replay-moved",
        );
        assert_eq!(subscriber.current_node().addr_string(), source.addr);

        drop(subscriber);
        bounded(
            "shutting down sharded relocation client",
            OPERATION_TIMEOUT,
            client.shutdown(),
        )
        .await;
    }
}
