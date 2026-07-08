//! Destructive sentinel failover sequence.
//!
//! These phases are split out of `sentinel_integration.rs` because they kill
//! processes in the topology (first a sentinel, then the master), which
//! permanently degrades it for anything that runs afterwards. Two properties
//! keep the sentinel suite robust to parallel and reordered execution (nextest,
//! `--test-threads`, alphabetical reordering):
//!
//! 1. This is a **separate test binary** on its own port block (master 6393,
//!    replicas 6394-6395, sentinels 26392-26394), so it never shares a topology
//!    with the healthy `sentinel_integration` suite even when Cargo runs the two
//!    binaries concurrently.
//! 2. The destructive steps live in a **single orchestrating test** that fixes
//!    their order. Kill-a-sentinel, kill-the-master failover, and
//!    reconnect-after-failover each depend on the state the previous step left
//!    behind, so they cannot be allowed to run as independent, reorderable
//!    tests.
//!
//! Run with:
//! `cargo test -p redis-tower-sentinel --test sentinel_failover --all-features -- --ignored`

use bytes::Bytes;
use redis_server_wrapper::{RedisSentinel, RedisSentinelHandle};
use redis_tower_commands::*;
use redis_tower_sentinel::{SentinelClient, SentinelConnection};

/// Format the `ip:port` pair from a sentinel `poke()` response.
fn master_addr(info: &std::collections::HashMap<String, String>) -> String {
    format!(
        "{}:{}",
        info.get("ip").expect("no ip in sentinel response"),
        info.get("port").expect("no port in sentinel response"),
    )
}

/// Drives the destructive sentinel phases in a fixed order against a dedicated
/// topology. Splitting these out of the healthy suite (and into one test)
/// removes the order-dependence flake described in #509.
#[tokio::test]
#[ignore]
async fn sentinel_failover_sequence() {
    let handle: RedisSentinelHandle = RedisSentinel::builder()
        .master_port(6393)
        .replica_base_port(6394)
        .sentinel_base_port(26392)
        .replicas(2)
        .sentinels(3)
        .quorum(2)
        .start()
        .await
        .expect("failed to start sentinel topology");

    // -- Phase 1: discovery survives one sentinel down --
    //
    // Kill one sentinel process. The topology has 3 sentinels; quorum is 2, so
    // discovery must still succeed using the remaining 2. The killed sentinel's
    // address stays in `sentinel_addrs`, so `discover_master` tries the dead one
    // (failing fast via the per-sentinel timeout) before falling back to a live
    // sentinel.
    let addrs = handle.sentinel_addrs();
    let pids = handle.pids();
    // pids: [master, replica1, replica2, sentinel1, sentinel2, sentinel3]
    let sentinel_pid = pids[3];
    std::process::Command::new("kill")
        .args(["-9", &sentinel_pid.to_string()])
        .status()
        .expect("kill failed");

    // Give the OS a moment to reap the process.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let addr = redis_tower_sentinel::discovery::discover_master(&addrs, "mymaster")
        .await
        .expect("discovery should succeed with 2/3 sentinels alive");
    assert!(
        addr.contains("6393") || addr.contains("6394") || addr.contains("6395"),
        "discovered address should be a known redis port, got: {addr}"
    );

    // -- Phase 2: failover after the master is killed --
    let client = SentinelClient::connect(&addrs, "mymaster").await.unwrap();

    // Verify initial connectivity.
    let k = "sentinel_failover_seq:before";
    client.execute(Set::new(k, "before")).await.unwrap();
    let val: Option<Bytes> = client.execute(Get::new(k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("before")));

    // Record the sentinel-reported master address before the failover.
    // `handle.master_addr()` returns the original static address of the master
    // handle, which does not change after a failover, so we `poke()` the
    // sentinel for the live master address instead.
    let initial_info = handle
        .poke()
        .await
        .expect("sentinel poke failed before kill");
    let initial_master = master_addr(&initial_info);

    // Kill the master process (index 0 is always the master).
    let master_pid = handle.pids()[0];
    std::process::Command::new("kill")
        .args(["-9", &master_pid.to_string()])
        .status()
        .expect("kill failed");

    // Wait for sentinel to elect a new master. After the kill, the topology has
    // one fewer replica (the promoted node is now master), so we poll the
    // sentinel directly for a master with flags="master" rather than relying on
    // the full-replica-count health check in `wait_for_healthy`.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let new_master = loop {
        if let Ok(info) = handle.poke().await {
            let flags = info.get("flags").map(String::as_str).unwrap_or("");
            if flags == "master" {
                let addr = master_addr(&info);
                if addr != initial_master {
                    break addr;
                }
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sentinel did not elect a new master within 30 seconds"
        );
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };

    // Force the client to rediscover the current master.
    client.rediscover().await.unwrap();

    // Verify commands succeed on the promoted master.
    let k2 = "sentinel_failover_seq:after";
    client.execute(Set::new(k2, "after")).await.unwrap();
    let val2: Option<Bytes> = client.execute(Get::new(k2)).await.unwrap();
    assert_eq!(val2, Some(Bytes::from("after")));

    // The promoted master must be a different node than the original.
    assert_ne!(
        initial_master, new_master,
        "expected a different master after failover"
    );

    // -- Phase 3: a fresh connection discovers the new master --
    //
    // Without calling `rediscover()` manually, a brand-new `SentinelConnection`
    // must discover the newly elected master and execute commands.
    let mut conn = SentinelConnection::connect(&addrs, "mymaster")
        .await
        .expect("fresh connection should discover the current master");

    let k3 = "sentinel_failover_seq:reconnect";
    conn.execute(Set::new(k3, "rediscovered"))
        .await
        .expect("set should succeed on new master");
    let val3: Option<Bytes> = conn
        .execute(Get::new(k3))
        .await
        .expect("get should succeed on new master");
    assert_eq!(val3, Some(Bytes::from("rediscovered")));
    conn.execute(Del::new(k3)).await.ok();
}
