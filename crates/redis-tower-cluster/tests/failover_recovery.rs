//! Recovery from failovers, demotions, and node loss against scripted fake
//! cluster nodes (see `fake_cluster`).

mod fake_cluster;

use bytes::Bytes;
use fake_cluster::*;
use redis_tower::commands::RawCommand;
use redis_tower_cluster::{ClusterConnection, MultiplexedClusterClient, ReadPreference};
use redis_tower_core::Frame;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::time::Duration;

/// Answers `READONLY` to `command` on `demoted_port` once `demoted` is set,
/// like a master that a failover demoted with its sockets intact.
fn readonly_after_demotion(
    command: &'static [u8],
    demoted_port: Arc<AtomicU16>,
    demoted: Arc<AtomicBool>,
) -> Handler {
    Arc::new(move |request| {
        (request.is(command)
            && demoted.load(Ordering::SeqCst)
            && request.port == demoted_port.load(Ordering::SeqCst))
        .then(|| Action::Reply(err(READONLY_ERR)))
    })
}

/// A keyless write (keyless commands bypass the MOVED check) sent to the
/// demoted default master is rejected with READONLY. The client must refresh
/// and re-send it to a current master instead of failing forever.
#[tokio::test]
async fn multiplexed_keyless_write_on_demoted_default_master_is_rerouted() {
    let cluster = Cluster::new();
    let demoted_port = Arc::new(AtomicU16::new(0));
    let demoted = Arc::new(AtomicBool::new(false));
    let handler = readonly_after_demotion(b"FLUSHALL", demoted_port.clone(), demoted.clone());
    let a = fake_node(cluster.clone(), handler.clone()).await;
    let b = fake_node(cluster.clone(), handler.clone()).await;
    let c = fake_node(cluster.clone(), handler).await;
    demoted_port.store(a.port, Ordering::SeqCst);
    cluster.set(slots(vec![
        (0, 8191, a.port, vec![b.port]),
        (8192, 16383, c.port, vec![]),
    ]));
    let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
    let flushall = || RawCommand::new("FLUSHALL");
    client.execute(flushall()).await.unwrap();

    // CLUSTER FAILOVER: b is promoted, a becomes its replica; sockets stay up.
    cluster.set(slots(vec![
        (0, 8191, b.port, vec![a.port]),
        (8192, 16383, c.port, vec![]),
    ]));
    demoted.store(true, Ordering::SeqCst);

    client
        .execute(flushall())
        .await
        .expect("keyless write after the default master was demoted");
    assert_eq!(
        a.received("FLUSHALL"),
        2,
        "one rejected attempt on the demoted node"
    );
    assert_eq!(b.received("FLUSHALL"), 1, "re-sent to the promoted master");
}

/// redis-rs #2115: a keyed write answered with READONLY instead of MOVED (a
/// managed service, or a connection in READONLY mode). Both clients refresh
/// and re-send to the promoted master. The multiplexed client must do so even
/// inside the background refresh rate-limit window, which a MOVED just
/// opened.
#[tokio::test]
async fn keyed_write_answered_readonly_is_rerouted_inside_the_refresh_window() {
    let cluster = Cluster::new();
    let demoted_port = Arc::new(AtomicU16::new(0));
    let demoted = Arc::new(AtomicBool::new(false));
    let moved_once = Arc::new(AtomicBool::new(false));
    let handler: Handler = {
        let readonly = readonly_after_demotion(b"SET", demoted_port.clone(), demoted.clone());
        let demoted_port = demoted_port.clone();
        let moved_once = moved_once.clone();
        Arc::new(move |request| {
            if request.is(b"GET")
                && request.port == demoted_port.load(Ordering::SeqCst)
                && !moved_once.swap(true, Ordering::SeqCst)
            {
                // MOVED to the same node: harmless, but it schedules a
                // rate-limited background refresh.
                let slot = redis_tower_cluster::slot_for_key(&request.args[1]);
                return Some(Action::Reply(err(&format!(
                    "MOVED {slot} 127.0.0.1:{}",
                    request.port
                ))));
            }
            readonly(request)
        })
    };
    let a = fake_node(cluster.clone(), handler.clone()).await;
    let b = fake_node(cluster.clone(), handler).await;
    demoted_port.store(a.port, Ordering::SeqCst);
    cluster.set(slots(vec![(0, 16383, a.port, vec![b.port])]));
    let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
    let mut direct = ClusterConnection::connect(&a.addr()).await.unwrap();

    client
        .execute(RawCommand::new("GET").arg("k"))
        .await
        .unwrap();
    // Let the MOVED-triggered background refresh finish, then fail over.
    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.set(slots(vec![(0, 16383, b.port, vec![a.port])]));
    demoted.store(true, Ordering::SeqCst);

    let set = || RawCommand::new("SET").arg("k").arg("v");
    client
        .execute(set())
        .await
        .expect("multiplexed keyed write answered READONLY");
    direct
        .execute(set())
        .await
        .expect("ClusterConnection keyed write answered READONLY");
    assert_eq!(b.received("SET"), 2);
}

/// READONLY on every attempt exhausts the redirect budget and surfaces the
/// server's error rather than looping.
#[tokio::test]
async fn persistent_readonly_surfaces_the_server_error_after_the_budget() {
    let cluster = Cluster::new();
    let handler: Handler =
        Arc::new(|request| request.is(b"SET").then(|| Action::Reply(err(READONLY_ERR))));
    let a = fake_node(cluster.clone(), handler).await;
    cluster.set(slots(vec![(0, 16383, a.port, vec![])]));
    let client = MultiplexedClusterClient::builder(a.addr())
        .max_redirects(2)
        .connect()
        .await
        .unwrap();
    let mut direct = ClusterConnection::builder(a.addr())
        .max_redirects(2)
        .connect()
        .await
        .unwrap();

    let set = || RawCommand::new("SET").arg("k").arg("v");
    for error in [
        client.execute(set()).await.unwrap_err(),
        direct.execute(set()).await.unwrap_err(),
    ] {
        assert!(
            matches!(&error, redis_tower_core::RedisError::Redis(text) if text == READONLY_ERR),
            "expected the server's READONLY text, got {error:?}"
        );
    }
    assert_eq!(
        a.received("SET"),
        6,
        "initial attempt plus two follow-ups each"
    );
}

/// `ClusterConnection` used to keep one socket per address regardless of
/// role. After a failover the demoted master is the shard's replica, but its
/// socket was opened as a master (no READONLY), so strict replica reads on it
/// bounced with MOVED forever.
#[tokio::test]
async fn cluster_connection_reopens_a_demoted_masters_socket_for_replica_reads() {
    let cluster = Cluster::new();
    let replica_port = Arc::new(AtomicU16::new(0));
    let handler: Handler = {
        let replica_port = replica_port.clone();
        Arc::new(move |request| {
            if !request.is(b"GET") {
                return None;
            }
            if request.port == replica_port.load(Ordering::SeqCst) && !request.readonly {
                let slot = redis_tower_cluster::slot_for_key(&request.args[1]);
                return Some(Action::Reply(err(&format!("MOVED {slot} 127.0.0.1:0"))));
            }
            Some(Action::Reply(Frame::BulkString(Some(Bytes::from(
                format!("from-{}", request.port),
            )))))
        })
    };
    let a = fake_node(cluster.clone(), handler.clone()).await;
    let b = fake_node(cluster.clone(), handler).await;
    replica_port.store(b.port, Ordering::SeqCst);
    cluster.set(slots(vec![(0, 16383, a.port, vec![b.port])]));
    let mut conn = ClusterConnection::builder(a.addr())
        .read_preference(ReadPreference::Replica)
        .connect()
        .await
        .unwrap();
    assert_eq!(
        conn.execute(RawCommand::new("GET").arg("k")).await.unwrap(),
        Frame::BulkString(Some(Bytes::from(format!("from-{}", b.port))))
    );

    // Failover: a (whose socket was opened as a master) is now the replica.
    cluster.set(slots(vec![(0, 16383, b.port, vec![a.port])]));
    replica_port.store(a.port, Ordering::SeqCst);
    conn.refresh_topology().await.unwrap();

    for _ in 0..3 {
        assert_eq!(
            conn.execute(RawCommand::new("GET").arg("k")).await.unwrap(),
            Frame::BulkString(Some(Bytes::from(format!("from-{}", a.port)))),
            "strict replica read after the failover"
        );
    }
    conn.execute(RawCommand::new("SET").arg("k").arg("v"))
        .await
        .expect("writes reach the promoted master");
    assert_eq!(b.received("SET"), 1);
}

/// A `ClusterConnection` refresh asked only its first socket, failed when one
/// new master was unreachable, and never moved the keyless default.
#[tokio::test]
async fn cluster_connection_refresh_survives_a_dead_seed_and_moves_the_default() {
    let cluster = Cluster::new();
    let a = fake_node(cluster.clone(), default_handler()).await;
    let b = fake_node(cluster.clone(), default_handler()).await;
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_port = dead.local_addr().unwrap().port();
    drop(dead);
    cluster.set(slots(vec![
        (0, 8191, a.port, vec![]),
        (8192, 16383, b.port, vec![]),
    ]));
    let mut conn = ClusterConnection::connect(&a.addr()).await.unwrap();

    // a dies; b takes every slot, and a not-yet-reachable master is listed.
    a.kill_connections();
    drop(a);
    cluster.set(slots(vec![
        (0, 8191, b.port, vec![]),
        (8192, 16383, dead_port, vec![]),
    ]));
    conn.refresh_topology()
        .await
        .expect("refresh asks the next node and skips the unreachable one");
    assert_eq!(
        conn.execute(RawCommand::new("PING")).await.unwrap(),
        Frame::SimpleString(Bytes::from_static(b"PONG")),
        "keyless commands follow the default to a current master"
    );
    assert_eq!(b.received("PING"), 1);
}
