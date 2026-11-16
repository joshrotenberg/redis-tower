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

/// A keyless write (keyless commands bypass the MOVED check) sent to a
/// demoted master is rejected with READONLY. The client must refresh and
/// re-send it to the promoted master instead of failing forever. FLUSHALL is
/// node-scoped, so it runs on every master: only the demoted one is re-sent.
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
    assert_eq!(c.received("FLUSHALL"), 2, "the other master is not re-sent");
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

/// A replica restarts and stays down longer than its worker's reconnect
/// budget, so the worker exits. Reads route around a dead worker, so nothing
/// scheduled the refresh that rebuilds it: strict replica reads failed
/// forever and PreferReplica reads silently stayed on the master.
#[tokio::test]
async fn multiplexed_replica_reads_recover_after_the_replica_restarts() {
    for preference in [ReadPreference::Replica, ReadPreference::PreferReplica] {
        let cluster = Cluster::new();
        let a = fake_node(cluster.clone(), default_handler()).await;
        let b = fake_node(cluster.clone(), default_handler()).await;
        let b_port = b.port;
        cluster.set(slots(vec![(0, 16383, a.port, vec![b_port])]));
        let client = MultiplexedClusterClient::builder(a.addr())
            .read_preference(preference)
            .connect()
            .await
            .unwrap();
        client
            .execute(RawCommand::new("GET").arg("k"))
            .await
            .unwrap();
        assert_eq!(b.received("GET"), 1);

        // Connections closed and the port refusing for longer than the
        // worker's bounded reconnect budget.
        drop(b);
        tokio::time::sleep(Duration::from_secs(2)).await;
        let b = fake_node_at(cluster.clone(), default_handler(), b_port).await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let result = client.execute(RawCommand::new("GET").arg("k")).await;
            if result.is_ok() && b.received("GET") > 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{preference:?}: replica reads never returned to the restarted replica \
                 (last result {result:?})"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        client.shutdown().await;
    }
}

/// `ClusterConnection` dropped a replica socket after one I/O error and never
/// reconnected it, disabling replica reads for its slots permanently.
#[tokio::test]
async fn cluster_connection_reconnects_a_replica_after_one_io_error() {
    let cluster = Cluster::new();
    let replica_port = Arc::new(AtomicU16::new(0));
    let dropped_once = Arc::new(AtomicBool::new(false));
    let handler: Handler = {
        let replica_port = replica_port.clone();
        let dropped_once = dropped_once.clone();
        Arc::new(move |request| {
            (request.port == replica_port.load(Ordering::SeqCst)
                && request.is(b"GET")
                && !dropped_once.swap(true, Ordering::SeqCst))
            .then_some(Action::Close)
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

    assert!(conn.execute(RawCommand::new("GET").arg("k")).await.is_err());
    conn.execute(RawCommand::new("GET").arg("k"))
        .await
        .expect("the replica socket is reconnected on the next read");
    assert_eq!(b.accepted.load(Ordering::SeqCst), 2);
    assert_eq!(
        a.received("GET"),
        0,
        "strict replica reads never use the master"
    );
}

/// A replica whose reconnect fails is skipped for a growing backoff rather
/// than costing a connect attempt on every read, then retried.
#[tokio::test]
async fn cluster_connection_backs_off_a_replica_whose_reconnect_fails() {
    let cluster = Cluster::new();
    let reject_setup = Arc::new(AtomicBool::new(false));
    let handler: Handler = {
        let reject_setup = reject_setup.clone();
        Arc::new(move |request| {
            (request.is(b"READONLY") && reject_setup.load(Ordering::SeqCst))
                .then_some(Action::Close)
        })
    };
    let a = fake_node(cluster.clone(), default_handler()).await;
    let b = fake_node(cluster.clone(), handler).await;
    cluster.set(slots(vec![(0, 16383, a.port, vec![b.port])]));
    let mut conn = ClusterConnection::builder(a.addr())
        .read_preference(ReadPreference::Replica)
        .connect()
        .await
        .unwrap();
    let get = || RawCommand::new("GET").arg("k");

    // The replica's socket dies, and reconnecting to it now fails.
    reject_setup.store(true, Ordering::SeqCst);
    b.kill_connections();
    assert!(conn.execute(get()).await.is_err(), "the dead socket");
    assert!(conn.execute(get()).await.is_err(), "the failed reconnect");
    let attempts = b.accepted.load(Ordering::SeqCst);
    assert_eq!(attempts, 2, "startup plus one reconnect");
    let error = conn.execute(get()).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("ReadPreference::Replica requires"),
        "{error}"
    );
    assert_eq!(
        b.accepted.load(Ordering::SeqCst),
        attempts,
        "no reconnect inside the backoff"
    );

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(conn.execute(get()).await.is_err());
    assert_eq!(b.accepted.load(Ordering::SeqCst), attempts + 1, "retried");

    reject_setup.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(600)).await;
    conn.execute(get())
        .await
        .expect("replica reads resume once it accepts connections again");
}

/// Under `ReadPreference::Master` replicas have no connection, and a refresh
/// only asked nodes with one plus the seed. In a one-shard cluster whose
/// master (also the seed) died, nobody was left to ask.
#[tokio::test]
async fn refresh_asks_listed_replicas_when_every_connected_node_is_gone() {
    let cluster = Cluster::new();
    let a = fake_node(cluster.clone(), default_handler()).await;
    let b = fake_node(cluster.clone(), default_handler()).await;
    cluster.set(slots(vec![(0, 16383, a.port, vec![b.port])]));
    let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
    let mut direct = ClusterConnection::connect(&a.addr()).await.unwrap();

    drop(a);
    cluster.set(slots(vec![(0, 16383, b.port, vec![])]));
    client
        .refresh_topology()
        .await
        .expect("multiplexed refresh through the listed replica");
    direct
        .refresh_topology()
        .await
        .expect("ClusterConnection refresh through the listed replica");
    client
        .execute(RawCommand::new("SET").arg("k").arg("v"))
        .await
        .unwrap();
    direct
        .execute(RawCommand::new("SET").arg("k").arg("v"))
        .await
        .unwrap();
    assert_eq!(b.received("SET"), 2);
}

/// redis-rs #2092: `reauthenticate_all` sent AUTH to every node in turn while
/// holding the routing write lock, so one node slow to answer stalled every
/// command to every node, and a hung one stalled them indefinitely.
#[tokio::test]
async fn reauthentication_does_not_block_commands_to_other_nodes() {
    let hang: fn() -> Action = || Action::Hang;
    let delay: fn() -> Action = || Action::Delayed(Duration::from_millis(1500), ok());
    for (scenario, slow) in [("hung", hang), ("slow", delay)] {
        let cluster = Cluster::new();
        let slow_port = Arc::new(AtomicU16::new(0));
        let handler: Handler = {
            let slow_port = slow_port.clone();
            Arc::new(move |request| {
                (request.is(b"AUTH") && request.port == slow_port.load(Ordering::SeqCst)).then(slow)
            })
        };
        let a = fake_node(cluster.clone(), handler.clone()).await;
        let b = fake_node(cluster.clone(), handler).await;
        slow_port.store(b.port, Ordering::SeqCst);
        cluster.set(slots(vec![
            (0, 8191, a.port, vec![]),
            (8192, 16383, b.port, vec![]),
        ]));
        let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
        let key_on_a = key_in(0, 8191);

        let reauth = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .reauthenticate_all(&redis_tower::credentials::Credentials::new(
                        "default", "rotated",
                    ))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        let started = tokio::time::Instant::now();
        tokio::time::timeout(
            Duration::from_secs(2),
            client.execute(RawCommand::new("GET").arg(key_on_a.as_str())),
        )
        .await
        .unwrap_or_else(|_| panic!("{scenario}: GET on a healthy node waited for AUTH"))
        .unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "{scenario}: GET on a healthy node took {:?}",
            started.elapsed()
        );
        let outcome = tokio::time::timeout(Duration::from_secs(10), reauth)
            .await
            .expect("reauthentication is bounded")
            .unwrap();
        match scenario {
            "hung" => assert!(
                matches!(outcome, Err(redis_tower_core::RedisError::CommandTimeout)),
                "{outcome:?}"
            ),
            _ => outcome.expect("a slow AUTH still succeeds"),
        }
    }
}

/// Redis reports an empty (single-node cluster) or NULL
/// (`cluster-preferred-endpoint-type unknown-endpoint`) endpoint for a node
/// that does not know its own address, meaning "the node you asked". Neither
/// client could connect to such a cluster.
#[tokio::test]
async fn clusters_reporting_unknown_endpoints_are_usable() {
    for (label, host) in [("empty", bulk("")), ("null", Frame::BulkString(None))] {
        let cluster = Cluster::new();
        let a = fake_node(cluster.clone(), default_handler()).await;
        cluster.set(Frame::Array(Some(vec![Frame::Array(Some(vec![
            Frame::Integer(0),
            Frame::Integer(16383),
            node_entry_with_host(host, a.port),
        ]))])));

        let client = MultiplexedClusterClient::connect(&a.addr())
            .await
            .unwrap_or_else(|error| panic!("{label}: multiplexed connect: {error}"));
        client
            .execute(RawCommand::new("SET").arg("k").arg("v"))
            .await
            .unwrap();
        client.refresh_topology().await.unwrap();
        let mut direct = ClusterConnection::builder(a.addr())
            .max_redirects(0)
            .connect()
            .await
            .unwrap_or_else(|error| panic!("{label}: ClusterConnection connect: {error}"));
        direct
            .execute(RawCommand::new("SET").arg("k").arg("v"))
            .await
            .unwrap();
        direct.refresh_topology().await.unwrap();
        assert_eq!(a.received("SET"), 2, "{label}");
        client.shutdown().await;
    }
}

/// `MOVED <slot> :<port>` (no host) is what such a node sends as a redirect;
/// the target is on the replying node's host.
#[tokio::test]
async fn host_less_redirects_resolve_to_the_replying_nodes_host() {
    let cluster = Cluster::new();
    let target_port = Arc::new(AtomicU16::new(0));
    let handler: Handler = {
        let target_port = target_port.clone();
        Arc::new(move |request| {
            let target = target_port.load(Ordering::SeqCst);
            (request.port != target && (request.is(b"GET") || request.is(b"SET"))).then(|| {
                let slot = redis_tower_cluster::slot_for_key(&request.args[1]);
                Action::Reply(err(&format!("MOVED {slot} :{target}")))
            })
        })
    };
    let a = fake_node(cluster.clone(), handler.clone()).await;
    let b = fake_node(cluster.clone(), handler).await;
    target_port.store(b.port, Ordering::SeqCst);
    cluster.set(slots(vec![(0, 16383, a.port, vec![])]));
    let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
    let mut direct = ClusterConnection::connect(&a.addr()).await.unwrap();

    client
        .execute(RawCommand::new("GET").arg("k"))
        .await
        .unwrap();
    direct
        .execute(RawCommand::new("GET").arg("k"))
        .await
        .unwrap();
    let mut pipeline = client.clone();
    redis_tower::PipelineExecutor::execute_pipeline(
        &mut pipeline,
        vec![redis_tower_protocol::helpers::array(vec![
            redis_tower_protocol::helpers::bulk("SET"),
            redis_tower_protocol::helpers::bulk("other"),
            redis_tower_protocol::helpers::bulk("v"),
        ])],
    )
    .await
    .unwrap();
    assert_eq!(b.received("GET"), 2);
    assert_eq!(
        b.received("SET"),
        1,
        "the pipelined entry followed the redirect"
    );
}

/// One unreachable master (still listed by CLUSTER SLOTS during a failover
/// window, or restarting) made every new client fail to connect, including
/// ones that only need healthy slots. Its slots are now connected lazily.
#[tokio::test]
async fn connect_tolerates_an_unreachable_master_and_connects_it_lazily() {
    let cluster = Cluster::new();
    let a = fake_node(cluster.clone(), default_handler()).await;
    let parked = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let late_port = parked.local_addr().unwrap().port();
    drop(parked);
    cluster.set(slots(vec![
        (0, 8191, a.port, vec![]),
        (8192, 16383, late_port, vec![]),
    ]));
    let healthy_key = key_in(0, 8191);
    let late_key = key_in(8192, 16383);
    let get = |key: &str| RawCommand::new("GET").arg(key);

    let client = MultiplexedClusterClient::connect(&a.addr())
        .await
        .expect("multiplexed connect with one unreachable master");
    let mut direct = ClusterConnection::builder(a.addr())
        .max_redirects(0)
        .connect()
        .await
        .expect("ClusterConnection connect with one unreachable master");
    client.execute(get(&healthy_key)).await.unwrap();
    direct.execute(get(&healthy_key)).await.unwrap();
    for error in [
        client.execute(get(&late_key)).await.unwrap_err(),
        direct.execute(get(&late_key)).await.unwrap_err(),
    ] {
        assert!(
            error.is_connection_error(),
            "the unreachable owner's slots fail as a connection error: {error:?}"
        );
    }
    assert_eq!(
        a.received("GET"),
        2,
        "no MOVED detour through the default node"
    );

    // The master comes up: its slots work without reconnecting the client.
    let late = fake_node_at(cluster.clone(), default_handler(), late_port).await;
    client.execute(get(&late_key)).await.unwrap();
    direct.execute(get(&late_key)).await.unwrap();
    assert_eq!(late.received("GET"), 2);
}

/// With no master reachable, connecting still fails.
#[tokio::test]
async fn connect_fails_when_no_master_is_reachable() {
    let cluster = Cluster::new();
    let seed = fake_node(cluster.clone(), default_handler()).await;
    let parked = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_port = parked.local_addr().unwrap().port();
    drop(parked);
    cluster.set(slots(vec![(0, 16383, dead_port, vec![])]));
    assert!(
        MultiplexedClusterClient::connect(&seed.addr())
            .await
            .is_err()
    );
    assert!(ClusterConnection::connect(&seed.addr()).await.is_err());
}

/// redis-rs #2474: node-scoped commands reached only the keyless default
/// master and returned a normal-looking reply -- one master's DBSIZE, one
/// master flushed, a script cached where EVALSHA for other slots cannot find
/// it. They now run on every node of their scope with combined replies.
#[tokio::test]
async fn node_scoped_commands_run_on_every_master() {
    let cluster = Cluster::new();
    let handler: Handler = Arc::new(|request| {
        if request.is(b"DBSIZE") {
            return Some(Action::Reply(Frame::Integer(i64::from(
                request.port % 7 + 1,
            ))));
        }
        if request.is(b"SCRIPT") && request.args[1].eq_ignore_ascii_case(b"EXISTS") {
            // Every node knows the first script; only even ports the second.
            let second = i64::from(request.port % 2 == 0);
            return Some(Action::Reply(Frame::Array(Some(vec![
                Frame::Integer(1),
                Frame::Integer(second),
            ]))));
        }
        if request.is(b"SCRIPT") {
            return Some(Action::Reply(bulk("sha")));
        }
        None
    });
    let a = fake_node(cluster.clone(), handler.clone()).await;
    let b = fake_node(cluster.clone(), handler.clone()).await;
    let r = fake_node(cluster.clone(), handler).await;
    cluster.set(slots(vec![
        (0, 8191, a.port, vec![r.port]),
        (8192, 16383, b.port, vec![]),
    ]));
    let expected_size = i64::from(a.port % 7 + 1) + i64::from(b.port % 7 + 1);
    let expected_second = i64::from(a.port % 2 == 0 && b.port % 2 == 0 && r.port % 2 == 0);

    let client = MultiplexedClusterClient::builder(a.addr())
        .read_preference(ReadPreference::PreferReplica)
        .connect()
        .await
        .unwrap();
    let mut direct = ClusterConnection::builder(a.addr())
        .read_preference(ReadPreference::PreferReplica)
        .connect()
        .await
        .unwrap();
    let script_exists = || RawCommand::new("SCRIPT").arg("EXISTS").arg("x").arg("y");
    let script_load = || RawCommand::new("SCRIPT").arg("LOAD").arg("return 1");
    let replies = [
        (
            client.execute(RawCommand::new("DBSIZE")).await.unwrap(),
            client.execute(script_exists()).await.unwrap(),
            client.execute(script_load()).await.unwrap(),
        ),
        (
            direct.execute(RawCommand::new("DBSIZE")).await.unwrap(),
            direct.execute(script_exists()).await.unwrap(),
            direct.execute(script_load()).await.unwrap(),
        ),
    ];
    for (size, exists, sha) in replies {
        assert_eq!(
            size,
            Frame::Integer(expected_size),
            "DBSIZE sums the masters"
        );
        assert_eq!(
            exists,
            Frame::Array(Some(vec![
                Frame::Integer(1),
                Frame::Integer(expected_second)
            ])),
            "SCRIPT EXISTS is true only where every node has the script"
        );
        assert_eq!(sha, bulk("sha"));
    }
    client.execute(RawCommand::new("FLUSHALL")).await.unwrap();
    direct.execute(RawCommand::new("FLUSHALL")).await.unwrap();

    for (node, master) in [(&a, true), (&b, true), (&r, false)] {
        let expected = if master { 2 } else { 0 };
        assert_eq!(node.received("DBSIZE"), expected, "{}", node.port);
        assert_eq!(node.received("FLUSHALL"), expected, "{}", node.port);
        assert_eq!(
            node.received("SCRIPT"),
            4,
            "{}: SCRIPT reaches connected replicas too",
            node.port
        );
    }
    client.shutdown().await;
}

/// A fan-out is not atomic: one failing master fails the command with the
/// server's error, after every master was attempted.
#[tokio::test]
async fn node_scoped_command_fails_when_any_master_fails() {
    let cluster = Cluster::new();
    let failing = Arc::new(AtomicU16::new(0));
    let handler: Handler = {
        let failing = failing.clone();
        Arc::new(move |request| {
            (request.is(b"FLUSHALL") && request.port == failing.load(Ordering::SeqCst))
                .then(|| Action::Reply(err("ERR flush refused")))
        })
    };
    let a = fake_node(cluster.clone(), handler.clone()).await;
    let b = fake_node(cluster.clone(), handler).await;
    failing.store(b.port, Ordering::SeqCst);
    cluster.set(slots(vec![
        (0, 8191, a.port, vec![]),
        (8192, 16383, b.port, vec![]),
    ]));
    let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
    let mut direct = ClusterConnection::connect(&a.addr()).await.unwrap();
    for error in [
        client
            .execute(RawCommand::new("FLUSHALL"))
            .await
            .unwrap_err(),
        direct
            .execute(RawCommand::new("FLUSHALL"))
            .await
            .unwrap_err(),
    ] {
        assert!(
            matches!(&error, redis_tower_core::RedisError::Redis(text) if text == "ERR flush refused"),
            "{error:?}"
        );
    }
    assert_eq!(a.received("FLUSHALL"), 2, "the healthy master still ran it");
}

/// `ClusterConnection` discarded the ASKING reply and returned whatever the
/// migrated command answered without the migration flag (redis-rs #2305).
#[tokio::test]
async fn rejected_asking_is_surfaced() {
    let cluster = Cluster::new();
    let target_port = Arc::new(AtomicU16::new(0));
    let handler: Handler = {
        let target_port = target_port.clone();
        Arc::new(move |request| {
            let target = target_port.load(Ordering::SeqCst);
            if request.port != target && request.is(b"GET") {
                let slot = redis_tower_cluster::slot_for_key(&request.args[1]);
                return Some(Action::Reply(err(&format!(
                    "ASK {slot} 127.0.0.1:{target}"
                ))));
            }
            if request.port == target && request.is(b"ASKING") {
                return Some(Action::Reply(err("ERR ASKING failed")));
            }
            if request.port == target && request.is(b"GET") {
                return Some(Action::Reply(bulk("unexpected-success")));
            }
            None
        })
    };
    let a = fake_node(cluster.clone(), handler.clone()).await;
    let b = fake_node(cluster.clone(), handler).await;
    target_port.store(b.port, Ordering::SeqCst);
    cluster.set(slots(vec![(0, 16383, a.port, vec![])]));
    let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
    let mut direct = ClusterConnection::connect(&a.addr()).await.unwrap();
    for error in [
        client
            .execute(RawCommand::new("GET").arg("key"))
            .await
            .unwrap_err(),
        direct
            .execute(RawCommand::new("GET").arg("key"))
            .await
            .unwrap_err(),
    ] {
        assert!(
            matches!(&error, redis_tower_core::RedisError::Redis(text) if text == "ERR ASKING failed"),
            "{error:?}"
        );
    }
}

/// Exhausting the redirect budget replaced the server's reply with "too many
/// redirects (N)", hiding whether the slot moved or the cluster was down.
/// Under `max_redirects(0)`, the first consumer's setting, that was every
/// redirect.
#[tokio::test]
async fn exhausted_redirect_budget_returns_the_servers_reply() {
    let cluster = Cluster::new();
    let handler: Handler = Arc::new(|request| {
        if request.is(b"GET") {
            let slot = redis_tower_cluster::slot_for_key(&request.args[1]);
            return Some(Action::Reply(err(&format!(
                "MOVED {slot} 127.0.0.1:{}",
                request.port
            ))));
        }
        request
            .is(b"SET")
            .then(|| Action::Reply(err("CLUSTERDOWN The cluster is down")))
    });
    let a = fake_node(cluster.clone(), handler).await;
    cluster.set(slots(vec![(0, 16383, a.port, vec![])]));
    let client = MultiplexedClusterClient::builder(a.addr())
        .max_redirects(0)
        .connect()
        .await
        .unwrap();
    let mut direct = ClusterConnection::builder(a.addr())
        .max_redirects(0)
        .connect()
        .await
        .unwrap();
    let moved = [
        client
            .execute(RawCommand::new("GET").arg("k"))
            .await
            .unwrap_err(),
        direct
            .execute(RawCommand::new("GET").arg("k"))
            .await
            .unwrap_err(),
    ];
    for error in moved {
        assert!(error.is_moved(), "{error:?}");
    }
    let down = [
        client
            .execute(RawCommand::new("SET").arg("k").arg("v"))
            .await
            .unwrap_err(),
        direct
            .execute(RawCommand::new("SET").arg("k").arg("v"))
            .await
            .unwrap_err(),
    ];
    for error in down {
        assert!(error.to_string().contains("CLUSTERDOWN"), "{error:?}");
    }
}

/// Redis Cluster has only database 0: `SELECT n` and a `/n` URL path were
/// passed through or silently ignored.
#[tokio::test]
async fn nonzero_databases_are_rejected() {
    let cluster = Cluster::new();
    let a = fake_node(cluster.clone(), default_handler()).await;
    cluster.set(slots(vec![(0, 16383, a.port, vec![])]));
    let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
    let mut direct = ClusterConnection::connect(&a.addr()).await.unwrap();
    client
        .execute(RawCommand::new("SELECT").arg("0"))
        .await
        .expect("SELECT 0 is a no-op");
    for error in [
        client
            .execute(RawCommand::new("SELECT").arg("1"))
            .await
            .unwrap_err(),
        direct
            .execute(RawCommand::new("select").arg("2"))
            .await
            .unwrap_err(),
    ] {
        assert!(error.to_string().contains("only database 0"), "{error}");
    }
    let mut pipeline = client.clone();
    assert!(
        redis_tower::PipelineExecutor::execute_pipeline(
            &mut pipeline,
            vec![redis_tower_protocol::helpers::array(vec![
                redis_tower_protocol::helpers::bulk("SELECT"),
                redis_tower_protocol::helpers::bulk("3"),
            ])],
        )
        .await
        .is_err(),
        "pipelines reject it before any I/O"
    );
    assert_eq!(a.received("SELECT"), 1, "only SELECT 0 reached the node");

    for url in [
        format!("redis://127.0.0.1:{}/2", a.port),
        format!("redis://127.0.0.1:{}/15", a.port),
    ] {
        assert!(
            matches!(
                MultiplexedClusterClient::connect_url(&url).await,
                Err(redis_tower_core::RedisError::InvalidUrl(_))
            ),
            "{url}"
        );
        assert!(
            matches!(
                ClusterConnection::connect_url(&url).await,
                Err(redis_tower_core::RedisError::InvalidUrl(_))
            ),
            "{url}"
        );
    }
    MultiplexedClusterClient::connect_url(&format!("redis://127.0.0.1:{}/0", a.port))
        .await
        .expect("/0 is database 0");
}

/// A pipeline whose slot has no known owner failed before any I/O, so no node
/// error scheduled the refresh that would learn the owner.
#[tokio::test]
async fn pipeline_planning_failure_schedules_a_refresh() {
    let cluster = Cluster::new();
    let handler: Handler = Arc::new(|request| {
        request.is(b"MGET").then(|| {
            Action::Reply(Frame::Array(Some(vec![
                Frame::BulkString(None);
                request.args.len() - 1
            ])))
        })
    });
    let a = fake_node(cluster.clone(), handler).await;
    // Slots 8192..16383 are unowned.
    cluster.set(slots(vec![(0, 8191, a.port, vec![])]));
    let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
    let before = cluster.refreshes();
    let orphan = key_in(8192, 16383);
    let mut pipeline = client.clone();
    let result = redis_tower::PipelineExecutor::execute_pipeline(
        &mut pipeline,
        vec![redis_tower_protocol::helpers::array(vec![
            redis_tower_protocol::helpers::bulk("GET"),
            redis_tower_protocol::helpers::bulk(orphan.as_str()),
        ])],
    )
    .await;
    assert!(result.is_err());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while cluster.refreshes() == before {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no refresh was scheduled"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Once the cluster reports an owner, the same pipeline works.
    cluster.set(slots(vec![(0, 16383, a.port, vec![])]));
    client.refresh_topology().await.unwrap();
    assert_eq!(
        client.mget_split([orphan.as_str()]).await.unwrap(),
        vec![None]
    );
}

/// Review finding: a replica whose reconnect fails more slowly than its
/// backoff (250 ms, then doubling) became eligible again while the other
/// replica was being tried, so re-routing alternated between two dead
/// replicas forever -- never falling back to the master under
/// `PreferReplica`, never returning under `Replica`, and holding
/// `ClusterClient`'s mutex throughout. Each command now tries a replica once.
#[tokio::test]
async fn cluster_connection_tries_each_failing_replica_once_per_command() {
    let cluster = Cluster::new();
    let slow_failure: Handler = Arc::new(|request| {
        request
            .is(b"READONLY")
            .then(|| Action::CloseAfter(Duration::from_millis(400)))
    });
    let a = fake_node(cluster.clone(), default_handler()).await;
    let r1 = fake_node(cluster.clone(), slow_failure.clone()).await;
    let r2 = fake_node(cluster.clone(), slow_failure).await;
    cluster.set(slots(vec![(0, 16383, a.port, vec![r1.port, r2.port])]));

    for preference in [ReadPreference::PreferReplica, ReadPreference::Replica] {
        let mut conn = ClusterConnection::builder(a.addr())
            .read_preference(preference)
            .connect()
            .await
            .unwrap();
        let attempts_before =
            r1.accepted.load(Ordering::SeqCst) + r2.accepted.load(Ordering::SeqCst);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            conn.execute(RawCommand::new("GET").arg("k")),
        )
        .await
        .unwrap_or_else(|_| panic!("{preference:?}: routing looped over failing replicas"));
        match preference {
            ReadPreference::PreferReplica => {
                result.expect("PreferReplica falls back to the master");
            }
            _ => assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("ReadPreference::Replica requires"),
                "strict replica reads fail once both replicas failed"
            ),
        }
        let attempts = r1.accepted.load(Ordering::SeqCst) + r2.accepted.load(Ordering::SeqCst)
            - attempts_before;
        assert_eq!(attempts, 2, "{preference:?}: each replica tried once");
    }
    assert_eq!(
        a.received("GET"),
        1,
        "only PreferReplica reached the master"
    );
}

/// Review finding: after the keyless default left the master set, the
/// refresh pointed it at the first listed master even when connecting to that
/// master had just failed, pinning PING/INFO to an unreachable node.
#[tokio::test]
async fn cluster_connection_refresh_moves_the_default_to_a_connected_master() {
    let cluster = Cluster::new();
    let a = fake_node(cluster.clone(), default_handler()).await;
    let c = fake_node(cluster.clone(), default_handler()).await;
    let parked = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unreachable = parked.local_addr().unwrap().port();
    drop(parked);
    cluster.set(slots(vec![
        (0, 8191, a.port, vec![]),
        (8192, 16383, c.port, vec![]),
    ]));
    let mut conn = ClusterConnection::connect(&a.addr()).await.unwrap();

    // a is replaced by an unreachable master listed first; c stays up.
    drop(a);
    cluster.set(slots(vec![
        (0, 8191, unreachable, vec![]),
        (8192, 16383, c.port, vec![]),
    ]));
    conn.refresh_topology().await.unwrap();
    assert_eq!(
        conn.execute(RawCommand::new("PING")).await.unwrap(),
        Frame::SimpleString(Bytes::from_static(b"PONG")),
        "keyless commands use the connected master"
    );
    assert_eq!(c.received("PING"), 1);
}

/// Review finding: node-scoped fan-out retried only READONLY, so a single
/// LOADING/TRYAGAIN/CLUSTERDOWN reply -- which ordinary routing retries within
/// the redirect budget -- failed DBSIZE or SCRIPT LOAD outright. Transient
/// nodes are now retried without re-sending to nodes that already answered.
#[tokio::test]
async fn node_scoped_fan_out_retries_transient_replies_per_node() {
    for transient in [
        "LOADING Redis is loading the dataset in memory",
        "TRYAGAIN Multiple keys request during rehashing of slot",
        "CLUSTERDOWN The cluster is down",
    ] {
        let cluster = Cluster::new();
        let flaky_port = Arc::new(AtomicU16::new(0));
        let remaining = Arc::new(std::sync::Mutex::new(0usize));
        let handler: Handler = {
            let flaky_port = flaky_port.clone();
            let remaining = remaining.clone();
            Arc::new(move |request| {
                if !request.is(b"DBSIZE") {
                    return None;
                }
                let mut left = remaining.lock().unwrap();
                if request.port == flaky_port.load(Ordering::SeqCst) && *left > 0 {
                    *left -= 1;
                    return Some(Action::Reply(err(transient)));
                }
                Some(Action::Reply(Frame::Integer(5)))
            })
        };
        let a = fake_node(cluster.clone(), handler.clone()).await;
        let b = fake_node(cluster.clone(), handler).await;
        flaky_port.store(b.port, Ordering::SeqCst);
        cluster.set(slots(vec![
            (0, 8191, a.port, vec![]),
            (8192, 16383, b.port, vec![]),
        ]));
        let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
        let mut direct = ClusterConnection::connect(&a.addr()).await.unwrap();

        *remaining.lock().unwrap() = 1;
        assert_eq!(
            client.execute(RawCommand::new("DBSIZE")).await.unwrap(),
            Frame::Integer(10),
            "multiplexed: {transient}"
        );
        *remaining.lock().unwrap() = 1;
        assert_eq!(
            direct.execute(RawCommand::new("DBSIZE")).await.unwrap(),
            Frame::Integer(10),
            "ClusterConnection: {transient}"
        );
        assert_eq!(
            a.received("DBSIZE"),
            2,
            "{transient}: the healthy node is not re-sent"
        );
        assert_eq!(b.received("DBSIZE"), 4, "{transient}: one retry per client");

        // A persistent transient error still ends within the budget.
        *remaining.lock().unwrap() = usize::MAX;
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            client.execute(RawCommand::new("DBSIZE")),
        )
        .await
        .expect("bounded")
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains(transient.split(' ').next().unwrap()),
            "{error}"
        );
        client.shutdown().await;
    }
}
