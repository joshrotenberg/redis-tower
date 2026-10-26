//! Cluster Pub/Sub recovery from migrations, failovers, and node loss against
//! scripted fake cluster nodes (see `fake_cluster`).

mod fake_cluster;

use fake_cluster::*;
use redis_tower::reconnect::ReconnectConfig;
use redis_tower_cluster::{MultiplexedClusterClient, NodeAddr, slot_for_key};
use redis_tower_core::{Frame, RedisError};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::time::Duration;

const CHANNEL: &str = "{s}orders";

/// SSUBSCRIBE on any node but the current owner answers MOVED to it.
fn shard_owner_handler(owner: Arc<AtomicU16>) -> Handler {
    Arc::new(move |request| {
        let owner = owner.load(Ordering::SeqCst);
        (request.is(b"SSUBSCRIBE") && request.port != owner).then(|| {
            let slot = slot_for_key(&request.args[1]);
            Action::Reply(err(&format!("MOVED {slot} 127.0.0.1:{owner}")))
        })
    })
}

/// Once `node` has received `count` of `command`, push `message` from it.
fn publish_after_subscribe(
    node: &FakeNode,
    command: &'static str,
    count: usize,
    message: Frame,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    let commands = node.commands.clone();
    let pusher = node.pusher();
    async move {
        loop {
            let seen = commands
                .lock()
                .unwrap()
                .iter()
                .filter(|seen| seen.as_str() == command)
                .count();
            if seen >= count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Let the acknowledgement land before the message.
        tokio::time::sleep(Duration::from_millis(50)).await;
        pusher.push(message);
    }
}

fn smessage(payload: &str) -> Frame {
    Frame::Array(Some(vec![bulk("smessage"), bulk(CHANNEL), bulk(payload)]))
}

async fn expect_payload(
    session: &mut redis_tower_cluster::ShardedClusterPubSubConnection,
    payload: &str,
    scenario: &str,
) {
    let message = tokio::time::timeout(Duration::from_secs(10), session.next_message())
        .await
        .unwrap_or_else(|_| panic!("{scenario}: no message after recovery"))
        .unwrap_or_else(|error| panic!("{scenario}: session failed: {error}"));
    assert_eq!(message.payload.as_ref(), payload.as_bytes(), "{scenario}");
}

/// Redis drops a shard subscription with an unrequested `sunsubscribe` when
/// the slot migrates away, leaving the socket open. The session used to
/// discard it as a confirmation and wait on the old owner forever.
#[tokio::test]
async fn sharded_session_follows_a_server_initiated_unsubscribe() {
    // `refreshed` = the migration is visible in CLUSTER SLOTS by the time the
    // session refreshes; otherwise the old owner's MOVED must be followed.
    for refreshed in [true, false] {
        let cluster = Cluster::new();
        let owner = Arc::new(AtomicU16::new(0));
        let a = fake_node(cluster.clone(), shard_owner_handler(owner.clone())).await;
        let b = fake_node(cluster.clone(), shard_owner_handler(owner.clone())).await;
        owner.store(a.port, Ordering::SeqCst);
        cluster.set(slots(vec![(0, 16383, a.port, vec![])]));
        let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
        let mut session = client.sharded_pubsub(&[CHANNEL]).await.unwrap();
        assert_eq!(session.current_node().port, a.port);

        // Migrate the slot to b; a drops the subscription.
        owner.store(b.port, Ordering::SeqCst);
        if refreshed {
            cluster.set(slots(vec![(0, 16383, b.port, vec![])]));
        }
        a.push(sunsubscribe_push(CHANNEL));
        tokio::spawn(publish_after_subscribe(
            &b,
            "SSUBSCRIBE",
            1,
            smessage("after"),
        ));

        expect_payload(&mut session, "after", "slot migration").await;
        assert_eq!(session.current_node().port, b.port);
        assert!(
            session.subscriptions().shard_channels.contains(CHANNEL),
            "the dropped channel is tracked again"
        );
    }
}

/// The owner dies and the failover takes longer than the node workers'
/// reconnect budget (~1.5 s). The session used to keep dialing the dead
/// address from its cached topology and give up after that budget.
#[tokio::test]
async fn sharded_session_survives_a_slow_hard_failover() {
    let cluster = Cluster::new();
    let owner = Arc::new(AtomicU16::new(0));
    let a = fake_node(cluster.clone(), shard_owner_handler(owner.clone())).await;
    let b = fake_node(cluster.clone(), shard_owner_handler(owner.clone())).await;
    owner.store(a.port, Ordering::SeqCst);
    cluster.set(slots(vec![(0, 16383, a.port, vec![b.port])]));
    let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
    let mut session = client.sharded_pubsub(&[CHANNEL]).await.unwrap();

    drop(a);
    let failover = {
        let cluster = cluster.clone();
        let owner = owner.clone();
        let b_port = b.port;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(2500)).await;
            owner.store(b_port, Ordering::SeqCst);
            cluster.set(slots(vec![(0, 16383, b_port, vec![])]));
        })
    };
    tokio::spawn(publish_after_subscribe(
        &b,
        "SSUBSCRIBE",
        1,
        smessage("promoted"),
    ));

    expect_payload(&mut session, "promoted", "hard failover").await;
    assert_eq!(session.current_node().port, b.port);
    failover.await.unwrap();
}

/// A bounded `pubsub_reconnect_config` still ends the session.
#[tokio::test]
async fn sharded_session_honors_a_bounded_reconnect_budget() {
    let cluster = Cluster::new();
    let owner = Arc::new(AtomicU16::new(0));
    let a = fake_node(cluster.clone(), shard_owner_handler(owner.clone())).await;
    let b = fake_node(cluster.clone(), shard_owner_handler(owner.clone())).await;
    owner.store(a.port, Ordering::SeqCst);
    cluster.set(slots(vec![
        (0, 8191, a.port, vec![]),
        (8192, 16383, b.port, vec![]),
    ]));
    let client = MultiplexedClusterClient::builder(b.addr())
        .pubsub_reconnect_config(
            ReconnectConfig::default()
                .max_retries(1)
                .base_delay(Duration::from_millis(10)),
        )
        .connect()
        .await
        .unwrap();
    let channel = key_in(0, 8191);
    let mut session = client.sharded_pubsub(&[channel.as_str()]).await.unwrap();

    drop(a);
    let result = tokio::time::timeout(Duration::from_secs(5), session.next_message())
        .await
        .expect("a bounded budget must end the session");
    assert!(
        matches!(result, Err(RedisError::ReconnectFailed { attempts: 2, .. })),
        "{result:?}"
    );
}

/// `changes.changed()` marks a topology revision seen as soon as it returns.
/// Cancelling `next_message` while it was following that change lost it, and
/// the session stayed on the old owner.
#[tokio::test]
async fn sharded_session_resumes_an_owner_change_after_cancellation() {
    let cluster = Cluster::new();
    let owner = Arc::new(AtomicU16::new(0));
    let stall_first = Arc::new(AtomicBool::new(true));
    let handler: Handler = {
        let base = shard_owner_handler(owner.clone());
        let owner = owner.clone();
        let stall_first = stall_first.clone();
        Arc::new(move |request| {
            if request.is(b"SSUBSCRIBE")
                && request.port == owner.load(Ordering::SeqCst)
                && stall_first.swap(false, Ordering::SeqCst)
            {
                // Long enough for the caller's timeout to cancel the wait.
                return Some(Action::Hang);
            }
            base(request)
        })
    };
    let a = fake_node(cluster.clone(), handler.clone()).await;
    let b = fake_node(cluster.clone(), handler).await;
    owner.store(a.port, Ordering::SeqCst);
    stall_first.store(false, Ordering::SeqCst);
    cluster.set(slots(vec![(0, 16383, a.port, vec![])]));
    let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
    let mut session = client.sharded_pubsub(&[CHANNEL]).await.unwrap();

    owner.store(b.port, Ordering::SeqCst);
    stall_first.store(true, Ordering::SeqCst);
    cluster.set(slots(vec![(0, 16383, b.port, vec![])]));
    client.refresh_topology().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(200), session.next_message())
            .await
            .is_err(),
        "the first resubscribe on b stalls and the caller gives up"
    );
    assert_eq!(session.current_node().port, a.port);

    tokio::spawn(publish_after_subscribe(
        &b,
        "SSUBSCRIBE",
        2,
        smessage("resumed"),
    ));
    expect_payload(&mut session, "resumed", "cancelled owner change").await;
    assert_eq!(session.current_node().port, b.port);
}

fn message(channel: &str, payload: &str) -> Frame {
    Frame::Array(Some(vec![bulk("message"), bulk(channel), bulk(payload)]))
}

/// A pinned regular session reconnects to its node across a restart longer
/// than the node workers' reconnect budget.
#[tokio::test]
async fn regular_session_survives_a_node_restart() {
    let cluster = Cluster::new();
    let a = fake_node(cluster.clone(), default_handler()).await;
    let b = fake_node(cluster.clone(), default_handler()).await;
    let a_port = a.port;
    cluster.set(slots(vec![
        (0, 8191, a_port, vec![]),
        (8192, 16383, b.port, vec![]),
    ]));
    let client = MultiplexedClusterClient::connect(&b.addr()).await.unwrap();
    let mut session = client
        .pubsub_on(NodeAddr::new("127.0.0.1", a_port))
        .await
        .unwrap();
    session.subscribe(&["news"]).await.unwrap();

    drop(a);
    let restarted = tokio::spawn({
        let cluster = cluster.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(2500)).await;
            let a = fake_node_at(cluster, default_handler(), a_port).await;
            publish_after_subscribe(&a, "SUBSCRIBE", 1, message("news", "back")).await;
            a
        }
    });
    let received = tokio::time::timeout(Duration::from_secs(10), session.next_message())
        .await
        .expect("regular session never recovered")
        .unwrap();
    assert_eq!(received.payload.as_ref(), b"back");
    let _a = restarted.await.unwrap();
}

/// Once the pinned node leaves the topology the session ends with a clear
/// error instead of retrying forever.
#[tokio::test]
async fn regular_session_ends_when_its_node_leaves_the_topology() {
    let cluster = Cluster::new();
    let a = fake_node(cluster.clone(), default_handler()).await;
    let b = fake_node(cluster.clone(), default_handler()).await;
    cluster.set(slots(vec![
        (0, 8191, a.port, vec![]),
        (8192, 16383, b.port, vec![]),
    ]));
    let client = MultiplexedClusterClient::connect(&b.addr()).await.unwrap();
    let mut session = client
        .pubsub_on(NodeAddr::new("127.0.0.1", a.port))
        .await
        .unwrap();
    session.subscribe(&["news"]).await.unwrap();

    cluster.set(slots(vec![(0, 16383, b.port, vec![])]));
    drop(a);
    let error = tokio::time::timeout(Duration::from_secs(5), session.next_message())
        .await
        .expect("the session must end")
        .unwrap_err();
    assert!(
        error.to_string().contains("left the cluster topology"),
        "{error}"
    );
}

/// The socket breaks and the cached topology still names the old owner, which
/// answers the replayed SSUBSCRIBE with MOVED. The reconnect must commit the
/// redirect and continue on the new owner.
#[tokio::test]
async fn sharded_reconnect_follows_a_replay_time_moved() {
    let cluster = Cluster::new();
    let owner = Arc::new(AtomicU16::new(0));
    let a = fake_node(cluster.clone(), shard_owner_handler(owner.clone())).await;
    let b = fake_node(cluster.clone(), shard_owner_handler(owner.clone())).await;
    owner.store(a.port, Ordering::SeqCst);
    // CLUSTER SLOTS keeps naming a, so only the MOVED reveals b.
    cluster.set(slots(vec![(0, 16383, a.port, vec![])]));
    let client = MultiplexedClusterClient::connect(&a.addr()).await.unwrap();
    let mut session = client.sharded_pubsub(&[CHANNEL]).await.unwrap();

    owner.store(b.port, Ordering::SeqCst);
    a.kill_connections();
    tokio::spawn(publish_after_subscribe(
        &b,
        "SSUBSCRIBE",
        1,
        smessage("moved"),
    ));

    expect_payload(&mut session, "moved", "replay-time MOVED").await;
    assert_eq!(session.current_node().port, b.port);
    assert_eq!(
        client
            .topology()
            .await
            .master_for_slot(slot_for_key(CHANNEL.as_bytes()))
            .map(|node| node.port),
        Some(b.port),
        "the redirect is committed for command routing too"
    );
}
