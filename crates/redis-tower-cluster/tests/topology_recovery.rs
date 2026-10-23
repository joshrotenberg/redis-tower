//! Topology-refresh recovery against scripted fake cluster nodes.
//!
//! Each fake node answers CLUSTER SLOTS from shared state, so a test can flip
//! the topology between refreshes without running a real cluster.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use redis_tower::commands::RawCommand;
use redis_tower_cluster::{MultiplexedClusterClient, slot_for_key};
use redis_tower_core::{Frame, RedisStream, RespCodec};
use std::sync::{Arc, Mutex};
use tokio_util::codec::Framed;

type Shared = Arc<Mutex<Frame>>;

fn node_entry(port: u16) -> Frame {
    Frame::Array(Some(vec![
        Frame::BulkString(Some(Bytes::from_static(b"127.0.0.1"))),
        Frame::Integer(i64::from(port)),
        Frame::BulkString(Some(Bytes::from(format!("id{port}")))),
    ]))
}

/// `[(start, end, master_port, [replica ports])]` as a CLUSTER SLOTS reply.
fn slots(ranges: Vec<(i64, i64, u16, Vec<u16>)>) -> Frame {
    Frame::Array(Some(
        ranges
            .into_iter()
            .map(|(start, end, master, replicas)| {
                let mut items = vec![
                    Frame::Integer(start),
                    Frame::Integer(end),
                    node_entry(master),
                ];
                items.extend(replicas.into_iter().map(node_entry));
                Frame::Array(Some(items))
            })
            .collect(),
    ))
}

fn empty_slots() -> Frame {
    Frame::Array(Some(Vec::new()))
}

struct FakeNode {
    port: u16,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FakeNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A minimal cluster node: rejects HELLO, answers CLUSTER SLOTS from shared
/// state, PING with PONG, GET with nil, and everything else with OK.
async fn fake_node(cluster_slots: Shared) -> FakeNode {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let cluster_slots = cluster_slots.clone();
            tokio::spawn(async move {
                let mut framed = Framed::new(RedisStream::Tcp(stream), RespCodec::new());
                while let Some(Ok(frame)) = framed.next().await {
                    let Frame::Array(Some(parts)) = frame else {
                        return;
                    };
                    let args: Vec<Vec<u8>> = parts
                        .into_iter()
                        .map(|part| match part {
                            Frame::BulkString(Some(bytes)) => bytes.to_ascii_uppercase(),
                            _ => Vec::new(),
                        })
                        .collect();
                    let reply = match args[0].as_slice() {
                        b"HELLO" => Frame::Error(Bytes::from_static(b"ERR unknown command")),
                        b"CLUSTER" if args.get(1).map(Vec::as_slice) == Some(b"SLOTS") => {
                            cluster_slots.lock().unwrap().clone()
                        }
                        b"PING" => Frame::SimpleString(Bytes::from_static(b"PONG")),
                        b"GET" => Frame::BulkString(None),
                        _ => Frame::SimpleString(Bytes::from_static(b"OK")),
                    };
                    if framed.send(reply).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    FakeNode { port, task }
}

/// A key whose slot lies in `[lo, hi]`.
fn key_in(lo: u16, hi: u16) -> String {
    (0..)
        .map(|i| format!("k{i}"))
        .find(|key| (lo..=hi).contains(&slot_for_key(key.as_bytes())))
        .unwrap()
}

/// A failover that demotes the first-listed master (the initial keyless
/// default) must move keyless traffic to a surviving master.
#[tokio::test]
async fn keyless_commands_follow_the_default_master_through_failover() {
    let state: Shared = Arc::new(Mutex::new(Frame::Null));
    let a = fake_node(state.clone()).await;
    let b = fake_node(state.clone()).await;
    *state.lock().unwrap() = slots(vec![
        (0, 8191, a.port, vec![]),
        (8192, 16383, b.port, vec![]),
    ]);
    let client = MultiplexedClusterClient::builder(format!("127.0.0.1:{}", a.port))
        .connect()
        .await
        .unwrap();
    client.execute(RawCommand::new("PING")).await.unwrap();

    // B now owns every slot and A is its replica.
    *state.lock().unwrap() = slots(vec![(0, 16383, b.port, vec![a.port])]);
    client.refresh_topology().await.unwrap();

    client
        .execute(RawCommand::new("GET").arg(key_in(0, 8191).as_str()))
        .await
        .expect("keyed command routes to the new owner");
    let pong = client.execute(RawCommand::new("PING")).await;
    assert_eq!(
        pong.expect("keyless PING after the default master was demoted"),
        Frame::SimpleString(Bytes::from_static(b"PONG"))
    );
}

/// An empty CLUSTER SLOTS reply (a node restarted without nodes.conf, or mid
/// CLUSTER RESET) must not be committed: committing it prunes every service.
#[tokio::test]
async fn empty_cluster_slots_refresh_keeps_the_existing_topology() {
    let state: Shared = Arc::new(Mutex::new(Frame::Null));
    let a = fake_node(state.clone()).await;
    let b = fake_node(state.clone()).await;
    let healthy = slots(vec![
        (0, 8191, a.port, vec![]),
        (8192, 16383, b.port, vec![]),
    ]);
    *state.lock().unwrap() = healthy.clone();
    let client = MultiplexedClusterClient::builder(format!("127.0.0.1:{}", a.port))
        .connect()
        .await
        .unwrap();

    *state.lock().unwrap() = empty_slots();
    assert!(
        client.refresh_topology().await.is_err(),
        "a refresh in which every node reports no slot owners is a failed refresh"
    );
    assert_eq!(client.topology().await.master_addrs().len(), 2);
    client
        .execute(RawCommand::new("GET").arg("k"))
        .await
        .expect("commands keep routing on the last good topology");

    *state.lock().unwrap() = healthy;
    client.refresh_topology().await.unwrap();
    client
        .execute(RawCommand::new("GET").arg("k"))
        .await
        .unwrap();
}

/// One node reporting an empty topology must not brick the client, whichever
/// node a refresh happens to ask first.
#[tokio::test]
async fn one_node_with_empty_cluster_slots_cannot_brick_the_client() {
    for _ in 0..20 {
        let healthy_state: Shared = Arc::new(Mutex::new(Frame::Null));
        let empty_state: Shared = Arc::new(Mutex::new(Frame::Null));
        let a = fake_node(empty_state.clone()).await;
        let b = fake_node(healthy_state.clone()).await;
        let healthy = slots(vec![
            (0, 8191, a.port, vec![]),
            (8192, 16383, b.port, vec![]),
        ]);
        *healthy_state.lock().unwrap() = healthy.clone();
        *empty_state.lock().unwrap() = healthy;
        let client = MultiplexedClusterClient::builder(format!("127.0.0.1:{}", b.port))
            .connect()
            .await
            .unwrap();

        // A restarts as a fresh node.
        *empty_state.lock().unwrap() = empty_slots();
        let _ = client.refresh_topology().await;
        let _ = client.refresh_topology().await;
        client
            .execute(RawCommand::new("GET").arg(key_in(8192, 16383).as_str()))
            .await
            .expect("a key owned by the healthy node keeps routing");
    }
}
