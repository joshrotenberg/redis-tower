//! Scripted fake cluster nodes for failure-mode tests.
//!
//! Every node answers `CLUSTER SLOTS` from one shared [`Cluster`] state, so a
//! test can flip ownership (a failover, a migration) between commands without
//! running a real cluster. A per-test [`Handler`] overrides individual replies;
//! anything it declines gets the default handling in [`fake_node_at`].

#![allow(dead_code)]

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use redis_tower_cluster::slot_for_key;
use redis_tower_core::{Frame, RedisStream, RespCodec};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::codec::Framed;

/// How a node answers one command.
pub enum Action {
    Reply(Frame),
    /// Several frames for one command (a reply followed by pushes).
    Replies(Vec<Frame>),
    Delayed(Duration, Frame),
    /// Never answer; keep the socket open until the node is dropped.
    Hang,
    /// Close the socket without answering.
    Close,
}

/// One command as seen by a fake node.
pub struct Request<'a> {
    /// Port of the node that received it.
    pub port: u16,
    /// Whether this connection sent `READONLY` earlier.
    pub readonly: bool,
    pub args: &'a [Vec<u8>],
}

impl Request<'_> {
    pub fn is(&self, name: &[u8]) -> bool {
        is_cmd(self.args, name)
    }
}

/// Reply override. `None` falls back to the default handling.
pub type Handler = Arc<dyn Fn(&Request<'_>) -> Option<Action> + Send + Sync>;

/// Shared `CLUSTER SLOTS` state for a group of fake nodes.
pub struct Cluster {
    slots: Mutex<Frame>,
    pub slots_calls: AtomicUsize,
}

impl Cluster {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            slots: Mutex::new(Frame::Null),
            slots_calls: AtomicUsize::new(0),
        })
    }

    pub fn set(&self, frame: Frame) {
        *self.slots.lock().unwrap() = frame;
    }

    pub fn refreshes(&self) -> usize {
        self.slots_calls.load(Ordering::SeqCst)
    }
}

/// One fake node. Dropping it stops accepting connections; existing sockets
/// are closed by [`kill_connections`](Self::kill_connections).
pub struct FakeNode {
    pub port: u16,
    pub accepted: Arc<AtomicUsize>,
    /// Every command received, as upper-cased `argv[0]`.
    pub commands: Arc<Mutex<Vec<String>>>,
    kill: tokio::sync::watch::Sender<u64>,
    pushes: tokio::sync::broadcast::Sender<Frame>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeNode {
    /// Close every currently open client connection (FIN).
    pub fn kill_connections(&self) {
        self.kill.send_modify(|generation| *generation += 1);
    }

    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// Send `frame` unrequested to every open connection that has issued a
    /// subscribe-family command (a Pub/Sub socket, never a command worker).
    pub fn push(&self, frame: Frame) {
        let _ = self.pushes.send(frame);
    }

    /// A handle that pushes from this node and outlives borrows of it.
    pub fn pusher(&self) -> Pusher {
        Pusher(self.pushes.clone())
    }

    /// Publish a shard message to this node's subscribers.
    pub fn smessage(&self, channel: &str, payload: &str) {
        self.push(Frame::Array(Some(vec![
            bulk("smessage"),
            bulk(channel),
            bulk(payload),
        ])));
    }

    /// How many times this node received `command` (upper-case).
    pub fn received(&self, command: &str) -> usize {
        self.commands
            .lock()
            .unwrap()
            .iter()
            .filter(|seen| seen.as_str() == command)
            .count()
    }
}

impl Drop for FakeNode {
    fn drop(&mut self) {
        self.kill_connections();
        self.task.abort();
    }
}

/// See [`FakeNode::push`].
#[derive(Clone)]
pub struct Pusher(tokio::sync::broadcast::Sender<Frame>);

impl Pusher {
    pub fn push(&self, frame: Frame) {
        let _ = self.0.send(frame);
    }
}

pub fn err(text: &str) -> Frame {
    Frame::Error(Bytes::copy_from_slice(text.as_bytes()))
}

pub fn ok() -> Frame {
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

pub fn bulk(text: &str) -> Frame {
    Frame::BulkString(Some(Bytes::copy_from_slice(text.as_bytes())))
}

pub fn default_handler() -> Handler {
    Arc::new(|_| None)
}

pub fn is_cmd(args: &[Vec<u8>], name: &[u8]) -> bool {
    args.first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case(name))
}

pub async fn fake_node(cluster: Arc<Cluster>, handler: Handler) -> FakeNode {
    fake_node_at(cluster, handler, 0).await
}

/// Bind a fake node on `port` (0 = any free port).
///
/// Default replies: `HELLO` is rejected (RESP2), `CLUSTER SLOTS` comes from
/// the shared state, `PING` is `PONG`, `GET` is nil, `DBSIZE` is 0, and
/// everything else is `OK`.
pub async fn fake_node_at(cluster: Arc<Cluster>, handler: Handler, port: u16) -> FakeNode {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let commands = Arc::new(Mutex::new(Vec::new()));
    let (kill, _) = tokio::sync::watch::channel(0u64);
    let kill_tx = kill.clone();
    let (pushes, _) = tokio::sync::broadcast::channel(64);
    let pushes_tx = pushes.clone();
    let accepted_in_task = accepted.clone();
    let commands_in_task = commands.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            accepted_in_task.fetch_add(1, Ordering::SeqCst);
            let cluster = cluster.clone();
            let handler = handler.clone();
            let commands = commands_in_task.clone();
            let mut kill_rx = kill_tx.subscribe();
            kill_rx.borrow_and_update();
            let mut push_rx = pushes_tx.subscribe();
            tokio::spawn(async move {
                let mut framed = Framed::new(RedisStream::Tcp(stream), RespCodec::new());
                let mut readonly = false;
                let mut subscriber = false;
                loop {
                    let frame = tokio::select! {
                        _ = kill_rx.changed() => return,
                        push = push_rx.recv() => {
                            if let Ok(push) = push
                                && subscriber
                                && framed.send(push).await.is_err()
                            {
                                return;
                            }
                            continue;
                        }
                        frame = framed.next() => frame,
                    };
                    let Some(Ok(Frame::Array(Some(parts)))) = frame else {
                        return;
                    };
                    let args: Vec<Vec<u8>> = parts
                        .into_iter()
                        .map(|part| match part {
                            Frame::BulkString(Some(bytes)) => bytes.to_vec(),
                            _ => Vec::new(),
                        })
                        .collect();
                    let upper = args[0].to_ascii_uppercase();
                    commands
                        .lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&upper).into_owned());
                    let request = Request {
                        port,
                        readonly,
                        args: &args,
                    };
                    let action = handler(&request).unwrap_or_else(|| {
                        Action::Reply(match upper.as_slice() {
                            b"HELLO" => err("ERR unknown command 'HELLO'"),
                            b"CLUSTER"
                                if args
                                    .get(1)
                                    .is_some_and(|arg| arg.eq_ignore_ascii_case(b"SLOTS")) =>
                            {
                                cluster.slots_calls.fetch_add(1, Ordering::SeqCst);
                                cluster.slots.lock().unwrap().clone()
                            }
                            b"PING" => Frame::SimpleString(Bytes::from_static(b"PONG")),
                            b"GET" => Frame::BulkString(None),
                            b"DBSIZE" => Frame::Integer(0),
                            b"SUBSCRIBE" | b"PSUBSCRIBE" | b"SSUBSCRIBE" | b"SUNSUBSCRIBE" => {
                                return Action::Replies(subscribe_acks(&args));
                            }
                            _ => ok(),
                        })
                    });
                    match upper.as_slice() {
                        b"READONLY" => readonly = true,
                        b"READWRITE" => readonly = false,
                        b"SUBSCRIBE" | b"PSUBSCRIBE" | b"SSUBSCRIBE" => subscriber = true,
                        _ => {}
                    }
                    let replies = match action {
                        Action::Reply(reply) => vec![reply],
                        Action::Replies(replies) => replies,
                        Action::Delayed(delay, reply) => {
                            tokio::time::sleep(delay).await;
                            vec![reply]
                        }
                        Action::Hang => {
                            let _ = kill_rx.changed().await;
                            return;
                        }
                        Action::Close => return,
                    };
                    for reply in replies {
                        if framed.send(reply).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    });
    FakeNode {
        port,
        accepted,
        commands,
        kill,
        pushes,
        task,
    }
}

/// One `<kind> <name> <count>` acknowledgement per named channel.
pub fn subscribe_acks(args: &[Vec<u8>]) -> Vec<Frame> {
    let kind = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
    args[1..]
        .iter()
        .enumerate()
        .map(|(index, name)| {
            Frame::Array(Some(vec![
                bulk(&kind),
                Frame::BulkString(Some(Bytes::copy_from_slice(name))),
                Frame::Integer(index as i64 + 1),
            ]))
        })
        .collect()
}

/// The server dropping a shard channel on its own.
pub fn sunsubscribe_push(channel: &str) -> Frame {
    Frame::Array(Some(vec![
        bulk("sunsubscribe"),
        bulk(channel),
        Frame::Integer(0),
    ]))
}

pub fn node_entry(port: u16) -> Frame {
    node_entry_with_host(bulk("127.0.0.1"), port)
}

pub fn node_entry_with_host(host: Frame, port: u16) -> Frame {
    Frame::Array(Some(vec![
        host,
        Frame::Integer(i64::from(port)),
        Frame::BulkString(Some(Bytes::from(format!("id{port}")))),
    ]))
}

/// `[(start, end, master_port, [replica ports])]` as a CLUSTER SLOTS reply.
pub fn slots(ranges: Vec<(i64, i64, u16, Vec<u16>)>) -> Frame {
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

/// A key whose slot lies in `[lo, hi]`.
pub fn key_in(lo: u16, hi: u16) -> String {
    (0..)
        .map(|i| format!("k{i}"))
        .find(|key| (lo..=hi).contains(&slot_for_key(key.as_bytes())))
        .unwrap()
}

pub const READONLY_ERR: &str = "READONLY You can't write against a read only replica.";
