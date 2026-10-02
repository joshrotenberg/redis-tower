//! Public Cluster paths must set up new sockets before any ordinary traffic.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use redis_tower_cluster::{ClusterConnection, MultiplexedClusterClient, slot_for_key};
use redis_tower_commands::Get;
use redis_tower_core::{Frame, ProtocolVersion, RedisError, RedisStream, RespCodec};
use redis_tower_protocol::helpers::{array, bulk};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio_util::codec::Framed;

struct Node {
    addr: String,
    setups: Arc<AtomicUsize>,
    traffic: Arc<AtomicUsize>,
    reject: Arc<AtomicBool>,
    redirect: Arc<Mutex<Option<(&'static str, String)>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Node {
    async fn start(owner_port: Arc<Mutex<u16>>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let setups = Arc::new(AtomicUsize::new(0));
        let traffic = Arc::new(AtomicUsize::new(0));
        let reject = Arc::new(AtomicBool::new(false));
        let redirect = Arc::new(Mutex::new(None::<(&'static str, String)>));
        let (setups_task, traffic_task, reject_task, redirect_task) = (
            setups.clone(),
            traffic.clone(),
            reject.clone(),
            redirect.clone(),
        );
        let task = tokio::spawn(async move {
            let mut sessions = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let (setups, traffic, reject, redirect, owner_port) = (
                    setups_task.clone(),
                    traffic_task.clone(),
                    reject_task.clone(),
                    redirect_task.clone(),
                    owner_port.clone(),
                );
                sessions.spawn(async move {
                    let mut server = Framed::new(RedisStream::Tcp(stream), RespCodec::new());
                    let mut configured = false;
                    while let Some(frame) = server.next().await {
                        let Frame::Array(Some(parts)) = frame.unwrap() else {
                            panic!("expected command");
                        };
                        let args: Vec<Bytes> = parts
                            .into_iter()
                            .map(|f| match f {
                                Frame::BulkString(Some(b)) => b,
                                _ => panic!("expected argument"),
                            })
                            .collect();
                        let response = match args[0].as_ref() {
                            b"CLIENT" if args[1].as_ref() == b"SETINFO" => {
                                Frame::SimpleString("OK".into())
                            }
                            b"HELLO" => Frame::SimpleString("OK".into()),
                            b"CLIENT" if args[1].as_ref() == b"SETNAME" => {
                                assert!(!configured, "policy ran twice on one physical socket");
                                assert_eq!(args[2].as_ref(), b"cluster-736");
                                if reject.load(Ordering::SeqCst) {
                                    Frame::Error("ERR private-setup-value".into())
                                } else {
                                    configured = true;
                                    setups.fetch_add(1, Ordering::SeqCst);
                                    Frame::SimpleString("OK".into())
                                }
                            }
                            b"CLUSTER" => {
                                assert!(configured, "topology query ran before setup");
                                array(vec![array(vec![
                                    Frame::Integer(0),
                                    Frame::Integer(16383),
                                    array(vec![
                                        bulk("127.0.0.1"),
                                        Frame::Integer(i64::from(*owner_port.lock().unwrap())),
                                        bulk("node-id"),
                                    ]),
                                ])])
                            }
                            b"GET" => {
                                assert!(configured, "application traffic ran before setup");
                                traffic.fetch_add(1, Ordering::SeqCst);
                                match redirect.lock().unwrap().take() {
                                    Some((kind, target)) => Frame::Error(
                                        format!("{kind} {} {target}", slot_for_key(&args[1]))
                                            .into(),
                                    ),
                                    None => Frame::BulkString(Some("configured-value".into())),
                                }
                            }
                            b"ASKING" => {
                                assert!(configured, "ASKING ran before setup");
                                Frame::SimpleString("OK".into())
                            }
                            _ => panic!("unexpected command {args:?}"),
                        };
                        if server.send(response).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        Self {
            addr,
            setups,
            traffic,
            reject,
            redirect,
            task,
        }
    }

    fn port(&self) -> u16 {
        self.addr.rsplit_once(':').unwrap().1.parse().unwrap()
    }
}

enum Client {
    Direct(Box<ClusterConnection>),
    Multiplexed(MultiplexedClusterClient),
}
impl Client {
    async fn connect(seed: &str, multiplexed: bool) -> Self {
        if multiplexed {
            Self::Multiplexed(
                MultiplexedClusterClient::builder(seed)
                    .protocol(ProtocolVersion::Resp2)
                    .client_name("cluster-736")
                    .connect()
                    .await
                    .unwrap(),
            )
        } else {
            Self::Direct(Box::new(
                ClusterConnection::builder(seed)
                    .protocol(ProtocolVersion::Resp2)
                    .client_name("cluster-736")
                    .connect()
                    .await
                    .unwrap(),
            ))
        }
    }
    async fn get(&mut self) -> Result<Option<Bytes>, RedisError> {
        match self {
            Self::Direct(c) => c.execute(Get::new("setup-key")).await,
            Self::Multiplexed(c) => c.execute(Get::new("setup-key")).await,
        }
    }
    async fn refresh(&mut self) -> Result<(), RedisError> {
        match self {
            Self::Direct(c) => c.refresh_topology().await,
            Self::Multiplexed(c) => c.refresh_topology().await,
        }
    }
    async fn shutdown(self) {
        if let Self::Multiplexed(c) = self {
            c.shutdown().await;
        }
    }
}

#[tokio::test]
async fn new_redirect_and_topology_nodes_receive_setup_on_both_clients() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        for multiplexed in [false, true] {
            for route in ["MOVED", "ASK", "refresh"] {
                let owner = Arc::new(Mutex::new(0));
                let seed = Node::start(owner.clone()).await;
                let target = Node::start(owner.clone()).await;
                *owner.lock().unwrap() = seed.port();
                let mut client = Client::connect(&seed.addr, multiplexed).await;
                assert_eq!(
                    seed.setups.load(Ordering::SeqCst),
                    2,
                    "discovery and initial worker must both be configured"
                );
                assert_eq!(target.setups.load(Ordering::SeqCst), 0);
                if route == "refresh" {
                    *owner.lock().unwrap() = target.port();
                    client.refresh().await.unwrap();
                } else {
                    *seed.redirect.lock().unwrap() = Some((route, target.addr.clone()));
                    if route == "MOVED" {
                        *owner.lock().unwrap() = target.port();
                    }
                }
                assert_eq!(client.get().await.unwrap(), Some("configured-value".into()));
                assert!(
                    target.setups.load(Ordering::SeqCst) >= 1,
                    "new {route} target was not configured"
                );
                assert_eq!(target.traffic.load(Ordering::SeqCst), 1);
                client.shutdown().await;
            }
        }
    })
    .await
    .expect("Cluster setup path hung");
}

#[tokio::test]
async fn redirect_setup_failure_prevents_application_traffic() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for multiplexed in [false, true] {
            let owner = Arc::new(Mutex::new(0));
            let seed = Node::start(owner.clone()).await;
            let target = Node::start(owner.clone()).await;
            *owner.lock().unwrap() = seed.port();
            let mut client = Client::connect(&seed.addr, multiplexed).await;
            target.reject.store(true, Ordering::SeqCst);
            *seed.redirect.lock().unwrap() = Some(("ASK", target.addr.clone()));
            let error = client.get().await.unwrap_err();
            assert!(error.is_connection_error());
            assert!(!format!("{error:?} {error}").contains("private"));
            assert_eq!(target.traffic.load(Ordering::SeqCst), 0);
            client.shutdown().await;
        }
    })
    .await
    .expect("failed Cluster setup hung");
}
