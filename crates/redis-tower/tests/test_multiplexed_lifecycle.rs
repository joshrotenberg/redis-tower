//! Lifecycle behavior of factory-backed and lazy `MultiplexedClient`s with
//! their default settings: unlimited reconnect retries, no connect timeout,
//! and no response timeout.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use redis_tower::auto_pipeline::AutoPipelineReconnectConfig;
use redis_tower::commands::{Get, Ping, Set};
use redis_tower::reconnect::AddrConnectionFactory;
use redis_tower::{AutoPipelineConfig, Frame, MultiplexedClient};
use redis_tower_protocol::RespCodec;
use tokio_util::codec::Framed;

fn command_name(frame: &Frame) -> Vec<u8> {
    match frame {
        Frame::Array(Some(items)) => match items.first() {
            Some(Frame::BulkString(Some(name))) => name.to_ascii_uppercase(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// Serve exactly one connection, answer one `PING` on it, then close it and
/// stop listening so the endpoint refuses every reconnect.
async fn one_shot_server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        drop(listener);
        let mut framed = Framed::new(stream, RespCodec::new());
        while let Some(Ok(request)) = framed.next().await {
            let name = command_name(&request);
            let reply = match name.as_slice() {
                b"HELLO" => Frame::Error(Bytes::from_static(b"ERR unknown command")),
                b"PING" => Frame::SimpleString(Bytes::from_static(b"PONG")),
                _ => Frame::SimpleString(Bytes::from_static(b"OK")),
            };
            framed.send(reply).await.unwrap();
            if name == b"PING" {
                return;
            }
        }
    });
    (addr, server)
}

/// redis-rs #2298 / #2269: with default reconnect settings, a factory-backed
/// client whose server went away for good parked every later caller behind an
/// endless reconnect loop instead of failing it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_factory_client_fails_requests_when_the_server_stays_down() {
    let (addr, server) = one_shot_server().await;
    let client = MultiplexedClient::from_factory(
        AddrConnectionFactory::new(&addr),
        AutoPipelineConfig::default(),
        AutoPipelineReconnectConfig::default(),
    )
    .await
    .unwrap();
    client.execute(Ping::new()).await.unwrap();
    server.await.unwrap();

    for attempt in 0..3 {
        let outcome =
            tokio::time::timeout(Duration::from_secs(10), client.execute(Ping::new())).await;
        assert!(
            matches!(outcome, Ok(Err(_))),
            "request {attempt} after the server vanished was parked: {outcome:?}"
        );
    }
    let shutdown = client.shutdown_handle();
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), shutdown.shutdown())
        .await
        .expect("shutdown cancels the background reconnect campaign");
}

/// The lazy constructor uses the same defaults once it has connected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_client_fails_requests_when_the_server_stays_down() {
    let (addr, server) = one_shot_server().await;
    let client = MultiplexedClient::connect_lazy(addr);
    client.execute(Ping::new()).await.unwrap();
    server.await.unwrap();

    let outcome = tokio::time::timeout(Duration::from_secs(10), client.execute(Ping::new())).await;
    assert!(matches!(outcome, Ok(Err(_))), "{outcome:?}");
    client.shutdown_handle().shutdown().await;
}

/// Against a real server, an owner that retains the shutdown handle can tear
/// the client down while a router still holds a clone, and later calls
/// through that clone fail instead of reviving the worker.
#[tokio::test]
async fn retained_handle_shuts_down_while_a_router_holds_a_clone() {
    let addr = common::redis_addr().await;
    let client = MultiplexedClient::from_factory(
        AddrConnectionFactory::new(addr),
        AutoPipelineConfig::default(),
        AutoPipelineReconnectConfig::default(),
    )
    .await
    .unwrap();
    let shutdown = client.shutdown_handle();
    let router: Arc<MultiplexedClient> = Arc::new(client.clone());
    client
        .execute(Set::new("lifecycle:router", "value"))
        .await
        .unwrap();
    drop(client);

    assert_eq!(
        shutdown.shutdown_timeout(Duration::from_secs(5)).await,
        redis_tower::ShutdownOutcome::Drained
    );
    assert!(shutdown.is_finished());
    assert!(matches!(
        router.execute(Get::new("lifecycle:router")).await,
        Err(redis_tower::RedisError::ConnectionClosed)
    ));
}
