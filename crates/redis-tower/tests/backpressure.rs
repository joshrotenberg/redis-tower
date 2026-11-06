//! Large-value exchanges against a peer that applies backpressure.
//!
//! Managed services and proxies can stop reading requests while their own
//! replies go unread. A client that writes a whole batch before reading any
//! reply then deadlocks with such a peer: it blocks writing a large value while
//! the peer blocks writing replies to earlier commands.

#![cfg(unix)]

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use redis_tower::commands::{Get, Set};
use redis_tower::{AutoPipelineConfig, AutoPipelineService, MultiplexedClient, RedisConnection};
use redis_tower_core::{Frame, RedisStream};
use redis_tower_protocol::RespCodec;
use redis_tower_protocol::helpers::{array, bulk};
use tokio::net::UnixStream;
use tokio_util::codec::Framed;

const PAYLOAD: usize = 256 * 1024;

fn command_name(frame: &Frame) -> Vec<u8> {
    match frame {
        Frame::Array(Some(items)) => match items.first() {
            Some(Frame::BulkString(Some(name))) => name.to_ascii_uppercase(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// A peer that reads one request, then finishes writing its whole reply before
/// it reads the next request. GET gets a `payload`-byte bulk reply; anything
/// else gets `+OK`.
fn spawn_read_after_write_peer(
    server_half: UnixStream,
    payload: usize,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut framed = Framed::new(server_half, RespCodec::new());
        let big = Frame::BulkString(Some(Bytes::from(vec![b'v'; payload])));
        while let Some(Ok(request)) = framed.next().await {
            let reply = if command_name(&request) == b"GET" {
                big.clone()
            } else {
                Frame::SimpleString(Bytes::from_static(b"OK"))
            };
            if framed.send(reply).await.is_err() {
                return;
            }
        }
    })
}

fn large_get_then_large_set() -> Vec<Frame> {
    vec![
        array(vec![bulk("GET"), bulk("big")]),
        array(vec![
            bulk("SET"),
            bulk("other"),
            Frame::BulkString(Some(Bytes::from(vec![b'x'; PAYLOAD]))),
        ]),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_reads_replies_while_its_writes_are_backpressured() {
    let (client_half, server_half) = UnixStream::pair().unwrap();
    let server = spawn_read_after_write_peer(server_half, PAYLOAD);

    let mut connection = RedisConnection::from_stream(RedisStream::Unix(client_half));
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        connection.execute_pipeline(large_get_then_large_set()),
    )
    .await;
    server.abort();

    let responses = outcome
        .expect("pipeline deadlocked: it stopped reading while its SET write was backpressured")
        .unwrap();
    assert!(matches!(&responses[0], Frame::BulkString(Some(value)) if value.len() == PAYLOAD));
    assert!(matches!(&responses[1], Frame::SimpleString(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_pipeline_batch_completes_against_a_backpressuring_peer() {
    let (client_half, server_half) = UnixStream::pair().unwrap();
    let server = spawn_read_after_write_peer(server_half, PAYLOAD);

    let connection = RedisConnection::from_stream(RedisStream::Unix(client_half));
    let mut service = AutoPipelineService::new(connection, AutoPipelineConfig::default());
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        service.call_pipeline(large_get_then_large_set()),
    )
    .await;
    server.abort();

    let responses = outcome
        .expect("auto-pipeline batch deadlocked against a backpressuring peer")
        .unwrap();
    assert_eq!(responses.len(), 2);
}

/// Two independent callers coalesced into one batch by `batch_window`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_large_get_and_set_share_a_multiplexed_connection() {
    let (client_half, server_half) = UnixStream::pair().unwrap();
    let server = spawn_read_after_write_peer(server_half, PAYLOAD);

    let connection = RedisConnection::from_stream(RedisStream::Unix(client_half));
    let client = MultiplexedClient::from_connection_with_config(
        connection,
        AutoPipelineConfig {
            batch_window: Duration::from_millis(20),
            ..AutoPipelineConfig::default()
        },
    );

    let getter = client.clone();
    let get = tokio::spawn(async move { getter.execute(Get::new("big")).await });
    let setter = client.clone();
    let set =
        tokio::spawn(async move { setter.execute(Set::new("other", vec![b'x'; PAYLOAD])).await });

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        (get.await.unwrap(), set.await.unwrap())
    })
    .await;
    server.abort();

    let (get, set) = outcome.expect("concurrent GET and large SET wedged the shared connection");
    assert_eq!(get.unwrap().map(|value| value.len()), Some(PAYLOAD));
    set.unwrap();
}

/// One large reply or request must not pin a long-lived connection's buffers at
/// that frame's size.
#[tokio::test]
async fn connection_buffers_shrink_after_large_frames() {
    const BIG: usize = 8 * 1024 * 1024;
    const LIMIT: usize = 1024 * 1024;
    let (client_half, server_half) = UnixStream::pair().unwrap();
    let server = spawn_read_after_write_peer(server_half, BIG);

    let mut connection = RedisConnection::from_stream(RedisStream::Unix(client_half));
    let replies = connection
        .execute_pipeline(vec![array(vec![bulk("GET"), bulk("big")])])
        .await
        .unwrap();
    assert!(matches!(&replies[0], Frame::BulkString(Some(value)) if value.len() == BIG));
    drop(replies);
    connection
        .execute_pipeline(vec![array(vec![bulk("PING")])])
        .await
        .unwrap();
    connection
        .execute_pipeline(vec![array(vec![
            bulk("SET"),
            bulk("k"),
            Frame::BulkString(Some(Bytes::from(vec![b'x'; BIG]))),
        ])])
        .await
        .unwrap();
    connection
        .execute_pipeline(vec![array(vec![bulk("PING")])])
        .await
        .unwrap();

    let mut framed = connection.into_framed().unwrap();
    server.abort();
    // `capacity()` hides the consumed prefix; reserving a modest amount makes a
    // retained large allocation visible while a reclaimed buffer stays small.
    framed.read_buffer_mut().reserve(128 * 1024);
    framed.write_buffer_mut().reserve(128 * 1024);
    let read = framed.read_buffer().capacity();
    let write = framed.write_buffer().capacity();
    assert!(
        read <= LIMIT && write <= LIMIT,
        "idle connection retains read={read} write={write} bytes after a {BIG}-byte frame"
    );
}
