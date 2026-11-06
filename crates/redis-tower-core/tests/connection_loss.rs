//! A peer that fails part-way through a reply is a lost connection, not a
//! protocol violation: reconnect gates and "outcome unknown" handling key off
//! `is_connection_error()`.

use futures::StreamExt;
use redis_tower_core::{Command, ConnectionConfig, RedisConnection, RedisError, RedisStream};
use redis_tower_protocol::helpers::{array, bulk};
use redis_tower_protocol::{Frame, RespCodec};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

struct RawPing;

impl Command for RawPing {
    type Response = Frame;
    fn to_frame(&self) -> Frame {
        array(vec![bulk("PING")])
    }
    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }
    fn name(&self) -> &str {
        "PING"
    }
}

#[tokio::test]
async fn eof_in_the_middle_of_a_reply_is_a_connection_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
    let (server, _) = accepted.unwrap();
    let task = tokio::spawn(async move {
        let mut framed = Framed::new(server, RespCodec::new());
        let _ = framed.next().await;
        // Half a bulk string, then the server goes away (crash, CLIENT KILL,
        // client-output-buffer-limit disconnect, proxy idle timeout...).
        framed.get_mut().write_all(b"$10\r\nabc").await.unwrap();
        framed.get_mut().shutdown().await.unwrap();
    });
    let mut conn = RedisConnection::from_stream_with_config(
        RedisStream::Tcp(client.unwrap()),
        &ConnectionConfig::default(),
    );
    let error = conn.execute(RawPing).await.unwrap_err();
    task.await.unwrap();
    assert!(
        error.is_connection_error(),
        "mid-reply EOF classified as {error:?} (display: {error}); is_connection_error() = false"
    );
}

/// Queued commands and EXEC are pipelined; the server answers QUEUED, commits,
/// then the link resets before the EXEC reply arrives. The caller must be able
/// to tell "outcome unknown".
#[tokio::test]
async fn reset_before_exec_reply_in_a_pipeline_is_a_connection_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
    let (server, _) = accepted.unwrap();
    let task = tokio::spawn(async move {
        let mut framed = Framed::new(server, RespCodec::new());
        let _set = framed.next().await;
        let _exec = framed.next().await;
        framed
            .get_mut()
            .write_all(b"+QUEUED\r\n*1\r\n+O")
            .await
            .unwrap();
        let server = framed.into_inner();
        #[allow(deprecated)]
        server.set_linger(Some(std::time::Duration::ZERO)).unwrap();
        drop(server);
    });
    let mut conn = RedisConnection::from_stream_with_config(
        RedisStream::Tcp(client.unwrap()),
        &ConnectionConfig::default(),
    );
    let error = conn
        .execute_pipeline(vec![
            array(vec![bulk("SET"), bulk("k"), bulk("v")]),
            array(vec![bulk("EXEC")]),
        ])
        .await
        .unwrap_err();
    task.await.unwrap();
    assert!(
        error.is_connection_error(),
        "EXEC-reply loss classified as {error:?} (display: {error}); is_connection_error() = false"
    );
}

#[tokio::test]
async fn reset_while_awaiting_a_reply_is_a_connection_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
    let (server, _) = accepted.unwrap();
    let task = tokio::spawn(async move {
        let mut framed = Framed::new(server, RespCodec::new());
        let _ = framed.next().await;
        let server = framed.into_inner();
        // SO_LINGER(0) + close sends RST: the client's read fails with ECONNRESET.
        #[allow(deprecated)]
        server.set_linger(Some(std::time::Duration::ZERO)).unwrap();
        drop(server);
    });
    let mut conn = RedisConnection::from_stream_with_config(
        RedisStream::Tcp(client.unwrap()),
        &ConnectionConfig::default(),
    );
    let error = conn.execute(RawPing).await.unwrap_err();
    task.await.unwrap();
    assert!(
        error.is_connection_error(),
        "read-side reset classified as {error:?} (display: {error}); is_connection_error() = false"
    );
}
