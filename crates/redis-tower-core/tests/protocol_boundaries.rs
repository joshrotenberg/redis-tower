//! A protocol error must quarantine its connection, never shift later replies.

use std::time::Duration;

use futures::{StreamExt, future::poll_fn};
use redis_tower_core::{Command, ConnectionConfig, RedisConnection, RedisError, RedisStream};
use redis_tower_protocol::helpers::{array, bulk};
use redis_tower_protocol::{Frame, ProtocolError, RespCodec, RespLimits};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tower_service::Service;

const DEADLINE: Duration = Duration::from_secs(5);

struct RawPing;

impl Command for RawPing {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("PING")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        // Accept any frame so these tests cannot pass merely because typed
        // response conversion happens to reject a leaked streaming token.
        Ok(frame)
    }

    fn name(&self) -> &str {
        "PING"
    }
}

async fn fake_connection(
    response: Vec<u8>,
    request_count: usize,
    config: ConnectionConfig,
) -> (RedisConnection, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
    let client = client.unwrap();
    let (server, _) = accepted.unwrap();

    let task = tokio::spawn(async move {
        let mut framed = Framed::new(server, RespCodec::new());
        for _ in 0..request_count {
            let request = timeout(DEADLINE, framed.next())
                .await
                .expect("client never sent the expected request")
                .expect("client disconnected before the request")
                .expect("client sent malformed RESP");
            assert_eq!(request, RawPing.to_frame());
        }
        framed.get_mut().write_all(&response).await.unwrap();
        match timeout(DEADLINE, framed.next()).await {
            Ok(None) | Ok(Some(Err(_))) => {}
            Ok(Some(Ok(request))) => panic!("quarantined transport was reused: {request:?}"),
            Err(_) => panic!("protocol error left the transport open"),
        }
    });
    (
        RedisConnection::from_stream_with_config(RedisStream::Tcp(client), &config),
        task,
    )
}

fn assert_unsupported(error: RedisError) {
    assert!(
        matches!(
            error,
            RedisError::Protocol(ProtocolError::Io(ref error))
                if error.kind() == std::io::ErrorKind::Unsupported
        ),
        "expected an unsupported protocol error, got {error:?}"
    );
}

async fn assert_closed(connection: &mut RedisConnection) {
    assert!(matches!(
        timeout(DEADLINE, connection.execute(RawPing))
            .await
            .unwrap(),
        Err(RedisError::ConnectionClosed)
    ));
}

#[tokio::test]
async fn unsupported_responses_cannot_become_typed_results_or_later_replies() {
    let responses: &[&[u8]] = &[
        b"$?\r\n;3\r\nabc\r\n;0\r\n",
        b"!?\r\n",
        b"=?\r\n",
        b"*?\r\n:1\r\n.\r\n",
        b"~?\r\n:1\r\n.\r\n",
        b"%?\r\n+key\r\n:1\r\n.\r\n",
        b">?\r\n+message\r\n.\r\n",
        b"|?\r\n+metadata\r\n:1\r\n.\r\n",
        b";3\r\nabc\r\n",
        b".\r\n",
        b"|0\r\n",
        b"*1\r\n|?\r\n",
    ];
    for &response in responses {
        let mut wire = response.to_vec();
        wire.extend_from_slice(b"+FIRST\r\n+SECOND\r\n");
        let (mut connection, server) = fake_connection(wire, 1, ConnectionConfig::new()).await;
        let result = timeout(DEADLINE, connection.execute(RawPing))
            .await
            .expect("unsupported input left the command waiting");
        assert_unsupported(result.unwrap_err());
        assert_closed(&mut connection).await;
        timeout(DEADLINE, server).await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn service_call_quarantines_unsupported_response_before_returning_transport() {
    let (mut connection, server) = fake_connection(
        b"|?\r\n+metadata\r\n:1\r\n.\r\n+OK\r\n".to_vec(),
        1,
        ConnectionConfig::new(),
    )
    .await;
    timeout(
        DEADLINE,
        poll_fn(|cx| Service::<RawPing>::poll_ready(&mut connection, cx)),
    )
    .await
    .unwrap()
    .unwrap();
    let error = timeout(DEADLINE, Service::<RawPing>::call(&mut connection, RawPing))
        .await
        .unwrap()
        .unwrap_err();
    assert_unsupported(error);
    assert!(matches!(
        timeout(
            DEADLINE,
            poll_fn(|cx| Service::<RawPing>::poll_ready(&mut connection, cx)),
        )
        .await
        .unwrap(),
        Err(RedisError::ConnectionClosed)
    ));
    assert_closed(&mut connection).await;
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn pipeline_rejects_unsupported_second_reply_without_reusing_the_tail() {
    let (mut connection, server) = fake_connection(
        b"+FIRST\r\n*?\r\n:1\r\n.\r\n+SECOND\r\n+THIRD\r\n".to_vec(),
        2,
        ConnectionConfig::new(),
    )
    .await;
    let error = timeout(
        DEADLINE,
        connection.execute_pipeline(vec![RawPing.to_frame(), RawPing.to_frame()]),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_unsupported(error);
    assert_closed(&mut connection).await;
    timeout(DEADLINE, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn complete_oversized_reply_quarantines_connection_before_later_valid_reply() {
    let config = ConnectionConfig::new().with_resp_limits(RespLimits {
        max_frame_size: 8,
        max_depth: 16,
    });
    let (mut connection, server) =
        fake_connection(b"$16\r\nabcdefghijklmnop\r\n+OK\r\n".to_vec(), 1, config).await;
    let error = timeout(DEADLINE, connection.execute(RawPing))
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        error,
        RedisError::Protocol(ProtocolError::FrameTooLarge { max: 8, .. })
    ));
    assert_closed(&mut connection).await;
    timeout(DEADLINE, server).await.unwrap().unwrap();
}
