mod common;

use std::time::Duration;

use bytes::Bytes;
use redis_tower::{Command, Frame, MonitorStream, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};
use tokio_stream::StreamExt;

struct BinaryEcho(Bytes);

impl Command for BinaryEcho {
    type Response = Bytes;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("ECHO"), bulk(&self.0)])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(value)) => Ok(value),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "ECHO"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn monitor_stream_preserves_binary_command_arguments() {
    let addr = common::redis_addr().await;

    // Connect the command client first so its CLIENT SETINFO handshake cannot
    // race with the command this test expects from the monitor.
    let mut command_conn = RedisConnection::connect(addr).await.unwrap();
    let monitor_conn = RedisConnection::connect(addr).await.unwrap();
    let mut monitor = MonitorStream::new(monitor_conn).await.unwrap();

    let payload = Bytes::from_static(b"monitor:\x00\"\\\n\x07\x08\r\t\x80\xff");
    let echoed = command_conn
        .execute(BinaryEcho(payload.clone()))
        .await
        .unwrap();
    assert_eq!(echoed, payload);

    let event = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = monitor
                .next()
                .await
                .expect("monitor connection closed")
                .expect("invalid monitor event");
            if event.command.eq_ignore_ascii_case(b"ECHO")
                && event.arguments.first() == Some(&payload)
            {
                break event;
            }
        }
    })
    .await
    .expect("timed out waiting for ECHO monitor event");

    assert_eq!(event.database, 0);
    assert!(!event.source.is_empty());
    assert_eq!(event.arguments, vec![payload]);
    assert!(event.raw.windows(4).any(|window| window == b"\\x00"));
    assert!(event.raw.windows(4).any(|window| window == b"\\xff"));
}
