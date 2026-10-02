//! Public connection-setup lifecycle regressions (real Redis and wire ordering).

use futures::{SinkExt, StreamExt};
use redis_tower::commands::{ClientGetName, ClientId, RawCommand, Set};
use redis_tower::credentials::{CredentialConnectionFactory, StaticCredentials};
use redis_tower::reconnect::ConnectionFactory;
use redis_tower::{
    ConnectionConfig, Frame, ProtocolVersion, RedisConnection, RedisError, RedisStream,
    ResilientRedisClient, RespCodec,
};
use tokio_util::codec::Framed;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn logical_name_survives_forced_standalone_reconnect() {
    let server = redis_server_wrapper::RedisServer::new()
        .port(free_port())
        .start()
        .await
        .unwrap();
    let client = ResilientRedisClient::connect_with_connection_config(
        &server.addr(),
        ConnectionConfig::new().with_client_name("redis-mcp-736"),
    )
    .await
    .unwrap();
    assert_eq!(
        client
            .execute(ClientGetName::new())
            .await
            .unwrap()
            .as_deref(),
        Some(b"redis-mcp-736".as_slice())
    );
    let first_id = client.execute(ClientId::new()).await.unwrap();
    let mut admin = RedisConnection::connect(&server.addr()).await.unwrap();
    admin
        .execute(
            RawCommand::new("CLIENT")
                .arg("KILL")
                .arg("ID")
                .arg(first_id.to_string()),
        )
        .await
        .unwrap();
    assert!(
        client.execute(ClientId::new()).await.is_err(),
        "the killed socket was unexpectedly usable"
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Ok(id) = client.execute(ClientId::new()).await {
                assert_ne!(id, first_id);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("standalone did not reconnect");
    assert_eq!(
        client
            .execute(ClientGetName::new())
            .await
            .unwrap()
            .as_deref(),
        Some(b"redis-mcp-736".as_slice())
    );
}

#[tokio::test]
async fn url_and_provider_setup_follow_auth_select_and_hello() {
    for provider_backed in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("redis://alice:secret@{}/2", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut server = Framed::new(RedisStream::Tcp(socket), RespCodec::new());
            for expected in [
                "SETINFO", "SETINFO", "AUTH", "SELECT", "HELLO", "SETNAME", "SET",
            ] {
                let Frame::Array(Some(parts)) = server.next().await.unwrap().unwrap() else {
                    panic!("expected array");
                };
                let command = if expected.starts_with("SET") && expected != "SET" {
                    &parts[1]
                } else {
                    &parts[0]
                };
                assert_eq!(
                    command,
                    &Frame::BulkString(Some(expected.into())),
                    "setup ran before handshake completed"
                );
                server.send(Frame::SimpleString("OK".into())).await.unwrap();
            }
        });
        let config = ConnectionConfig::new()
            .with_protocol(ProtocolVersion::Resp3)
            .with_client_name("after-auth");
        let mut connection = if provider_backed {
            CredentialConnectionFactory::from_url(
                url,
                StaticCredentials::new("alice", "provider-secret"),
            )
            .with_connection_config(config)
            .connect()
            .await
            .unwrap()
        } else {
            RedisConnection::connect_url_with_config(&url, &config)
                .await
                .unwrap()
        };
        connection.execute(Set::new("key", "value")).await.unwrap();
        task.await.unwrap();
    }
}

#[tokio::test]
async fn provider_address_setup_does_not_run_before_authentication() {
    let server = redis_server_wrapper::RedisServer::new()
        .port(free_port())
        .password("secret")
        .start()
        .await
        .unwrap();
    let factory =
        CredentialConnectionFactory::new(server.addr(), StaticCredentials::password("secret"))
            .with_connection_config(
                ConnectionConfig::new()
                    .with_protocol(ProtocolVersion::Resp3)
                    .with_client_name("provider-name"),
            );
    for _ in 0..2 {
        let mut connection = factory.connect().await.unwrap();
        assert!(connection.is_resp3());
        assert_eq!(
            connection
                .execute(ClientGetName::new())
                .await
                .unwrap()
                .as_deref(),
            Some(b"provider-name".as_slice())
        );
    }
}

#[tokio::test]
async fn rejected_name_fails_connection_establishment() {
    let server = redis_server_wrapper::RedisServer::new()
        .port(free_port())
        .start()
        .await
        .unwrap();
    let error = RedisConnection::connect_with_config(
        &server.addr(),
        &ConnectionConfig::new().with_client_name("private invalid name"),
    )
    .await
    .err()
    .expect("invalid name was silently ignored");
    assert!(matches!(error, RedisError::Connection { .. }));
    assert!(!format!("{error:?} {error}").contains("private"));
}
