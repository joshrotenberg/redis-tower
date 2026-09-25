//! Live Unix URL compatibility coverage.
//!
//! These tests exercise the redis-rs-compatible URL grammar against a real
//! Redis server. The socket filename deliberately contains a space so the
//! test fails if the connector opens the encoded path literally.

#![cfg(unix)]

use std::net::TcpListener;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use redis_server_wrapper::RedisServer;
use redis_tower::commands::{AclSetUser, AclWhoAmI, Get, Select, Set};
use redis_tower::reconnect::{ConnectionFactory, UrlConnectionFactory};
use redis_tower::{ConnectionConfig, ProtocolVersion, RedisConnection, RedisError};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read ephemeral port")
        .port()
}

fn unix_url(scheme: &str, socket: &Path, query: &str) -> String {
    let encoded_path = socket
        .to_str()
        .expect("test socket path is UTF-8")
        .replace(' ', "%20");
    format!("{scheme}://{encoded_path}?{query}")
}

#[tokio::test]
async fn authenticated_unix_aliases_replay_setup_and_preserve_typed_errors() {
    let port = free_port();
    let socket = PathBuf::from(format!(
        "/tmp/redis tower url-{}-{port}.sock",
        std::process::id()
    ));
    let _server = RedisServer::new()
        .port(port)
        .unixsocket(&socket)
        .unixsocketperm(700)
        .start()
        .await
        .expect("start Redis with a Unix socket");

    let mut admin = RedisConnection::connect_with_protocol(
        &format!("127.0.0.1:{port}"),
        ProtocolVersion::Resp2,
    )
    .await
    .expect("connect the ACL setup client");
    let username = "unix-agent";
    let password = "secret+value";
    admin
        .execute(
            AclSetUser::new(username)
                .rule("on")
                .rule(format!(">{password}"))
                .rule("+@all")
                .rule("~*"),
        )
        .await
        .expect("create the Unix URL ACL user");

    for scheme in ["redis+unix", "valkey+unix"] {
        let url = unix_url(
            scheme,
            &socket,
            "user=unix-agent&pass=secret%2Bvalue&db=1&protocol=resp3",
        );
        let factory =
            UrlConnectionFactory::new(url).with_connection_config(ConnectionConfig::default());

        let mut first = factory
            .connect()
            .await
            .unwrap_or_else(|error| panic!("connect through {scheme}: {error}"));
        assert!(
            first.is_resp3(),
            "the URL protocol must replace automatic negotiation for {scheme}"
        );
        assert_eq!(
            first.execute(AclWhoAmI::new()).await.unwrap(),
            username,
            "{scheme} should authenticate the ACL user"
        );
        let key = format!("test:unix-url:{scheme}");
        first.execute(Set::new(&key, scheme)).await.unwrap();

        // A second factory connection is reconnect-equivalent: AUTH, SELECT,
        // and HELLO must all be replayed on the fresh socket.
        let mut second = factory.connect().await.unwrap();
        assert!(
            second.is_resp3(),
            "{scheme} reconnect should replay HELLO 3"
        );
        assert_eq!(second.execute(AclWhoAmI::new()).await.unwrap(), username);
        let value: Option<Bytes> = second.execute(Get::new(&key)).await.unwrap();
        assert_eq!(value.as_deref(), Some(scheme.as_bytes()));

        // The setup selected database 1; the same key must not appear in the
        // administrator's still-selected database 0.
        let db_zero: Option<Bytes> = admin.execute(Get::new(&key)).await.unwrap();
        assert!(db_zero.is_none(), "{scheme} did not apply SELECT 1");
    }

    admin.execute(Select::new(1)).await.unwrap();
    for scheme in ["redis+unix", "valkey+unix"] {
        let key = format!("test:unix-url:{scheme}");
        let value: Option<Bytes> = admin.execute(Get::new(&key)).await.unwrap();
        assert_eq!(value.as_deref(), Some(scheme.as_bytes()));
    }

    let wrong_password = unix_url("unix", &socket, "user=unix-agent&pass=wrong&protocol=resp2");
    let auth_error = match RedisConnection::connect_url(&wrong_password).await {
        Err(error) => error,
        Ok(_) => panic!("a wrong Unix URL password must fail"),
    };
    assert!(
        matches!(auth_error, RedisError::Redis(ref message) if message.starts_with("WRONGPASS")),
        "authentication must remain a typed Redis error, got {auth_error:?}"
    );

    let missing_socket = PathBuf::from(format!(
        "/tmp/redis tower missing-{}-{port}.sock",
        std::process::id()
    ));
    let missing_url = unix_url("unix", &missing_socket, "protocol=resp2");
    let connection_error = match RedisConnection::connect_url(&missing_url).await {
        Err(error) => error,
        Ok(_) => panic!("a missing Unix socket must fail"),
    };
    assert!(matches!(connection_error, RedisError::Connection { .. }));
    assert_eq!(
        connection_error.connection_addr(),
        missing_socket.to_str(),
        "connection errors should report the decoded socket path"
    );
}
