//! Shared-connection safety for `MultiplexedClient`.
//!
//! Every clone shares one socket, and replies are paired with requests by
//! position. Commands that break that pairing or switch the session for every
//! clone must be rejected before they reach the wire, while the client keeps
//! serving everyone else.

mod common;

use std::time::Duration;

use bytes::Bytes;
use redis_tower::auto_pipeline::AutoPipelineReconnectConfig;
use redis_tower::commands::*;
use redis_tower::reconnect::UrlConnectionFactory;
use redis_tower::{
    AutoPipelineConfig, ConnectionConfig, Frame, MultiplexedClient, Pipeline, ProtocolVersion,
    RedisConnection, RedisError, Transaction,
};
use redis_tower_protocol::helpers::{array, bulk};

fn rejection(result: Result<Frame, RedisError>) -> String {
    match result {
        Err(RedisError::Redis(message)) => message,
        other => panic!("expected a client-side rejection, got {other:?}"),
    }
}

async fn client_with(protocol: ProtocolVersion) -> MultiplexedClient {
    let addr = common::redis_addr().await;
    let conn = RedisConnection::connect_with_config(
        addr,
        &ConnectionConfig::new().with_protocol(protocol),
    )
    .await
    .unwrap();
    MultiplexedClient::from_connection_with_config(
        conn,
        AutoPipelineConfig {
            // Coalesce the concurrent callers below into one batch, which is
            // what turned a stray reply into another caller's value.
            batch_window: Duration::from_millis(50),
            response_timeout: Some(Duration::from_secs(2)),
            ..AutoPipelineConfig::default()
        },
    )
}

/// Run `command` concurrently with two GETs batched behind it and check that
/// the GET callers still receive their own values.
async fn assert_neighbors_unaffected(client: &MultiplexedClient, command: RawCommand) -> String {
    let k1 = "shared_conn:neighbor:1";
    let k2 = "shared_conn:neighbor:2";
    client.execute(Set::new(k1, "value-1")).await.unwrap();
    client.execute(Set::new(k2, "value-2")).await.unwrap();

    let (a, b, c) = (client.clone(), client.clone(), client.clone());
    let first = tokio::spawn(async move { a.execute(command).await });
    tokio::time::sleep(Duration::from_millis(5)).await;
    let second = tokio::spawn(async move { b.execute(Get::new(k1)).await });
    tokio::time::sleep(Duration::from_millis(5)).await;
    let third = tokio::spawn(async move { c.execute(Get::new(k2)).await });

    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        (
            first.await.unwrap(),
            second.await.unwrap(),
            third.await.unwrap(),
        )
    })
    .await
    .expect("a rejected command wedged the shared connection");
    assert_eq!(outcome.1.unwrap(), Some(Bytes::from_static(b"value-1")));
    assert_eq!(outcome.2.unwrap(), Some(Bytes::from_static(b"value-2")));
    tokio::time::timeout(Duration::from_secs(2), client.health_check())
        .await
        .expect("later callers must not hang")
        .unwrap();
    rejection(outcome.0)
}

/// RESP3 SUBSCRIBE replies are push frames: accepted on the shared worker,
/// its caller took the next caller's reply and the last caller hung forever.
#[tokio::test]
async fn resp3_subscribe_is_rejected_without_disturbing_other_callers() {
    let client = client_with(ProtocolVersion::Resp3).await;
    let message =
        assert_neighbors_unaffected(&client, RawCommand::new("SUBSCRIBE").arg("shared_conn:ch"))
            .await;
    assert!(message.contains("PubSubConnection"), "{message}");
}

/// redis-rs #2423: a raw multi-channel SUBSCRIBE under RESP2 leaves extra
/// confirmations that become the next caller's reply.
#[tokio::test]
async fn resp2_multi_channel_subscribe_is_rejected() {
    let client = client_with(ProtocolVersion::Resp2).await;
    let message = assert_neighbors_unaffected(
        &client,
        RawCommand::new("SUBSCRIBE")
            .arg("shared_conn:a")
            .arg("shared_conn:b"),
    )
    .await;
    assert!(message.contains("PubSubConnection"), "{message}");
    let unsubscribe = client.execute(RawCommand::new("UNSUBSCRIBE")).await;
    assert!(rejection(unsubscribe).contains("PubSubConnection"));
}

/// CLIENT REPLY SKIP suppressed the next reply, so a batched caller received
/// another caller's value.
#[tokio::test]
async fn client_reply_skip_is_rejected() {
    let client = client_with(ProtocolVersion::Resp2).await;
    let message =
        assert_neighbors_unaffected(&client, RawCommand::new("CLIENT").arg("REPLY").arg("SKIP"))
            .await;
    assert!(message.contains("another caller"), "{message}");
}

#[tokio::test]
async fn monitor_is_rejected_in_favor_of_monitor_stream() {
    let client = client_with(ProtocolVersion::Resp2).await;
    let message = assert_neighbors_unaffected(&client, RawCommand::new("MONITOR")).await;
    assert!(message.contains("MonitorStream"), "{message}");
    // The typed builder reports is_blocking() and names the same API.
    match client.execute(Monitor::new()).await {
        Err(RedisError::Redis(message)) => assert!(message.contains("MonitorStream")),
        other => panic!("typed MONITOR was not rejected: {other:?}"),
    }
}

#[tokio::test]
async fn quit_and_hello_are_rejected() {
    let client = client_with(ProtocolVersion::Resp2).await;
    let quit = assert_neighbors_unaffected(&client, RawCommand::new("QUIT")).await;
    assert!(quit.contains("shutdown()"), "{quit}");
    let hello = assert_neighbors_unaffected(&client, RawCommand::new("HELLO").arg("3")).await;
    assert!(hello.contains("factory"), "{hello}");
}

/// RESET from one clone switched every clone from the ACL user back to
/// `default`, so a read-only user's other clones could suddenly write.
#[tokio::test]
async fn reset_cannot_escalate_other_clones_to_the_default_user() {
    let addr = common::redis_addr().await;
    let mut admin = RedisConnection::connect(addr).await.unwrap();
    admin
        .execute(
            RawCommand::new("ACL")
                .arg("SETUSER")
                .arg("shared_conn_ro")
                .arg("reset")
                .arg("on")
                .arg(">ropass")
                .arg("~*")
                .arg("+@read")
                .arg("+@connection")
                .arg("+acl|whoami"),
        )
        .await
        .unwrap();

    let client = MultiplexedClient::from_factory(
        UrlConnectionFactory::new(format!("redis://shared_conn_ro:ropass@{addr}")),
        AutoPipelineConfig::default(),
        AutoPipelineReconnectConfig::default(),
    )
    .await
    .unwrap();
    let other = client.clone();
    assert!(
        other
            .execute(Set::new("shared_conn:reset", "x"))
            .await
            .is_err()
    );

    let reset = client.execute(Reset::new()).await;
    assert!(
        matches!(&reset, Err(RedisError::Redis(message)) if message.contains("every clone")),
        "{reset:?}"
    );
    let whoami = other
        .execute(RawCommand::new("ACL").arg("WHOAMI"))
        .await
        .unwrap();
    assert_eq!(
        whoami,
        Frame::BulkString(Some(Bytes::from_static(b"shared_conn_ro")))
    );
    assert!(
        other
            .execute(Set::new("shared_conn:reset", "escalated"))
            .await
            .is_err(),
        "the read-only user must still be unable to write"
    );

    admin
        .execute(RawCommand::new("ACL").arg("DELUSER").arg("shared_conn_ro"))
        .await
        .unwrap();
    client.shutdown_handle().shutdown().await;
}

#[tokio::test]
async fn blocking_commands_are_rejected_before_they_stall_the_worker() {
    let client = client_with(ProtocolVersion::Resp2).await;
    let key = "shared_conn:blocking";

    let started = std::time::Instant::now();
    match client.execute(BLPop::new(key, 0.0)).await {
        Err(RedisError::Redis(message)) => assert!(message.contains("ConnectionPool"), "{message}"),
        other => panic!("typed BLPOP was not rejected: {other:?}"),
    }
    let raw =
        assert_neighbors_unaffected(&client, RawCommand::new("BLPOP").arg(key).arg("0")).await;
    assert!(raw.contains("ConnectionPool"), "{raw}");
    match client
        .execute(XRead::new("shared_conn:stream", "$").block(0))
        .await
    {
        Err(RedisError::Redis(message)) => {
            assert!(message.contains("BLOCK") || message.contains("XREAD"))
        }
        other => panic!("XREAD BLOCK was not rejected: {other:?}"),
    }
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "a zero-timeout block must never reach the server"
    );
    // A non-blocking stream read is still fine.
    client
        .execute(XRead::new("shared_conn:stream", "0"))
        .await
        .unwrap();
}

#[tokio::test]
async fn transactions_stay_supported_but_loose_multi_is_rejected() {
    let mut client = client_with(ProtocolVersion::Resp2).await;
    let key = "shared_conn:txn";

    let loose = client.execute(RawCommand::new("MULTI")).await;
    assert!(rejection(loose).contains("Transaction"));
    let watch = client.execute(Watch::new(key)).await;
    assert!(matches!(watch, Err(RedisError::Redis(message)) if message.contains("Transaction")));

    let results = Transaction::new()
        .watch([key])
        .push(Set::new(key, "committed"))
        .push(Get::new(key))
        .execute(&mut client)
        .await
        .unwrap();
    assert!(
        results.committed().is_some(),
        "the watched transaction should commit"
    );

    // A raw pipeline may carry a complete MULTI/EXEC block...
    let raw: Vec<Frame> = redis_tower::PipelineExecutor::execute_pipeline(
        &mut client,
        vec![
            array(vec![bulk("MULTI")]),
            array(vec![bulk("SET"), bulk(key), bulk("raw")]),
            array(vec![bulk("EXEC")]),
        ],
    )
    .await
    .unwrap();
    assert_eq!(raw.len(), 3);
    // ...but not one that leaves MULTI open for the next caller.
    let open = redis_tower::PipelineExecutor::execute_pipeline(
        &mut client,
        vec![
            array(vec![bulk("MULTI")]),
            array(vec![bulk("SET"), bulk(key), bulk("open")]),
        ],
    )
    .await;
    assert!(matches!(open, Err(RedisError::Redis(message)) if message.contains("MULTI")));
    let pipeline = Pipeline::new()
        .push(Get::new(key))
        .execute(&mut client)
        .await
        .unwrap();
    assert_eq!(pipeline.len(), 1);
    assert_eq!(
        client.execute(Get::new(key)).await.unwrap(),
        Some(Bytes::from_static(b"raw"))
    );
}

/// AUTH is how proactive credential rotation re-authenticates the shared
/// worker, so it must keep flowing through `execute`.
#[tokio::test]
async fn auth_remains_allowed_for_credential_rotation() {
    let client = client_with(ProtocolVersion::Resp2).await;
    // The test server runs without a password; Redis answers this AUTH with
    // its own error, which proves the command reached the server.
    match client.execute(RawCommand::new("AUTH").arg("unused")).await {
        Err(RedisError::Redis(message)) => {
            assert!(!message.contains("shared multiplexed"), "{message}");
        }
        Ok(_) => {}
        Err(other) => panic!("unexpected AUTH failure: {other:?}"),
    }
    client.health_check().await.unwrap();
}
