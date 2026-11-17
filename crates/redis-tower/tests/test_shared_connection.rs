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

/// Redis refuses a malformed `DISCARD` (wrong arity) without leaving `MULTI`,
/// so a batch that only looked terminated left later callers' commands queued
/// inside the transaction. (A refused `EXEC` is answered with `EXECABORT` and
/// does leave `MULTI` on current servers; it is covered here too.)
#[tokio::test]
async fn malformed_terminators_are_rejected_before_the_wire() {
    let mut client = client_with(ProtocolVersion::Resp2).await;
    let key = "shared_conn:malformed_terminator";
    client.execute(Set::new(key, "before")).await.unwrap();

    for terminator in [
        array(vec![bulk("DISCARD"), bulk("extra")]),
        array(vec![bulk("EXEC"), bulk("extra")]),
    ] {
        let result = redis_tower::PipelineExecutor::execute_pipeline(
            &mut client,
            vec![
                array(vec![bulk("MULTI")]),
                array(vec![bulk("SET"), bulk(key), bulk("inside")]),
                terminator,
            ],
        )
        .await;
        assert!(
            matches!(&result, Err(RedisError::Redis(message)) if message.contains("arguments")),
            "{result:?}"
        );
        let other = client.clone();
        let value = tokio::time::timeout(Duration::from_secs(2), other.execute(Get::new(key)))
            .await
            .expect("a later caller hung");
        assert_eq!(
            value.unwrap(),
            Some(Bytes::from_static(b"before")),
            "a later caller's GET was queued inside a leftover MULTI"
        );
    }
}

/// Create an ACL user allowed everything except `denied`.
async fn user_without(admin: &mut RedisConnection, user: &str, denied: &str) {
    admin
        .execute(
            RawCommand::new("ACL")
                .arg("SETUSER")
                .arg(user)
                .arg("reset")
                .arg("on")
                .arg(">pw")
                .arg("~*")
                .arg("&*")
                .arg("+@all")
                .arg(format!("-{denied}")),
        )
        .await
        .unwrap();
}

/// A well-formed `DISCARD` that the server refuses (here an ACL denial) also
/// leaves the connection inside `MULTI`. The worker must not reuse it: the
/// transaction's caller sees the refusal, a caller batched behind it fails
/// instead of receiving `QUEUED`, and later callers get a fresh connection.
#[tokio::test]
async fn refused_discard_discards_the_connection_left_inside_multi() {
    let addr = common::redis_addr().await;
    let mut admin = RedisConnection::connect(addr).await.unwrap();
    user_without(&mut admin, "shared_conn_nodiscard", "discard").await;
    let key = "shared_conn:refused_discard";
    admin.execute(Set::new(key, "before")).await.unwrap();

    let client = MultiplexedClient::from_factory(
        UrlConnectionFactory::new(format!("redis://shared_conn_nodiscard:pw@{addr}")),
        AutoPipelineConfig {
            batch_window: Duration::from_millis(50),
            ..AutoPipelineConfig::default()
        },
        AutoPipelineReconnectConfig::default(),
    )
    .await
    .unwrap();
    let batched = client.clone();
    let mut transaction_client = client.clone();
    let transaction = tokio::spawn(async move {
        redis_tower::PipelineExecutor::execute_pipeline(
            &mut transaction_client,
            vec![
                array(vec![bulk("MULTI")]),
                array(vec![bulk("SET"), bulk(key), bulk("inside")]),
                array(vec![bulk("DISCARD")]),
            ],
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    let behind = tokio::spawn(async move { batched.execute(Get::new(key)).await });

    let replies = transaction.await.unwrap().unwrap();
    assert!(
        matches!(&replies[2], Frame::Error(message) if message.starts_with(b"NOPERM")),
        "{replies:?}"
    );
    let behind = behind.await.unwrap();
    assert!(
        !matches!(&behind, Ok(Some(value)) if value.as_ref() == b"QUEUED"),
        "the caller batched behind the refused DISCARD received QUEUED"
    );
    let later = tokio::time::timeout(Duration::from_secs(5), client.execute(Get::new(key)))
        .await
        .expect("a later caller hung")
        .unwrap();
    assert_eq!(later, Some(Bytes::from_static(b"before")));

    admin
        .execute(
            RawCommand::new("ACL")
                .arg("DELUSER")
                .arg("shared_conn_nodiscard"),
        )
        .await
        .unwrap();
    client.shutdown_handle().shutdown().await;
}

/// A refused `EXEC` is answered with `EXECABORT`, which ends the transaction,
/// so the connection stays usable.
#[tokio::test]
async fn refused_exec_keeps_the_connection() {
    let addr = common::redis_addr().await;
    let mut admin = RedisConnection::connect(addr).await.unwrap();
    user_without(&mut admin, "shared_conn_noexec", "exec").await;
    let key = "shared_conn:refused_exec";
    admin.execute(Set::new(key, "before")).await.unwrap();

    let mut client = MultiplexedClient::from_factory(
        UrlConnectionFactory::new(format!("redis://shared_conn_noexec:pw@{addr}")),
        AutoPipelineConfig::default(),
        AutoPipelineReconnectConfig::default(),
    )
    .await
    .unwrap();
    let refused = Transaction::new()
        .push(Set::new(key, "inside"))
        .execute(&mut client)
        .await;
    assert!(refused.is_err());
    assert_eq!(
        client.execute(Get::new(key)).await.unwrap(),
        Some(Bytes::from_static(b"before"))
    );
    assert!(client.is_connection_healthy());

    admin
        .execute(
            RawCommand::new("ACL")
                .arg("DELUSER")
                .arg("shared_conn_noexec"),
        )
        .await
        .unwrap();
    client.shutdown_handle().shutdown().await;
}

/// `WAIT` and `WAITAOF` hold the connection until replicas acknowledge or the
/// timeout ends; with no replicas and a zero timeout they never return, which
/// would hang every caller sharing the worker.
#[tokio::test]
async fn wait_commands_are_rejected_before_they_stall_the_worker() {
    let client = client_with(ProtocolVersion::Resp2).await;
    client
        .execute(Set::new("shared_conn:wait", "written"))
        .await
        .unwrap();

    let started = std::time::Instant::now();
    match client.execute(Wait::new(1, 0)).await {
        Err(RedisError::Redis(message)) => {
            assert!(message.contains("shared multiplexed"), "{message}")
        }
        other => panic!("typed WAIT reached the shared connection: {other:?}"),
    }
    match client.execute(WaitAof::new(0, 1, 0)).await {
        Err(RedisError::Redis(message)) => {
            assert!(message.contains("shared multiplexed"), "{message}")
        }
        other => panic!("typed WAITAOF reached the shared connection: {other:?}"),
    }
    let raw = assert_neighbors_unaffected(&client, RawCommand::new("WAIT").arg("1").arg("0")).await;
    assert!(raw.contains("ConnectionPool"), "{raw}");
    let raw_aof = assert_neighbors_unaffected(
        &client,
        RawCommand::new("WAITAOF").arg("0").arg("1").arg("0"),
    )
    .await;
    assert!(raw_aof.contains("ConnectionPool"), "{raw_aof}");
    assert!(started.elapsed() < Duration::from_secs(2));
}

async fn guard_client(
    addr: &str,
    user: Option<&str>,
    protocol: ProtocolVersion,
) -> MultiplexedClient {
    let url = match user {
        Some(user) => format!("redis://{user}:pw@{addr}"),
        None => format!("redis://{addr}"),
    };
    MultiplexedClient::from_factory(
        UrlConnectionFactory::new(url)
            .with_connection_config(ConnectionConfig::new().with_protocol(protocol)),
        AutoPipelineConfig {
            batch_window: Duration::from_millis(50),
            // Exercise the default unlimited response wait; test observers
            // and the owned abort handle bound failure and cleanup.
            response_timeout: None,
            ..AutoPipelineConfig::default()
        },
        AutoPipelineReconnectConfig::default(),
    )
    .await
    .unwrap()
}

async fn guard_client_id(client: &MultiplexedClient) -> Frame {
    tokio::time::timeout(
        Duration::from_secs(5),
        client.execute(RawCommand::new("CLIENT").arg("ID")),
    )
    .await
    .expect("backend connection did not recover")
    .unwrap()
}

/// MULTI may be well formed but denied by ACLs. Its following BLPOP/WAIT
/// would then run outside a transaction and block the shared worker forever.
#[tokio::test]
async fn review_guard_denied_multi_cannot_admit_blocking_frames() {
    let addr = common::redis_addr().await;
    let mut admin = RedisConnection::connect(addr).await.unwrap();
    for protocol in [ProtocolVersion::Resp2, ProtocolVersion::Resp3] {
        let user = format!("shared_guard_nomulti_{protocol:?}");
        user_without(&mut admin, &user, "multi").await;
        for blocking in [
            array(vec![bulk("BLPOP"), bulk("shared_guard:empty"), bulk("0")]),
            array(vec![bulk("WAIT"), bulk("1"), bulk("0")]),
        ] {
            admin.execute(Del::new("shared_guard:empty")).await.unwrap();
            let mut client = guard_client(addr, Some(&user), protocol).await;
            let id = guard_client_id(&client).await;
            let Frame::Integer(id_number) = id else {
                panic!("CLIENT ID: {id:?}")
            };
            let outcome = tokio::time::timeout(
                Duration::from_secs(2),
                redis_tower::PipelineExecutor::execute_pipeline(
                    &mut client,
                    vec![
                        array(vec![bulk("MULTI")]),
                        blocking.clone(),
                        array(vec![bulk("EXEC")]),
                    ],
                ),
            )
            .await;
            let peer = admin
                .execute(
                    RawCommand::new("CLIENT")
                        .arg("LIST")
                        .arg("ID")
                        .arg(id_number.to_string()),
                )
                .await;
            let neighbor =
                tokio::time::timeout(Duration::from_millis(500), client.health_check()).await;
            client.shutdown_handle().abort().await;
            eprintln!(
                "{protocol:?} {blocking:?}: outcome={outcome:?}; peer={peer:?}; neighbor={neighbor:?}"
            );
            assert!(
                matches!(outcome, Ok(Err(RedisError::Redis(message))) if message.contains("ConnectionPool")),
                "the unsafe batch must be rejected before relying on MULTI acceptance"
            );
            assert!(
                matches!(neighbor, Ok(Ok(()))),
                "a neighbor stalled behind a blocking frame"
            );
            // A dedicated connection still supports the normal transaction:
            // accepted MULTI makes Redis execute the blocking command without
            // blocking during EXEC. Only the shared admission policy changed.
            let mut direct = RedisConnection::connect_with_config(
                addr,
                &ConnectionConfig::new().with_protocol(protocol),
            )
            .await
            .unwrap();
            let dedicated = tokio::time::timeout(
                Duration::from_secs(2),
                direct.execute_pipeline(vec![
                    array(vec![bulk("MULTI")]),
                    blocking,
                    array(vec![bulk("EXEC")]),
                ]),
            )
            .await
            .expect("a normal dedicated transaction blocked")
            .unwrap();
            assert!(matches!(&dedicated[0], Frame::SimpleString(text) if text.as_ref() == b"OK"));
            assert!(
                matches!(&dedicated[1], Frame::SimpleString(text) if text.as_ref() == b"QUEUED")
            );
            assert!(matches!(&dedicated[2], Frame::Array(Some(_))));
        }
        admin
            .execute(RawCommand::new("ACL").arg("DELUSER").arg(user))
            .await
            .unwrap();
    }
}

/// Wrong-arity UNWATCH never clears Redis's watch state. Reject the entire
/// request before WATCH reaches the socket, preserving unrelated transactions.
#[tokio::test]
async fn review_guard_malformed_unwatch_never_leaves_a_watch() {
    let addr = common::redis_addr().await;
    let mut admin = RedisConnection::connect(addr).await.unwrap();
    for protocol in [ProtocolVersion::Resp2, ProtocolVersion::Resp3] {
        let key = format!("shared_guard:malformed:{protocol:?}");
        let other = format!("{key}:other");
        admin.execute(Del::new(&other)).await.unwrap();
        let mut client = guard_client(addr, None, protocol).await;
        let outcome = redis_tower::PipelineExecutor::execute_pipeline(
            &mut client,
            vec![
                array(vec![bulk("WATCH"), bulk(key.clone())]),
                array(vec![bulk("UNWATCH"), bulk("extra")]),
            ],
        )
        .await;
        admin.execute(Set::new(&key, "changed")).await.unwrap();
        let later = Transaction::new()
            .push(Set::new(&other, "committed"))
            .execute(&mut client)
            .await
            .map(|result| result.is_committed());
        let value = admin.execute(Get::new(&other)).await.unwrap();
        client.shutdown_handle().abort().await;
        admin.execute(Del::new(&key)).await.unwrap();
        admin.execute(Del::new(&other)).await.unwrap();
        eprintln!("{protocol:?}: malformed cleanup={outcome:?}; later={later:?}; value={value:?}");
        assert!(
            matches!(outcome, Err(RedisError::Redis(message)) if message.contains("UNWATCH") && message.contains("arguments"))
        );
        assert!(later.unwrap(), "another caller inherited WATCH");
        assert_eq!(value, Some(Bytes::from_static(b"committed")));
    }
}

/// Confirm actual cleanup replies, including a WATCH that ran after MULTI
/// was refused. The socket must be replaced before later requests reuse it.
/// Commands already flushed behind the failure may have executed; their
/// callers receive ConnectionClosed because their results may be affected.
#[tokio::test]
async fn review_guard_refused_watch_cleanup_retires_the_socket() {
    let addr = common::redis_addr().await;
    let mut admin = RedisConnection::connect(addr).await.unwrap();
    for protocol in [ProtocolVersion::Resp2, ProtocolVersion::Resp3] {
        for denied in [None, Some("unwatch"), Some("multi")] {
            let scenario = denied.unwrap_or("allowed");
            let user = format!("shared_guard_{scenario}_{protocol:?}");
            if let Some(command) = denied {
                user_without(&mut admin, &user, command).await;
            }
            let key = format!("shared_guard:{scenario}:{protocol:?}");
            let other = format!("{key}:other");
            admin.execute(Del::new(&other)).await.unwrap();
            let client = guard_client(addr, denied.map(|_| user.as_str()), protocol).await;
            let before = guard_client_id(&client).await;
            let frames = if denied == Some("multi") {
                vec![
                    array(vec![bulk("MULTI")]),
                    array(vec![bulk("WATCH"), bulk(key.clone())]),
                    array(vec![bulk("EXEC")]),
                ]
            } else {
                vec![
                    array(vec![bulk("WATCH"), bulk(key.clone())]),
                    array(vec![bulk("UNWATCH")]),
                ]
            };
            let mut first = client.clone();
            // Both futures are first polled on this test's current-thread
            // runtime, before the worker flushes their shared batch.
            let (cleanup, follower) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(biased;
                    redis_tower::PipelineExecutor::execute_pipeline(&mut first, frames),
                    client.health_check(),
                )
            })
            .await
            .expect("watch cleanup or its neighbor hung");
            let after = guard_client_id(&client).await;
            admin.execute(Set::new(&key, "changed")).await.unwrap();
            let mut later_client = client.clone();
            // Grant MULTI for the later unrelated transaction without
            // changing the connection or clearing any inherited WATCH.
            if denied == Some("multi") {
                admin
                    .execute(
                        RawCommand::new("ACL")
                            .arg("SETUSER")
                            .arg(&user)
                            .arg("+multi"),
                    )
                    .await
                    .unwrap();
            }
            let later = Transaction::new()
                .push(Set::new(&other, "committed"))
                .execute(&mut later_client)
                .await
                .map(|result| result.is_committed());
            let value = admin.execute(Get::new(&other)).await.unwrap();
            client.shutdown_handle().abort().await;
            if denied.is_some() {
                admin
                    .execute(RawCommand::new("ACL").arg("DELUSER").arg(&user))
                    .await
                    .unwrap();
            }
            admin.execute(Del::new(&key)).await.unwrap();
            admin.execute(Del::new(&other)).await.unwrap();
            eprintln!(
                "{protocol:?} {scenario}: before={before:?}; after={after:?}; cleanup={cleanup:?}; follower={follower:?}; later={later:?}; value={value:?}"
            );
            let cleanup = cleanup.unwrap();
            if denied.is_some() {
                assert!(cleanup.iter().any(
                    |reply| matches!(reply, Frame::Error(message) if message.starts_with(b"NOPERM"))
                ));
                assert_ne!(
                    before, after,
                    "refused cleanup left a reusable watched socket"
                );
                // This follower was flushed behind the request whose
                // cleanup failed; its outcome must not be trusted.
                assert!(
                    matches!(follower, Err(RedisError::ConnectionClosed)),
                    "{follower:?}"
                );
            } else {
                assert_eq!(before, after, "successful UNWATCH need not reconnect");
                follower.unwrap();
            }
            assert!(later.unwrap(), "another caller inherited WATCH");
            assert_eq!(value, Some(Bytes::from_static(b"committed")));
        }
    }
}
