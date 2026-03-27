use bytes::Bytes;
use redis_tower::commands::*;
use redis_tower::{
    Pipeline, PubSubConnection, RedisClient, RedisConnection, Transaction, TransactionResult,
};
use tokio_stream::StreamExt;
use tower::Service;

async fn conn() -> RedisConnection {
    RedisConnection::connect("127.0.0.1:6379")
        .await
        .expect("Redis must be running on localhost:6379")
}

async fn client() -> RedisClient {
    RedisClient::connect("127.0.0.1:6379")
        .await
        .expect("Redis must be running on localhost:6379")
}

/// Generate a unique key prefix for test isolation.
fn key(test: &str, name: &str) -> String {
    format!("redis_tower_test:{test}:{name}")
}

// -- Connection tests --

#[tokio::test]
async fn connect_and_ping() {
    let conn = conn().await;
    let pong = conn.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

#[tokio::test]
async fn ping_with_message() {
    let conn = conn().await;
    let echo = conn.execute(Ping::with_message("hello")).await.unwrap();
    assert_eq!(echo, "hello");
}

#[tokio::test]
async fn connect_url() {
    let conn = RedisConnection::connect_url("redis://127.0.0.1:6379")
        .await
        .unwrap();
    let pong = conn.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

// -- String command tests --

#[tokio::test]
async fn set_and_get() {
    let conn = conn().await;
    let k = key("set_and_get", "foo");
    conn.execute(Set::new(&k, "bar")).await.unwrap();
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("bar")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn get_nonexistent() {
    let conn = conn().await;
    let val = conn
        .execute(Get::new(key("get_nonexistent", "x")))
        .await
        .unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn set_with_ex() {
    let conn = conn().await;
    let k = key("set_with_ex", "k");
    conn.execute(Set::new(&k, "value").ex(10)).await.unwrap();
    let ttl = conn.execute(Ttl::new(&k)).await.unwrap();
    assert!(ttl > 0 && ttl <= 10);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn set_nx_succeeds_when_missing() {
    let conn = conn().await;
    let k = key("set_nx_ok", "k");
    conn.execute(Del::new(&k)).await.unwrap();
    let result = conn.execute(Set::new(&k, "value").nx()).await.unwrap();
    assert_eq!(result, None); // OK
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("value")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn set_nx_fails_when_exists() {
    let conn = conn().await;
    let k = key("set_nx_fail", "k");
    conn.execute(Set::new(&k, "first")).await.unwrap();
    let result = conn.execute(Set::new(&k, "second").nx()).await.unwrap();
    assert_eq!(result, None);
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("first")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn set_with_get() {
    let conn = conn().await;
    let k = key("set_with_get", "k");
    conn.execute(Set::new(&k, "old")).await.unwrap();
    let old = conn.execute(Set::new(&k, "new").get()).await.unwrap();
    assert_eq!(old, Some(Bytes::from("old")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn incr() {
    let conn = conn().await;
    let k = key("incr", "counter");
    conn.execute(Set::new(&k, "10")).await.unwrap();
    let val = conn.execute(Incr::new(&k)).await.unwrap();
    assert_eq!(val, 11);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn incr_creates_key() {
    let conn = conn().await;
    let k = key("incr_create", "counter");
    conn.execute(Del::new(&k)).await.unwrap();
    let val = conn.execute(Incr::new(&k)).await.unwrap();
    assert_eq!(val, 1);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn mget() {
    let conn = conn().await;
    let a = key("mget", "a");
    let b = key("mget", "b");
    let missing = key("mget", "missing");
    conn.execute(Set::new(&a, "1")).await.unwrap();
    conn.execute(Set::new(&b, "2")).await.unwrap();
    let vals = conn
        .execute(MGet::new([a.as_str(), b.as_str(), missing.as_str()]))
        .await
        .unwrap();
    assert_eq!(vals.len(), 3);
    assert_eq!(vals[0], Some(Bytes::from("1")));
    assert_eq!(vals[1], Some(Bytes::from("2")));
    assert_eq!(vals[2], None);
    conn.execute(Del::keys([&a, &b])).await.unwrap();
}

// -- Key command tests --

#[tokio::test]
async fn del_single() {
    let conn = conn().await;
    let k = key("del_single", "k");
    conn.execute(Set::new(&k, "x")).await.unwrap();
    let removed = conn.execute(Del::new(&k)).await.unwrap();
    assert_eq!(removed, 1);
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn del_multiple() {
    let conn = conn().await;
    let k1 = key("del_multi", "d1");
    let k2 = key("del_multi", "d2");
    let k3 = key("del_multi", "d3");
    conn.execute(Set::new(&k1, "x")).await.unwrap();
    conn.execute(Set::new(&k2, "y")).await.unwrap();
    let removed = conn
        .execute(Del::keys([k1.as_str(), k2.as_str(), k3.as_str()]))
        .await
        .unwrap();
    assert_eq!(removed, 2);
}

#[tokio::test]
async fn exists() {
    let conn = conn().await;
    let k = key("exists", "e1");
    let missing = key("exists", "missing");
    conn.execute(Set::new(&k, "x")).await.unwrap();
    let count = conn
        .execute(Exists::keys([k.as_str(), missing.as_str()]))
        .await
        .unwrap();
    assert_eq!(count, 1);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn expire_and_ttl() {
    let conn = conn().await;
    let k = key("expire_ttl", "k");
    conn.execute(Set::new(&k, "x")).await.unwrap();

    let ttl_before = conn.execute(Ttl::new(&k)).await.unwrap();
    assert_eq!(ttl_before, -1);

    let set = conn.execute(Expire::new(&k, 60)).await.unwrap();
    assert!(set);

    let ttl_after = conn.execute(Ttl::new(&k)).await.unwrap();
    assert!(ttl_after > 0 && ttl_after <= 60);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn ttl_nonexistent() {
    let conn = conn().await;
    let ttl = conn.execute(Ttl::new(key("ttl_none", "k"))).await.unwrap();
    assert_eq!(ttl, -2);
}

#[tokio::test]
async fn expire_nonexistent() {
    let conn = conn().await;
    let set = conn
        .execute(Expire::new(key("expire_none", "k"), 60))
        .await
        .unwrap();
    assert!(!set);
}

// -- Tower Service trait tests --

#[tokio::test]
async fn service_call() {
    let mut conn = conn().await;
    let k = key("service_call", "k");
    conn.execute(Set::new(&k, "tower")).await.unwrap();
    let val: Option<Bytes> = conn.call(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("tower")));
    conn.execute(Del::new(&k)).await.unwrap();
}

// -- RedisClient (shared wrapper) tests --

#[tokio::test]
async fn client_shared_across_tasks() {
    let client = client().await;
    let k1 = key("client_shared", "task1");
    let k2 = key("client_shared", "task2");

    let c1 = client.clone();
    let k1c = k1.clone();
    let h1 = tokio::spawn(async move {
        c1.execute(Set::new(&k1c, "a")).await.unwrap();
    });

    let c2 = client.clone();
    let k2c = k2.clone();
    let h2 = tokio::spawn(async move {
        c2.execute(Set::new(&k2c, "b")).await.unwrap();
    });

    h1.await.unwrap();
    h2.await.unwrap();

    let v1 = client.execute(Get::new(&k1)).await.unwrap();
    let v2 = client.execute(Get::new(&k2)).await.unwrap();
    assert_eq!(v1, Some(Bytes::from("a")));
    assert_eq!(v2, Some(Bytes::from("b")));
    client.execute(Del::keys([&k1, &k2])).await.unwrap();
}

// -- Error handling tests --

#[tokio::test]
async fn redis_error_on_wrong_type() {
    let conn = conn().await;
    let k = key("err_wrong_type", "k");
    conn.execute(Set::new(&k, "not_a_number")).await.unwrap();
    let result = conn.execute(Incr::new(&k)).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("not an integer"),
        "expected integer error, got: {err}"
    );
    conn.execute(Del::new(&k)).await.unwrap();
}

// -- Pipeline tests --

#[tokio::test]
async fn pipeline_basic() {
    let conn = conn().await;
    let k1 = key("pipe_basic", "a");
    let k2 = key("pipe_basic", "b");

    let mut results = Pipeline::new()
        .push(Set::new(&k1, "hello"))
        .push(Set::new(&k2, "world"))
        .push(Get::new(&k1))
        .push(Get::new(&k2))
        .execute(&conn)
        .await
        .unwrap();

    assert_eq!(results.len(), 4);
    let v1: Option<Bytes> = results.take(2).unwrap();
    let v2: Option<Bytes> = results.take(3).unwrap();
    assert_eq!(v1, Some(Bytes::from("hello")));
    assert_eq!(v2, Some(Bytes::from("world")));

    conn.execute(Del::keys([&k1, &k2])).await.unwrap();
}

#[tokio::test]
async fn pipeline_with_errors() {
    let conn = conn().await;
    let k = key("pipe_err", "k");
    conn.execute(Set::new(&k, "not_a_number")).await.unwrap();

    let results = Pipeline::new()
        .push(Incr::new(&k)) // will error
        .push(Ping::new()) // will succeed
        .execute(&conn)
        .await
        .unwrap();

    // First result should be an error.
    assert!(results.get::<i64>(0).is_err());
    // Second result should succeed.
    let pong: &String = results.get(1).unwrap();
    assert_eq!(pong, "PONG");

    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn pipeline_incr_sequence() {
    let conn = conn().await;
    let k = key("pipe_incr", "counter");
    conn.execute(Del::new(&k)).await.unwrap();

    let results = Pipeline::new()
        .push(Incr::new(&k))
        .push(Incr::new(&k))
        .push(Incr::new(&k))
        .execute(&conn)
        .await
        .unwrap();

    assert_eq!(*results.get::<i64>(0).unwrap(), 1);
    assert_eq!(*results.get::<i64>(1).unwrap(), 2);
    assert_eq!(*results.get::<i64>(2).unwrap(), 3);

    conn.execute(Del::new(&k)).await.unwrap();
}

// -- Transaction tests --

#[tokio::test]
async fn transaction_basic() {
    let conn = conn().await;
    let k = key("txn_basic", "k");
    conn.execute(Del::new(&k)).await.unwrap();

    let result = Transaction::new()
        .push(Set::new(&k, "1"))
        .push(Incr::new(&k))
        .push(Get::new(&k))
        .execute(&conn)
        .await
        .unwrap();

    match result {
        TransactionResult::Committed(results) => {
            let incr_val: &i64 = results.get(1).unwrap();
            assert_eq!(*incr_val, 2);
            let get_val: &Option<Bytes> = results.get(2).unwrap();
            assert_eq!(*get_val, Some(Bytes::from("2")));
        }
        TransactionResult::Aborted => panic!("transaction should not abort"),
    }

    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn transaction_watch_no_conflict() {
    let conn = conn().await;
    let k = key("txn_watch_ok", "k");
    conn.execute(Set::new(&k, "10")).await.unwrap();

    let result = Transaction::new()
        .watch([k.as_str()])
        .push(Incr::new(&k))
        .execute(&conn)
        .await
        .unwrap();

    match result {
        TransactionResult::Committed(results) => {
            assert_eq!(*results.get::<i64>(0).unwrap(), 11);
        }
        TransactionResult::Aborted => panic!("transaction should not abort"),
    }

    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn transaction_watch_aborted() {
    let k = key("txn_watch_abort", "k");

    let conn1 = conn().await;
    conn1.execute(Set::new(&k, "original")).await.unwrap();

    // Send WATCH manually, then modify from another connection, then MULTI/EXEC.
    use redis_tower_protocol::helpers::{array, bulk};

    let watch_frame = array(vec![bulk("WATCH"), bulk(k.as_str())]);
    let multi_frame = array(vec![bulk("MULTI")]);
    let incr_frame = array(vec![bulk("INCR"), bulk(k.as_str())]);
    let exec_frame = array(vec![bulk("EXEC")]);

    // WATCH on conn1.
    let frames = conn1.execute_pipeline(vec![watch_frame]).await.unwrap();
    assert!(matches!(frames[0], redis_tower::Frame::SimpleString(_)));

    // Modify the key from conn2 (breaks the WATCH).
    let conn2 = conn().await;
    conn2.execute(Set::new(&k, "modified")).await.unwrap();

    // MULTI + INCR + EXEC on conn1. EXEC should return null (aborted).
    let frames = conn1
        .execute_pipeline(vec![multi_frame, incr_frame, exec_frame])
        .await
        .unwrap();

    // frames[0] = OK (MULTI), frames[1] = QUEUED, frames[2] = null (aborted)
    let exec_result = &frames[2];
    assert!(
        matches!(
            exec_result,
            redis_tower::Frame::Array(None) | redis_tower::Frame::Null
        ),
        "expected null for aborted transaction, got: {exec_result:?}"
    );

    conn1.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn transaction_empty() {
    let conn = conn().await;
    let result = Transaction::new().execute(&conn).await.unwrap();
    match result {
        TransactionResult::Committed(results) => {
            assert_eq!(results.len(), 0);
        }
        TransactionResult::Aborted => panic!("empty transaction should not abort"),
    }
}

// -- Pub/Sub tests --

#[tokio::test]
async fn pubsub_basic() {
    let channel = key("pubsub_basic", "ch");

    let sub_conn = conn().await;
    let mut pubsub = PubSubConnection::from_connection(sub_conn).unwrap();
    pubsub.subscribe(&[&channel]).await.unwrap();

    // Publish from a separate connection.
    let pub_conn = conn().await;
    use redis_tower_protocol::helpers::{array, bulk};
    let publish_frame = array(vec![
        bulk("PUBLISH"),
        bulk(channel.as_str()),
        bulk("hello pubsub"),
    ]);
    pub_conn
        .execute_pipeline(vec![publish_frame])
        .await
        .unwrap();

    let msg = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub.next())
        .await
        .expect("timeout waiting for message")
        .expect("stream ended")
        .expect("parse error");

    assert_eq!(msg.channel, channel);
    assert_eq!(msg.payload, Bytes::from("hello pubsub"));
    assert_eq!(msg.kind, redis_tower::MessageKind::Message);
    assert!(msg.pattern.is_none());
}

#[tokio::test]
async fn pubsub_pattern() {
    let prefix = key("pubsub_pat", "");
    let pattern = format!("{prefix}*");
    let channel = format!("{prefix}events");

    let sub_conn = conn().await;
    let mut pubsub = PubSubConnection::from_connection(sub_conn).unwrap();
    pubsub.psubscribe(&[&pattern]).await.unwrap();

    let pub_conn = conn().await;
    use redis_tower_protocol::helpers::{array, bulk};
    let publish_frame = array(vec![
        bulk("PUBLISH"),
        bulk(channel.as_str()),
        bulk("pattern msg"),
    ]);
    pub_conn
        .execute_pipeline(vec![publish_frame])
        .await
        .unwrap();

    let msg = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("parse error");

    assert_eq!(msg.kind, redis_tower::MessageKind::PMessage);
    assert_eq!(msg.channel, channel);
    assert_eq!(msg.pattern.as_deref(), Some(pattern.as_str()));
    assert_eq!(msg.payload, Bytes::from("pattern msg"));
}

#[tokio::test]
async fn pubsub_multiple_messages() {
    let channel = key("pubsub_multi", "ch");

    let sub_conn = conn().await;
    let mut pubsub = PubSubConnection::from_connection(sub_conn).unwrap();
    pubsub.subscribe(&[&channel]).await.unwrap();

    let pub_conn = conn().await;
    use redis_tower_protocol::helpers::{array, bulk};

    for i in 0..3 {
        let frame = array(vec![
            bulk("PUBLISH"),
            bulk(channel.as_str()),
            bulk(format!("msg-{i}")),
        ]);
        pub_conn.execute_pipeline(vec![frame]).await.unwrap();
    }

    for i in 0..3 {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("parse error");
        assert_eq!(msg.payload, Bytes::from(format!("msg-{i}")));
    }
}
