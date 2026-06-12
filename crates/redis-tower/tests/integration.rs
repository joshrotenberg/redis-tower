use bytes::Bytes;
use redis_server_wrapper::RedisServer;
use redis_tower::auto_pipeline::{AutoPipelineConfig, AutoPipelineReconnectConfig};
use redis_tower::commands::*;
use redis_tower::reconnect::{AddrConnectionFactory, ReconnectConfig, UrlConnectionFactory};
use redis_tower::{
    MultiplexedClient, Pipeline, PubSubConnection, RedisClient, RedisConnection,
    ResilientConnection, ResilientRedisClient, Transaction, TransactionResult,
};
use tokio::sync::OnceCell;
use tokio_stream::StreamExt;
use tower::Service;

/// Shared Redis instance -- started once, stopped on Drop.
static REDIS: OnceCell<redis_server_wrapper::RedisServerHandle> = OnceCell::const_new();

/// Address of the shared Redis instance (may be external via REDIS_URL).
static REDIS_ADDR: OnceCell<String> = OnceCell::const_new();

async fn redis_addr() -> &'static str {
    REDIS_ADDR
        .get_or_init(|| async {
            // Check for external Redis first (CI service container).
            if let Ok(url) = std::env::var("REDIS_URL") {
                let addr = url
                    .strip_prefix("redis://")
                    .unwrap_or(&url)
                    .trim_end_matches('/')
                    .to_string();
                return addr;
            }

            let handle = RedisServer::new()
                .port(6399)
                .start()
                .await
                .expect("failed to start Redis server");
            let addr = handle.addr();
            // Store the handle so it lives for the process lifetime.
            REDIS.set(handle).ok();
            addr
        })
        .await
}

async fn conn() -> RedisConnection {
    let addr = redis_addr().await;
    RedisConnection::connect(addr)
        .await
        .expect("failed to connect to Redis")
}

// Connection factory for the shared command test macro.
async fn standalone_conn() -> RedisConnection {
    conn().await
}

// Generate shared command tests for standalone (RESP2).
redis_test_harness::command_tests!(standalone_conn, "standalone_cmd");

// RESP3 connection factory.
async fn resp3_conn() -> RedisConnection {
    let addr = redis_addr().await;
    RedisConnection::connect_resp3(addr)
        .await
        .expect("failed to connect with RESP3")
}

// Generate shared command tests for RESP3 in a submodule to avoid name conflicts.
mod resp3 {
    use super::*;
    redis_test_harness::command_tests!(resp3_conn, "resp3_cmd");
}

async fn client() -> RedisClient {
    let addr = redis_addr().await;
    RedisClient::connect(addr)
        .await
        .expect("failed to connect to Redis")
}

/// Helper: poll_ready then call, honoring the Tower contract.
async fn call_ready<S, Req>(svc: &mut S, req: Req) -> Result<S::Response, S::Error>
where
    S: tower::Service<Req>,
{
    std::future::poll_fn(|cx| svc.poll_ready(cx)).await?;
    svc.call(req).await
}

/// Generate a unique key prefix for test isolation.
fn key(test: &str, name: &str) -> String {
    format!("redis_tower_test:{test}:{name}")
}

// -- Connection tests --

#[tokio::test]
async fn connect_and_ping() {
    let mut conn = conn().await;
    let pong = conn.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

#[tokio::test]
async fn ping_with_message() {
    let mut conn = conn().await;
    let echo = conn.execute(Ping::with_message("hello")).await.unwrap();
    assert_eq!(echo, "hello");
}

#[tokio::test]
async fn connect_url() {
    let addr = redis_addr().await;
    let url = format!("redis://{addr}");
    let mut conn = RedisConnection::connect_url(&url).await.unwrap();
    let pong = conn.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

// -- String command tests --

#[tokio::test]
async fn set_and_get() {
    let mut conn = conn().await;
    let k = key("set_and_get", "foo");
    conn.execute(Set::new(&k, "bar")).await.unwrap();
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("bar")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn get_nonexistent() {
    let mut conn = conn().await;
    let val = conn
        .execute(Get::new(key("get_nonexistent", "x")))
        .await
        .unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn set_with_ex() {
    let mut conn = conn().await;
    let k = key("set_with_ex", "k");
    conn.execute(Set::new(&k, "value").ex(10)).await.unwrap();
    let ttl = conn.execute(Ttl::new(&k)).await.unwrap();
    assert!(ttl > 0 && ttl <= 10);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn set_nx_succeeds_when_missing() {
    let mut conn = conn().await;
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
    let mut conn = conn().await;
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
    let mut conn = conn().await;
    let k = key("set_with_get", "k");
    conn.execute(Set::new(&k, "old")).await.unwrap();
    let old = conn.execute(Set::new(&k, "new").get()).await.unwrap();
    assert_eq!(old, Some(Bytes::from("old")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn incr() {
    let mut conn = conn().await;
    let k = key("incr", "counter");
    conn.execute(Set::new(&k, "10")).await.unwrap();
    let val = conn.execute(Incr::new(&k)).await.unwrap();
    assert_eq!(val, 11);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn incr_creates_key() {
    let mut conn = conn().await;
    let k = key("incr_create", "counter");
    conn.execute(Del::new(&k)).await.unwrap();
    let val = conn.execute(Incr::new(&k)).await.unwrap();
    assert_eq!(val, 1);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn mget() {
    let mut conn = conn().await;
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
    let mut conn = conn().await;
    let k = key("del_single", "k");
    conn.execute(Set::new(&k, "x")).await.unwrap();
    let removed = conn.execute(Del::new(&k)).await.unwrap();
    assert_eq!(removed, 1);
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn del_multiple() {
    let mut conn = conn().await;
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
    let mut conn = conn().await;
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
    let mut conn = conn().await;
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
    let mut conn = conn().await;
    let ttl = conn.execute(Ttl::new(key("ttl_none", "k"))).await.unwrap();
    assert_eq!(ttl, -2);
}

#[tokio::test]
async fn expire_nonexistent() {
    let mut conn = conn().await;
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
    let mut conn = conn().await;
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
    let mut conn = conn().await;
    let k1 = key("pipe_basic", "a");
    let k2 = key("pipe_basic", "b");

    let mut results = Pipeline::new()
        .push(Set::new(&k1, "hello"))
        .push(Set::new(&k2, "world"))
        .push(Get::new(&k1))
        .push(Get::new(&k2))
        .execute(&mut conn)
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
    let mut conn = conn().await;
    let k = key("pipe_err", "k");
    conn.execute(Set::new(&k, "not_a_number")).await.unwrap();

    let results = Pipeline::new()
        .push(Incr::new(&k)) // will error
        .push(Ping::new()) // will succeed
        .execute(&mut conn)
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
    let mut conn = conn().await;
    let k = key("pipe_incr", "counter");
    conn.execute(Del::new(&k)).await.unwrap();

    let results = Pipeline::new()
        .push(Incr::new(&k))
        .push(Incr::new(&k))
        .push(Incr::new(&k))
        .execute(&mut conn)
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
    let mut conn = conn().await;
    let k = key("txn_basic", "k");
    conn.execute(Del::new(&k)).await.unwrap();

    let result = Transaction::new()
        .push(Set::new(&k, "1"))
        .push(Incr::new(&k))
        .push(Get::new(&k))
        .execute(&mut conn)
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
    let mut conn = conn().await;
    let k = key("txn_watch_ok", "k");
    conn.execute(Set::new(&k, "10")).await.unwrap();

    let result = Transaction::new()
        .watch([k.as_str()])
        .push(Incr::new(&k))
        .execute(&mut conn)
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

    let mut conn1 = conn().await;
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
    let mut conn2 = conn().await;
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
async fn transaction_multiplexed_basic() {
    // A Transaction must commit atomically on the shared multiplexed
    // connection (the WATCH/MULTI/EXEC sequence is sent as one contiguous
    // pipeline, so no other task's commands interleave).
    let client = MultiplexedClient::connect(redis_addr().await)
        .await
        .unwrap();
    let k = key("txn_mux_basic", "k");
    client.execute(Del::new(&k)).await.unwrap();

    let mut txn_client = client.clone();
    let result = Transaction::new()
        .push(Set::new(&k, "1"))
        .push(Incr::new(&k))
        .push(Get::new(&k))
        .execute(&mut txn_client)
        .await
        .unwrap();

    match result {
        TransactionResult::Committed(results) => {
            assert_eq!(*results.get::<i64>(1).unwrap(), 2);
            let get_val: &Option<Bytes> = results.get(2).unwrap();
            assert_eq!(*get_val, Some(Bytes::from("2")));
        }
        TransactionResult::Aborted => panic!("transaction should not abort"),
    }

    client.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn transaction_multiplexed_watch_no_conflict() {
    // WATCH frames flow through the multiplexed atomic path; with no conflict
    // the transaction commits.
    let client = MultiplexedClient::connect(redis_addr().await)
        .await
        .unwrap();
    let k = key("txn_mux_watch", "k");
    client.execute(Set::new(&k, "10")).await.unwrap();

    let mut txn_client = client.clone();
    let result = Transaction::new()
        .watch([k.as_str()])
        .push(Incr::new(&k))
        .execute(&mut txn_client)
        .await
        .unwrap();

    match result {
        TransactionResult::Committed(results) => {
            assert_eq!(*results.get::<i64>(0).unwrap(), 11);
        }
        TransactionResult::Aborted => panic!("transaction should not abort"),
    }

    client.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn transaction_empty() {
    let mut conn = conn().await;
    let result = Transaction::new().execute(&mut conn).await.unwrap();
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
    let mut pub_conn = conn().await;
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

    let mut pub_conn = conn().await;
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

    let mut pub_conn = conn().await;
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

// -- Hash command tests --

#[tokio::test]
async fn hset_and_hget() {
    let mut conn = conn().await;
    let k = key("hset_hget", "h");
    let added = conn
        .execute(HSet::new(&k, "field1", "value1").field("field2", "value2"))
        .await
        .unwrap();
    assert_eq!(added, 2);
    let val = conn.execute(HGet::new(&k, "field1")).await.unwrap();
    assert_eq!(val, Some(Bytes::from("value1")));
    let missing = conn.execute(HGet::new(&k, "nope")).await.unwrap();
    assert_eq!(missing, None);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn hdel() {
    let mut conn = conn().await;
    let k = key("hdel", "h");
    conn.execute(HSet::new(&k, "a", "1").field("b", "2").field("c", "3"))
        .await
        .unwrap();
    let removed = conn.execute(HDel::fields(&k, ["a", "b"])).await.unwrap();
    assert_eq!(removed, 2);
    let remaining = conn.execute(HLen::new(&k)).await.unwrap();
    assert_eq!(remaining, 1);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn hexists() {
    let mut conn = conn().await;
    let k = key("hexists", "h");
    conn.execute(HSet::new(&k, "f", "v")).await.unwrap();
    assert!(conn.execute(HExists::new(&k, "f")).await.unwrap());
    assert!(!conn.execute(HExists::new(&k, "nope")).await.unwrap());
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn hgetall() {
    let mut conn = conn().await;
    let k = key("hgetall", "h");
    conn.execute(HSet::new(&k, "a", "1").field("b", "2"))
        .await
        .unwrap();
    let pairs = conn.execute(HGetAll::new(&k)).await.unwrap();
    assert_eq!(pairs.len(), 2);
    // Order is not guaranteed, so check both exist.
    let has_a = pairs
        .iter()
        .any(|(f, v)| f == &Bytes::from("a") && v == &Bytes::from("1"));
    let has_b = pairs
        .iter()
        .any(|(f, v)| f == &Bytes::from("b") && v == &Bytes::from("2"));
    assert!(has_a && has_b);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn hincrby() {
    let mut conn = conn().await;
    let k = key("hincrby", "h");
    conn.execute(HSet::new(&k, "count", "10")).await.unwrap();
    let val = conn.execute(HIncrBy::new(&k, "count", 5)).await.unwrap();
    assert_eq!(val, 15);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn hkeys_hvals_hlen() {
    let mut conn = conn().await;
    let k = key("hkeys_hvals", "h");
    conn.execute(HSet::new(&k, "x", "1").field("y", "2"))
        .await
        .unwrap();
    let len = conn.execute(HLen::new(&k)).await.unwrap();
    assert_eq!(len, 2);
    let keys = conn.execute(HKeys::new(&k)).await.unwrap();
    assert_eq!(keys.len(), 2);
    let vals = conn.execute(HVals::new(&k)).await.unwrap();
    assert_eq!(vals.len(), 2);
    conn.execute(Del::new(&k)).await.unwrap();
}

// -- List command tests --

#[tokio::test]
async fn lpush_rpush_lrange() {
    let mut conn = conn().await;
    let k = key("lpush_rpush", "l");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(RPush::new(&k, "a")).await.unwrap();
    conn.execute(RPush::new(&k, "b")).await.unwrap();
    let len = conn.execute(LPush::new(&k, "z")).await.unwrap();
    assert_eq!(len, 3);
    let items = conn.execute(LRange::new(&k, 0, -1)).await.unwrap();
    assert_eq!(
        items,
        vec![Bytes::from("z"), Bytes::from("a"), Bytes::from("b")]
    );
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn lpop_rpop() {
    let mut conn = conn().await;
    let k = key("lpop_rpop", "l");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(RPush::elements(&k, ["1", "2", "3"]))
        .await
        .unwrap();
    let left = conn.execute(LPop::new(&k)).await.unwrap();
    assert_eq!(left, Some(Bytes::from("1")));
    let right = conn.execute(RPop::new(&k)).await.unwrap();
    assert_eq!(right, Some(Bytes::from("3")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn llen_lindex_lset() {
    let mut conn = conn().await;
    let k = key("llen_lindex", "l");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(RPush::elements(&k, ["a", "b", "c"]))
        .await
        .unwrap();
    assert_eq!(conn.execute(LLen::new(&k)).await.unwrap(), 3);
    assert_eq!(
        conn.execute(LIndex::new(&k, 1)).await.unwrap(),
        Some(Bytes::from("b"))
    );
    conn.execute(LSet::new(&k, 1, "B")).await.unwrap();
    assert_eq!(
        conn.execute(LIndex::new(&k, 1)).await.unwrap(),
        Some(Bytes::from("B"))
    );
    conn.execute(Del::new(&k)).await.unwrap();
}

// -- Set command tests --

#[tokio::test]
async fn sadd_smembers_scard() {
    let mut conn = conn().await;
    let k = key("sadd_smembers", "s");
    conn.execute(Del::new(&k)).await.unwrap();
    let added = conn
        .execute(SAdd::members(&k, ["a", "b", "c"]))
        .await
        .unwrap();
    assert_eq!(added, 3);
    // Adding duplicate.
    let dup = conn.execute(SAdd::new(&k, "a")).await.unwrap();
    assert_eq!(dup, 0);
    assert_eq!(conn.execute(SCard::new(&k)).await.unwrap(), 3);
    let members = conn.execute(SMembers::new(&k)).await.unwrap();
    assert_eq!(members.len(), 3);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn srem_sismember() {
    let mut conn = conn().await;
    let k = key("srem_sismember", "s");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(SAdd::members(&k, ["x", "y", "z"]))
        .await
        .unwrap();
    assert!(conn.execute(SIsMember::new(&k, "x")).await.unwrap());
    let removed = conn.execute(SRem::new(&k, "x")).await.unwrap();
    assert_eq!(removed, 1);
    assert!(!conn.execute(SIsMember::new(&k, "x")).await.unwrap());
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn sinter() {
    let mut conn = conn().await;
    let k1 = key("sinter", "s1");
    let k2 = key("sinter", "s2");
    conn.execute(Del::keys([&k1, &k2])).await.unwrap();
    conn.execute(SAdd::members(&k1, ["a", "b", "c"]))
        .await
        .unwrap();
    conn.execute(SAdd::members(&k2, ["b", "c", "d"]))
        .await
        .unwrap();
    let inter = conn
        .execute(SInter::keys([k1.as_str(), k2.as_str()]))
        .await
        .unwrap();
    assert_eq!(inter.len(), 2);
    conn.execute(Del::keys([&k1, &k2])).await.unwrap();
}

// -- Sorted set command tests --

#[tokio::test]
async fn zadd_zscore_zcard() {
    let mut conn = conn().await;
    let k = key("zadd_zscore", "z");
    conn.execute(Del::new(&k)).await.unwrap();
    let added = conn
        .execute(
            ZAdd::new(&k)
                .member(1.0, "a")
                .member(2.0, "b")
                .member(3.0, "c"),
        )
        .await
        .unwrap();
    assert_eq!(added, 3);
    assert_eq!(conn.execute(ZCard::new(&k)).await.unwrap(), 3);
    let score = conn.execute(ZScore::new(&k, "b")).await.unwrap();
    assert_eq!(score, Some(2.0));
    let missing = conn.execute(ZScore::new(&k, "nope")).await.unwrap();
    assert_eq!(missing, None);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn zrem_zrank() {
    let mut conn = conn().await;
    let k = key("zrem_zrank", "z");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(ZAdd::new(&k).member(1.0, "a").member(2.0, "b"))
        .await
        .unwrap();
    assert_eq!(conn.execute(ZRank::new(&k, "b")).await.unwrap(), Some(1));
    conn.execute(ZRem::new(&k, "a")).await.unwrap();
    assert_eq!(conn.execute(ZRank::new(&k, "b")).await.unwrap(), Some(0));
    assert_eq!(conn.execute(ZRank::new(&k, "nope")).await.unwrap(), None);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn zrange() {
    let mut conn = conn().await;
    let k = key("zrange", "z");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(
        ZAdd::new(&k)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();
    let range = conn.execute(ZRange::new(&k, 0, -1)).await.unwrap();
    assert_eq!(
        range,
        vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
    );
    let partial = conn.execute(ZRange::new(&k, 0, 1)).await.unwrap();
    assert_eq!(partial, vec![Bytes::from("a"), Bytes::from("b")]);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn zincrby() {
    let mut conn = conn().await;
    let k = key("zincrby", "z");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(ZAdd::new(&k).member(10.0, "player"))
        .await
        .unwrap();
    let new_score = conn.execute(ZIncrBy::new(&k, 5.5, "player")).await.unwrap();
    assert!((new_score - 15.5).abs() < f64::EPSILON);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn zrangebyscore() {
    let mut conn = conn().await;
    let k = key("zrangebyscore", "z");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(
        ZAdd::new(&k)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c")
            .member(4.0, "d"),
    )
    .await
    .unwrap();
    let range = conn
        .execute(ZRangeByScore::new(&k, "2", "3"))
        .await
        .unwrap();
    assert_eq!(range, vec![Bytes::from("b"), Bytes::from("c")]);
    let all = conn
        .execute(ZRangeByScore::new(&k, "-inf", "+inf"))
        .await
        .unwrap();
    assert_eq!(all.len(), 4);
    conn.execute(Del::new(&k)).await.unwrap();
}

// -- Additional coverage tests --

// Pub/Sub: unsubscribe and multi-channel

#[tokio::test]
async fn pubsub_unsubscribe() {
    let ch1 = key("pubsub_unsub", "ch1");
    let ch2 = key("pubsub_unsub", "ch2");

    let sub_conn = conn().await;
    let mut pubsub = PubSubConnection::from_connection(sub_conn).unwrap();
    pubsub.subscribe(&[&ch1, &ch2]).await.unwrap();
    pubsub.unsubscribe(&[&ch1]).await.unwrap();

    // Publish to ch1 (unsubscribed) and ch2 (still subscribed).
    let mut pub_conn = conn().await;
    use redis_tower_protocol::helpers::{array, bulk};
    pub_conn
        .execute_pipeline(vec![array(vec![
            bulk("PUBLISH"),
            bulk(ch2.as_str()),
            bulk("hello"),
        ])])
        .await
        .unwrap();

    let msg = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("parse error");
    assert_eq!(msg.channel, ch2);
}

#[tokio::test]
async fn pubsub_punsubscribe() {
    let prefix = key("pubsub_punsub", "");
    let pat = format!("{prefix}*");
    let channel = format!("{prefix}events");

    let sub_conn = conn().await;
    let mut pubsub = PubSubConnection::from_connection(sub_conn).unwrap();
    pubsub.psubscribe(&[&pat]).await.unwrap();
    pubsub.punsubscribe(&[&pat]).await.unwrap();

    // After punsubscribe, publishing should not deliver.
    let mut pub_conn = conn().await;
    use redis_tower_protocol::helpers::{array, bulk};
    pub_conn
        .execute_pipeline(vec![array(vec![
            bulk("PUBLISH"),
            bulk(channel.as_str()),
            bulk("should not arrive"),
        ])])
        .await
        .unwrap();

    let result = tokio::time::timeout(std::time::Duration::from_millis(200), pubsub.next()).await;
    assert!(result.is_err(), "should timeout -- no messages expected");
}

// Server: FlushDb

/// Get a connection on DB 1 (isolated for destructive tests like FLUSHDB).
async fn conn_db1() -> RedisConnection {
    let addr = redis_addr().await;
    let url = format!("redis://{addr}/1");
    RedisConnection::connect_url(&url).await.unwrap()
}

#[tokio::test]
async fn flushdb() {
    let mut conn = conn_db1().await;
    let k = key("flushdb", "k");
    conn.execute(Set::new(&k, "x")).await.unwrap();
    conn.execute(FlushDb::new()).await.unwrap();
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn flushdb_sync_mode() {
    let mut conn = conn_db1().await;
    let k = key("flushdb_sync", "k");
    conn.execute(Set::new(&k, "x")).await.unwrap();
    conn.execute(FlushDb::new().sync_mode()).await.unwrap();
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, None);
}

// Pipeline: utility methods and error paths

#[tokio::test]
async fn pipeline_len_and_empty() {
    let p = Pipeline::new();
    assert!(p.is_empty());
    assert_eq!(p.len(), 0);
    let p = p.push(Ping::new()).push(Ping::new());
    assert!(!p.is_empty());
    assert_eq!(p.len(), 2);
}

#[tokio::test]
async fn pipeline_type_mismatch() {
    let mut conn = conn().await;
    let k = key("pipe_mismatch", "k");
    conn.execute(Set::new(&k, "hello")).await.unwrap();

    let results = Pipeline::new()
        .push(Get::new(&k))
        .execute(&mut conn)
        .await
        .unwrap();

    // Try to get as wrong type.
    let err = results.get::<i64>(0);
    assert!(err.is_err());

    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn pipeline_take_twice() {
    let mut conn = conn().await;
    let k = key("pipe_take2", "k");
    conn.execute(Set::new(&k, "val")).await.unwrap();

    let mut results = Pipeline::new()
        .push(Get::new(&k))
        .execute(&mut conn)
        .await
        .unwrap();

    let _first: Option<Bytes> = results.take(0).unwrap();
    let second = results.take::<Option<Bytes>>(0);
    assert!(second.is_err(), "double take should fail");

    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn pipeline_out_of_bounds() {
    let mut conn = conn().await;
    let results = Pipeline::new()
        .push(Ping::new())
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(results.get::<String>(99).is_err());
}

// Transaction: utility methods

#[tokio::test]
async fn transaction_len_and_empty() {
    let t = Transaction::new();
    assert!(t.is_empty());
    assert_eq!(t.len(), 0);
    let t = t.push(Ping::new());
    assert_eq!(t.len(), 1);
}

// Client: connect_url

#[tokio::test]
async fn client_connect_url() {
    let addr = redis_addr().await;
    let url = format!("redis://{addr}");
    let client = RedisClient::connect_url(&url).await.unwrap();
    let pong = client.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

// Strings: Set with PX and XX

#[tokio::test]
async fn set_with_px() {
    let mut conn = conn().await;
    let k = key("set_px", "k");
    conn.execute(Set::new(&k, "value").px(10000)).await.unwrap();
    let ttl = conn.execute(Ttl::new(&k)).await.unwrap();
    assert!(ttl > 0 && ttl <= 10);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn set_xx_succeeds_when_exists() {
    let mut conn = conn().await;
    let k = key("set_xx_ok", "k");
    conn.execute(Set::new(&k, "old")).await.unwrap();
    conn.execute(Set::new(&k, "new").xx()).await.unwrap();
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("new")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn set_xx_fails_when_missing() {
    let mut conn = conn().await;
    let k = key("set_xx_fail", "k");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(Set::new(&k, "value").xx()).await.unwrap();
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, None);
}

// Lists: multi-element push, pop on empty

#[tokio::test]
async fn lpush_multiple() {
    let mut conn = conn().await;
    let k = key("lpush_multi", "l");
    conn.execute(Del::new(&k)).await.unwrap();
    let len = conn
        .execute(LPush::elements(&k, ["a", "b", "c"]))
        .await
        .unwrap();
    assert_eq!(len, 3);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn lpop_empty() {
    let mut conn = conn().await;
    let k = key("lpop_empty", "l");
    conn.execute(Del::new(&k)).await.unwrap();
    let val = conn.execute(LPop::new(&k)).await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn rpop_empty() {
    let mut conn = conn().await;
    let k = key("rpop_empty", "l");
    conn.execute(Del::new(&k)).await.unwrap();
    let val = conn.execute(RPop::new(&k)).await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn lindex_out_of_range() {
    let mut conn = conn().await;
    let k = key("lindex_oor", "l");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(RPush::new(&k, "a")).await.unwrap();
    let val = conn.execute(LIndex::new(&k, 99)).await.unwrap();
    assert_eq!(val, None);
    conn.execute(Del::new(&k)).await.unwrap();
}

// Keys: single exists

#[tokio::test]
async fn exists_single() {
    let mut conn = conn().await;
    let k = key("exists_single", "k");
    conn.execute(Set::new(&k, "x")).await.unwrap();
    assert_eq!(conn.execute(Exists::new(&k)).await.unwrap(), 1);
    conn.execute(Del::new(&k)).await.unwrap();
    assert_eq!(conn.execute(Exists::new(&k)).await.unwrap(), 0);
}

// Hashes: single field HDel

#[tokio::test]
async fn hdel_single() {
    let mut conn = conn().await;
    let k = key("hdel_single", "h");
    conn.execute(HSet::new(&k, "f", "v")).await.unwrap();
    let removed = conn.execute(HDel::new(&k, "f")).await.unwrap();
    assert_eq!(removed, 1);
    conn.execute(Del::new(&k)).await.unwrap();
}

// Hashes: empty HGetAll

#[tokio::test]
async fn hgetall_empty() {
    let mut conn = conn().await;
    let k = key("hgetall_empty", "h");
    conn.execute(Del::new(&k)).await.unwrap();
    let pairs = conn.execute(HGetAll::new(&k)).await.unwrap();
    assert!(pairs.is_empty());
}

// Sets: single SRem, multi SRem

#[tokio::test]
async fn srem_multiple() {
    let mut conn = conn().await;
    let k = key("srem_multi", "s");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(SAdd::members(&k, ["a", "b", "c"]))
        .await
        .unwrap();
    let removed = conn.execute(SRem::members(&k, ["a", "b"])).await.unwrap();
    assert_eq!(removed, 2);
    conn.execute(Del::new(&k)).await.unwrap();
}

// Sorted sets: multi ZRem

#[tokio::test]
async fn zrem_multiple() {
    let mut conn = conn().await;
    let k = key("zrem_multi", "z");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(
        ZAdd::new(&k)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();
    let removed = conn.execute(ZRem::members(&k, ["a", "c"])).await.unwrap();
    assert_eq!(removed, 2);
    assert_eq!(conn.execute(ZCard::new(&k)).await.unwrap(), 1);
    conn.execute(Del::new(&k)).await.unwrap();
}

// Connection: Service::poll_ready

#[tokio::test]
async fn service_poll_ready() {
    use std::task::Poll;
    let mut conn = conn().await;
    let waker = futures::task::noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    let ready = <RedisConnection as Service<Ping>>::poll_ready(&mut conn, &mut cx);
    assert!(matches!(ready, Poll::Ready(Ok(()))));
}

// Transaction: error in queued command

#[tokio::test]
async fn transaction_with_redis_error() {
    let mut conn = conn().await;
    let k = key("txn_err", "k");
    conn.execute(Set::new(&k, "not_a_number")).await.unwrap();

    let result = Transaction::new()
        .push(Incr::new(&k)) // will fail inside EXEC
        .push(Ping::new())
        .execute(&mut conn)
        .await
        .unwrap();

    match result {
        TransactionResult::Committed(results) => {
            // Incr on a non-number returns error inside the transaction results.
            assert!(results.get::<i64>(0).is_err());
            // Ping still succeeds.
            let pong: &String = results.get(1).unwrap();
            assert_eq!(pong, "PONG");
        }
        TransactionResult::Aborted => panic!("should not abort"),
    }

    conn.execute(Del::new(&k)).await.unwrap();
}

// -- New command tests (APPEND, MSET, RENAME, TYPE, DBSIZE, SELECT, AUTH, LMOVE) --

#[tokio::test]
async fn append() {
    let mut conn = conn().await;
    let k = key("append", "k");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(Set::new(&k, "hello")).await.unwrap();
    let len = conn.execute(Append::new(&k, " world")).await.unwrap();
    assert_eq!(len, 11);
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("hello world")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn append_creates_key() {
    let mut conn = conn().await;
    let k = key("append_create", "k");
    conn.execute(Del::new(&k)).await.unwrap();
    let len = conn.execute(Append::new(&k, "new")).await.unwrap();
    assert_eq!(len, 3);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn mset() {
    let mut conn = conn().await;
    let k1 = key("mset", "a");
    let k2 = key("mset", "b");
    conn.execute(MSet::new([(k1.as_str(), "1"), (k2.as_str(), "2")]))
        .await
        .unwrap();
    let vals = conn
        .execute(MGet::new([k1.as_str(), k2.as_str()]))
        .await
        .unwrap();
    assert_eq!(vals[0], Some(Bytes::from("1")));
    assert_eq!(vals[1], Some(Bytes::from("2")));
    conn.execute(Del::keys([&k1, &k2])).await.unwrap();
}

#[tokio::test]
async fn rename() {
    let mut conn = conn().await;
    let k1 = key("rename", "old");
    let k2 = key("rename", "new");
    conn.execute(Del::keys([&k1, &k2])).await.unwrap();
    conn.execute(Set::new(&k1, "value")).await.unwrap();
    conn.execute(Rename::new(&k1, &k2)).await.unwrap();
    assert_eq!(conn.execute(Get::new(&k1)).await.unwrap(), None);
    assert_eq!(
        conn.execute(Get::new(&k2)).await.unwrap(),
        Some(Bytes::from("value"))
    );
    conn.execute(Del::new(&k2)).await.unwrap();
}

#[tokio::test]
async fn type_command() {
    let mut conn = conn().await;
    let ks = key("type", "string");
    let kl = key("type", "list");
    let kh = key("type", "hash");
    let km = key("type", "missing");
    conn.execute(Del::keys([&ks, &kl, &kh])).await.unwrap();
    conn.execute(Set::new(&ks, "val")).await.unwrap();
    conn.execute(RPush::new(&kl, "a")).await.unwrap();
    conn.execute(HSet::new(&kh, "f", "v")).await.unwrap();
    assert_eq!(conn.execute(Type::new(&ks)).await.unwrap(), "string");
    assert_eq!(conn.execute(Type::new(&kl)).await.unwrap(), "list");
    assert_eq!(conn.execute(Type::new(&kh)).await.unwrap(), "hash");
    assert_eq!(conn.execute(Type::new(&km)).await.unwrap(), "none");
    conn.execute(Del::keys([&ks, &kl, &kh])).await.unwrap();
}

#[tokio::test]
async fn dbsize() {
    let mut conn = conn().await;
    let size = conn.execute(DbSize::new()).await.unwrap();
    assert!(size >= 0, "DBSIZE should return non-negative");
    // Don't compare before/after -- parallel tests can change the count.
}

#[tokio::test]
async fn select_db() {
    let mut conn = conn().await;
    conn.execute(Select::new(2)).await.unwrap();
    conn.execute(Set::new("select_test", "val")).await.unwrap();
    conn.execute(Select::new(0)).await.unwrap();
    // Clean up DB 2.
    conn.execute(Select::new(2)).await.unwrap();
    conn.execute(Del::new("select_test")).await.unwrap();
}

#[tokio::test]
async fn lmove() {
    let mut conn = conn().await;
    let src = key("lmove", "src");
    let dst = key("lmove", "dst");
    conn.execute(Del::keys([&src, &dst])).await.unwrap();
    conn.execute(RPush::elements(&src, ["a", "b", "c"]))
        .await
        .unwrap();
    let moved = conn
        .execute(LMove::new(
            &src,
            &dst,
            ListDirection::Left,
            ListDirection::Right,
        ))
        .await
        .unwrap();
    assert_eq!(moved, Some(Bytes::from("a")));
    let src_items = conn.execute(LRange::new(&src, 0, -1)).await.unwrap();
    assert_eq!(src_items, vec![Bytes::from("b"), Bytes::from("c")]);
    let dst_items = conn.execute(LRange::new(&dst, 0, -1)).await.unwrap();
    assert_eq!(dst_items, vec![Bytes::from("a")]);
    conn.execute(Del::keys([&src, &dst])).await.unwrap();
}

// -- Resilient connection tests --

#[tokio::test]
async fn resilient_connection_basic() {
    let addr = redis_addr().await;
    let mut conn =
        ResilientConnection::new(AddrConnectionFactory::new(addr), ReconnectConfig::default())
            .await
            .unwrap();

    let k = key("resilient_basic", "k");
    conn.execute(Set::new(&k, "hello")).await.unwrap();
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("hello")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn resilient_connection_with_callbacks() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let addr = redis_addr().await;
    let connected = Arc::new(AtomicBool::new(false));
    let connected_clone = Arc::clone(&connected);

    let mut conn =
        ResilientConnection::new(AddrConnectionFactory::new(addr), ReconnectConfig::default())
            .await
            .unwrap()
            .on_connect(move || {
                connected_clone.store(true, Ordering::Release);
            });

    let pong = conn.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

#[tokio::test]
async fn resilient_connection_max_retries() {
    // Try to connect to a port where nothing is listening.
    let result = ResilientConnection::new(
        AddrConnectionFactory::new("127.0.0.1:1"),
        ReconnectConfig::default().max_retries(0),
    )
    .await;
    assert!(result.is_err());
}

// -- ResilientRedisClient tests --

#[tokio::test]
async fn resilient_client_basic() {
    let addr = redis_addr().await;
    let client = ResilientRedisClient::connect(addr).await.unwrap();

    let k = key("resilient_client", "k");
    client.execute(Set::new(&k, "val")).await.unwrap();
    let val = client.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("val")));
    client.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn resilient_client_shared_across_tasks() {
    let addr = redis_addr().await;
    let client = ResilientRedisClient::connect(addr).await.unwrap();
    let k1 = key("resilient_shared", "t1");
    let k2 = key("resilient_shared", "t2");

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

#[tokio::test]
async fn resilient_client_connect_url() {
    let addr = redis_addr().await;
    let url = format!("redis://{addr}");
    let client = ResilientRedisClient::connect_url(&url).await.unwrap();
    let pong = client.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

// -- Client-side caching tests --

#[tokio::test]
async fn csc_cache_hit() {
    let addr = redis_addr().await;
    let mut client = redis_tower::CachedClient::connect(addr).await.unwrap();

    let k = "csc_test:cache_hit";
    // Write via a separate connection (bypasses cache).
    let mut writer = conn().await;
    writer.execute(Set::new(k, "hello")).await.unwrap();

    // First read: cache miss, hits Redis.
    let v1: Option<Bytes> = client.execute(Get::new(k)).await.unwrap();
    assert_eq!(v1, Some(Bytes::from("hello")));
    assert_eq!(client.cache_size().await, 1);

    // Second read: cache hit, no network.
    let v2: Option<Bytes> = client.execute(Get::new(k)).await.unwrap();
    assert_eq!(v2, Some(Bytes::from("hello")));

    // Cleanup.
    writer.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn csc_invalidation() {
    let addr = redis_addr().await;
    let mut client = redis_tower::CachedClient::connect(addr).await.unwrap();

    let k = "csc_test:invalidation";

    // Write and read to populate cache.
    let mut writer = conn().await;
    writer.execute(Set::new(k, "original")).await.unwrap();
    let _: Option<Bytes> = client.execute(Get::new(k)).await.unwrap();
    assert_eq!(client.cache_size().await, 1);

    // Modify the key from another connection.
    writer.execute(Set::new(k, "modified")).await.unwrap();

    // The invalidation push arrives during the next read from the connection.
    // The GET below will trigger read_response which routes any push frames.
    // After the GET, the cache will have the new value (and the old entry
    // will have been invalidated by the push that arrived before/during the read).
    //
    // We need a small delay to let the server send the invalidation.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // This GET triggers the read that routes the invalidation push,
    // then sees a cache miss and fetches the new value.
    let v: Option<Bytes> = client.execute(Get::new(k)).await.unwrap();
    assert_eq!(v, Some(Bytes::from("modified")));

    writer.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn csc_write_not_cached() {
    let addr = redis_addr().await;
    let mut client = redis_tower::CachedClient::connect(addr).await.unwrap();

    let k = "csc_test:write_not_cached";
    // SET should not be cached.
    client.execute(Set::new(k, "val")).await.unwrap();
    assert_eq!(client.cache_size().await, 0);

    client.execute(Del::new(k)).await.unwrap();
}

// -- Tower layer CSC tests --

#[tokio::test]
async fn tower_csc_cache_hit() {
    use redis_tower::FrameService;
    use redis_tower::cache_layer::{CacheConfig, CacheService};
    use redis_tower::command_adapter::CommandAdapter;

    let addr = redis_addr().await;
    let frame_svc = FrameService::connect(addr).await.unwrap();
    let cache_svc = CacheService::new(frame_svc, CacheConfig::default());
    let mut svc = CommandAdapter::new(cache_svc);

    let k = "tower_csc:cache_hit";
    // Write directly.
    let mut writer = conn().await;
    writer.execute(Set::new(k, "hello")).await.unwrap();

    // First read: cache miss.
    let v1: Option<Bytes> = call_ready(&mut svc, Get::new(k)).await.unwrap();
    assert_eq!(v1, Some(Bytes::from("hello")));
    assert_eq!(svc.inner_mut().cache_size().await, 1);

    // Second read: cache hit.
    let v2: Option<Bytes> = call_ready(&mut svc, Get::new(k)).await.unwrap();
    assert_eq!(v2, Some(Bytes::from("hello")));

    writer.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn tower_csc_write_bypasses_cache() {
    use redis_tower::FrameService;
    use redis_tower::cache_layer::{CacheConfig, CacheService};
    use redis_tower::command_adapter::CommandAdapter;

    let addr = redis_addr().await;
    let frame_svc = FrameService::connect(addr).await.unwrap();
    let cache_svc = CacheService::new(frame_svc, CacheConfig::default());
    let mut svc = CommandAdapter::new(cache_svc);

    let k = "tower_csc:write_bypass";
    call_ready(&mut svc, Set::new(k, "val")).await.unwrap();
    assert_eq!(svc.inner_mut().cache_size().await, 0);

    call_ready(&mut svc, Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn tower_csc_with_invalidation() {
    use futures::StreamExt;
    use redis_tower::FrameService;
    use redis_tower::cache_layer::{CacheConfig, CacheService, spawn_invalidation_task};
    use redis_tower::command_adapter::CommandAdapter;
    use redis_tower::commands::ClientTracking;

    let addr = redis_addr().await;

    // Data connection as FrameService.
    let frame_svc = FrameService::connect(addr).await.unwrap();

    // Tracking connection for invalidation pushes.
    let mut tracking_conn = RedisConnection::connect_resp3(addr).await.unwrap();
    tracking_conn
        .execute(ClientTracking::on().bcast())
        .await
        .unwrap();
    let tracking_framed = tracking_conn.into_framed().unwrap();
    let (_sink, stream) = tracking_framed.split();

    // Build the cached service with shared cache.
    let cache_svc = CacheService::new(frame_svc, CacheConfig::default());
    let cache_ref = cache_svc.cache().clone();
    let mut svc = CommandAdapter::new(cache_svc);

    // Wire up invalidation.
    let _task = spawn_invalidation_task(cache_ref.clone(), stream);

    let k = "tower_csc:invalidation";
    let mut writer = conn().await;
    writer.execute(Set::new(k, "original")).await.unwrap();

    // Populate cache.
    let _: Option<Bytes> = call_ready(&mut svc, Get::new(k)).await.unwrap();
    assert_eq!(cache_ref.read().await.len(), 1);

    // Modify from another connection.
    writer.execute(Set::new(k, "modified")).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Cache should be invalidated.
    assert_eq!(cache_ref.read().await.len(), 0);

    // Fresh read gets new value.
    let v: Option<Bytes> = call_ready(&mut svc, Get::new(k)).await.unwrap();
    assert_eq!(v, Some(Bytes::from("modified")));

    writer.execute(Del::new(k)).await.unwrap();
}

// -- Streams tests --

#[tokio::test]
async fn streams_xadd_xlen_xrange() {
    let mut conn = conn().await;
    let k = "streams_test:basic";
    conn.execute(Del::new(k)).await.unwrap();

    let id1 = conn
        .execute(XAdd::new(k).field("temp", "22").field("humidity", "65"))
        .await
        .unwrap();
    assert!(!id1.is_empty());

    let id2 = conn
        .execute(XAdd::new(k).field("temp", "23"))
        .await
        .unwrap();
    assert!(id2 > id1);

    let len = conn.execute(XLen::new(k)).await.unwrap();
    assert_eq!(len, 2);

    let entries = conn.execute(XRange::all(k)).await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].id, id1);
    assert_eq!(entries[0].fields.len(), 2);
    assert_eq!(entries[0].fields[0].0, "temp");

    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xrevrange() {
    let mut conn = conn().await;
    let k = "streams_test:revrange";
    conn.execute(Del::new(k)).await.unwrap();

    conn.execute(XAdd::new(k).field("a", "1")).await.unwrap();
    conn.execute(XAdd::new(k).field("b", "2")).await.unwrap();

    let entries = conn.execute(XRevRange::all(k)).await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].fields[0].0, "b"); // newest first

    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xdel_xtrim() {
    let mut conn = conn().await;
    let k = "streams_test:del_trim";
    conn.execute(Del::new(k)).await.unwrap();

    let id = conn.execute(XAdd::new(k).field("x", "1")).await.unwrap();
    conn.execute(XAdd::new(k).field("y", "2")).await.unwrap();
    conn.execute(XAdd::new(k).field("z", "3")).await.unwrap();

    let deleted = conn.execute(XDel::new(k, &id)).await.unwrap();
    assert_eq!(deleted, 1);

    let trimmed = conn.execute(XTrim::maxlen(k, 1)).await.unwrap();
    assert!(trimmed >= 1);

    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xadd_maxlen() {
    let mut conn = conn().await;
    let k = "streams_test:maxlen";
    conn.execute(Del::new(k)).await.unwrap();

    for i in 0..10 {
        conn.execute(XAdd::new(k).maxlen(5).field("n", i.to_string()))
            .await
            .unwrap();
    }

    let len = conn.execute(XLen::new(k)).await.unwrap();
    assert_eq!(len, 5);

    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_consumer_groups() {
    let mut conn = conn().await;
    let k = "streams_test:groups";
    conn.execute(Del::new(k)).await.unwrap();

    // Add some entries first.
    conn.execute(XAdd::new(k).field("msg", "hello"))
        .await
        .unwrap();
    conn.execute(XAdd::new(k).field("msg", "world"))
        .await
        .unwrap();

    // Create consumer group starting from beginning.
    conn.execute(XGroupCreate::new(k, "mygroup", "0"))
        .await
        .unwrap();

    // Read as consumer.
    let result = conn
        .execute(XReadGroup::new("mygroup", "consumer1", k).count(10))
        .await
        .unwrap();
    assert_eq!(result.len(), 1); // one stream
    assert_eq!(result[0].1.len(), 2); // two entries

    // Ack the entries.
    let entry_id = result[0].1[0].id.clone();
    let acked = conn
        .execute(XAck::new(k, "mygroup", &entry_id))
        .await
        .unwrap();
    assert_eq!(acked, 1);

    // Destroy the group.
    conn.execute(XGroupDestroy::new(k, "mygroup"))
        .await
        .unwrap();
    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xread() {
    let mut conn = conn().await;
    let k = "streams_test:xread";
    conn.execute(Del::new(k)).await.unwrap();

    conn.execute(XAdd::new(k).field("a", "1")).await.unwrap();
    conn.execute(XAdd::new(k).field("b", "2")).await.unwrap();

    let result = conn.execute(XRead::new(k, "0-0").count(10)).await.unwrap();
    assert!(result.is_some());
    let streams = result.unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].1.len(), 2);

    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xrange_count() {
    let mut conn = conn().await;
    let k = "streams_test:range_count";
    conn.execute(Del::new(k)).await.unwrap();

    for i in 0..5 {
        conn.execute(XAdd::new(k).field("n", i.to_string()))
            .await
            .unwrap();
    }

    let entries = conn.execute(XRange::all(k).count(2)).await.unwrap();
    assert_eq!(entries.len(), 2);

    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xgroup_setid() {
    let mut conn = conn().await;
    let k = "streams_test:xgroup_setid";
    conn.execute(Del::new(k)).await.unwrap();

    conn.execute(XAdd::new(k).field("a", "1")).await.unwrap();
    conn.execute(XGroupCreate::new(k, "g1", "0")).await.unwrap();

    // Set the group's last-delivered ID to "$" (newest).
    conn.execute(XGroupSetId::new(k, "g1", "$")).await.unwrap();

    // Reading should return nothing since we've caught up.
    let result = conn
        .execute(XReadGroup::new("g1", "c1", k).count(10))
        .await
        .unwrap();
    assert!(result.is_empty() || result[0].1.is_empty());

    conn.execute(XGroupDestroy::new(k, "g1")).await.unwrap();
    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xgroup_createconsumer_delconsumer() {
    let mut conn = conn().await;
    let k = "streams_test:xgroup_consumer";
    conn.execute(Del::new(k)).await.unwrap();

    conn.execute(XAdd::new(k).field("a", "1")).await.unwrap();
    conn.execute(XGroupCreate::new(k, "g1", "0")).await.unwrap();

    let created = conn
        .execute(XGroupCreateConsumer::new(k, "g1", "mycons"))
        .await
        .unwrap();
    assert_eq!(created, 1);

    // Creating again returns 0.
    let created2 = conn
        .execute(XGroupCreateConsumer::new(k, "g1", "mycons"))
        .await
        .unwrap();
    assert_eq!(created2, 0);

    let pending = conn
        .execute(XGroupDelConsumer::new(k, "g1", "mycons"))
        .await
        .unwrap();
    assert_eq!(pending, 0); // no pending entries

    conn.execute(XGroupDestroy::new(k, "g1")).await.unwrap();
    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xclaim() {
    let mut conn = conn().await;
    let k = "streams_test:xclaim";
    conn.execute(Del::new(k)).await.unwrap();

    let id = conn.execute(XAdd::new(k).field("a", "1")).await.unwrap();
    conn.execute(XGroupCreate::new(k, "g1", "0")).await.unwrap();

    // Consumer c1 reads the entry.
    conn.execute(XReadGroup::new("g1", "c1", k).count(10))
        .await
        .unwrap();

    // Consumer c2 claims it with min-idle-time 0.
    let claimed = conn
        .execute(XClaim::new(k, "g1", "c2", 0, [&id]))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, id);

    conn.execute(XGroupDestroy::new(k, "g1")).await.unwrap();
    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xautoclaim() {
    let mut conn = conn().await;
    let k = "streams_test:xautoclaim";
    conn.execute(Del::new(k)).await.unwrap();

    let id = conn.execute(XAdd::new(k).field("a", "1")).await.unwrap();
    conn.execute(XGroupCreate::new(k, "g1", "0")).await.unwrap();

    // Consumer c1 reads the entry.
    conn.execute(XReadGroup::new("g1", "c1", k).count(10))
        .await
        .unwrap();

    // Consumer c2 auto-claims with min-idle-time 0.
    let result = conn
        .execute(XAutoClaim::new(k, "g1", "c2", 0, "0-0"))
        .await
        .unwrap();
    assert_eq!(result.entries.len(), 1);
    assert_eq!(result.entries[0].id, id);

    conn.execute(XGroupDestroy::new(k, "g1")).await.unwrap();
    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xpending_summary() {
    let mut conn = conn().await;
    let k = "streams_test:xpending_sum";
    conn.execute(Del::new(k)).await.unwrap();

    conn.execute(XAdd::new(k).field("a", "1")).await.unwrap();
    conn.execute(XAdd::new(k).field("b", "2")).await.unwrap();
    conn.execute(XGroupCreate::new(k, "g1", "0")).await.unwrap();

    // Read as consumer to create pending entries.
    conn.execute(XReadGroup::new("g1", "c1", k).count(10))
        .await
        .unwrap();

    let summary = conn.execute(XPendingSummary::new(k, "g1")).await.unwrap();
    assert_eq!(summary.count, 2);
    assert!(summary.min_id.is_some());
    assert!(summary.max_id.is_some());
    assert_eq!(summary.consumers.len(), 1);
    assert_eq!(summary.consumers[0].0, "c1");
    assert_eq!(summary.consumers[0].1, 2);

    conn.execute(XGroupDestroy::new(k, "g1")).await.unwrap();
    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xpending_range() {
    let mut conn = conn().await;
    let k = "streams_test:xpending_range";
    conn.execute(Del::new(k)).await.unwrap();

    conn.execute(XAdd::new(k).field("a", "1")).await.unwrap();
    conn.execute(XGroupCreate::new(k, "g1", "0")).await.unwrap();
    conn.execute(XReadGroup::new("g1", "c1", k).count(10))
        .await
        .unwrap();

    let entries = conn
        .execute(XPendingRange::new(k, "g1", "-", "+", 10))
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].consumer, "c1");
    assert_eq!(entries[0].delivery_count, 1);

    conn.execute(XGroupDestroy::new(k, "g1")).await.unwrap();
    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xinfo_stream() {
    let mut conn = conn().await;
    let k = "streams_test:xinfo_stream";
    conn.execute(Del::new(k)).await.unwrap();

    conn.execute(XAdd::new(k).field("a", "1")).await.unwrap();
    conn.execute(XAdd::new(k).field("b", "2")).await.unwrap();

    let info = conn.execute(XInfoStream::new(k)).await.unwrap();
    assert_eq!(info.length, 2);
    assert!(info.first_entry.is_some());
    assert!(info.last_entry.is_some());

    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xinfo_groups() {
    let mut conn = conn().await;
    let k = "streams_test:xinfo_groups";
    conn.execute(Del::new(k)).await.unwrap();

    conn.execute(XAdd::new(k).field("a", "1")).await.unwrap();
    conn.execute(XGroupCreate::new(k, "g1", "0")).await.unwrap();
    conn.execute(XGroupCreate::new(k, "g2", "0")).await.unwrap();

    let groups = conn.execute(XInfoGroups::new(k)).await.unwrap();
    assert_eq!(groups.len(), 2);

    let names: Vec<&str> = groups.iter().map(|g| g.name.as_str()).collect();
    assert!(names.contains(&"g1"));
    assert!(names.contains(&"g2"));

    conn.execute(XGroupDestroy::new(k, "g1")).await.unwrap();
    conn.execute(XGroupDestroy::new(k, "g2")).await.unwrap();
    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn streams_xinfo_consumers() {
    let mut conn = conn().await;
    let k = "streams_test:xinfo_consumers";
    conn.execute(Del::new(k)).await.unwrap();

    conn.execute(XAdd::new(k).field("a", "1")).await.unwrap();
    conn.execute(XGroupCreate::new(k, "g1", "0")).await.unwrap();

    // Read to create a consumer.
    conn.execute(XReadGroup::new("g1", "c1", k).count(10))
        .await
        .unwrap();

    let consumers = conn.execute(XInfoConsumers::new(k, "g1")).await.unwrap();
    assert_eq!(consumers.len(), 1);
    assert_eq!(consumers[0].name, "c1");
    assert_eq!(consumers[0].pending, 1);

    conn.execute(XGroupDestroy::new(k, "g1")).await.unwrap();
    conn.execute(Del::new(k)).await.unwrap();
}

// -- SCAN tests --

#[tokio::test]
async fn scan_basic() {
    let mut conn = conn().await;
    let prefix = "scan_test:basic";
    // Create some keys.
    for i in 0..5 {
        conn.execute(Set::new(format!("{prefix}:{i}"), "v"))
            .await
            .unwrap();
    }

    // Scan with pattern.
    let mut all_keys = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let result = conn
            .execute(
                Scan::new()
                    .cursor(&cursor)
                    .match_pattern(format!("{prefix}:*"))
                    .count(2),
            )
            .await
            .unwrap();
        let finished = result.is_finished();
        all_keys.extend(result.results);
        if finished {
            break;
        }
        cursor = result.cursor;
    }
    assert_eq!(all_keys.len(), 5);

    // Cleanup.
    for i in 0..5 {
        conn.execute(Del::new(format!("{prefix}:{i}")))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn sscan_basic() {
    let mut conn = conn().await;
    let k = "scan_test:sscan";
    conn.execute(Del::new(k)).await.unwrap();
    conn.execute(SAdd::members(k, ["a", "b", "c", "d", "e"]))
        .await
        .unwrap();

    let mut all_members = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let result = conn.execute(SScan::new(k).cursor(&cursor)).await.unwrap();
        let finished = result.is_finished();
        all_members.extend(result.results);
        if finished {
            break;
        }
        cursor = result.cursor;
    }
    assert_eq!(all_members.len(), 5);

    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn hscan_basic() {
    let mut conn = conn().await;
    let k = "scan_test:hscan";
    conn.execute(Del::new(k)).await.unwrap();
    conn.execute(HSet::new(k, "f1", "v1").field("f2", "v2").field("f3", "v3"))
        .await
        .unwrap();

    let mut all_pairs = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let result = conn.execute(HScan::new(k).cursor(&cursor)).await.unwrap();
        let finished = result.is_finished();
        all_pairs.extend(result.results);
        if finished {
            break;
        }
        cursor = result.cursor;
    }
    assert_eq!(all_pairs.len(), 3);

    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn zscan_basic() {
    let mut conn = conn().await;
    let k = "scan_test:zscan";
    conn.execute(Del::new(k)).await.unwrap();
    conn.execute(
        ZAdd::new(k)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let mut all_pairs = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let result = conn.execute(ZScan::new(k).cursor(&cursor)).await.unwrap();
        let finished = result.is_finished();
        all_pairs.extend(result.results);
        if finished {
            break;
        }
        cursor = result.cursor;
    }
    assert_eq!(all_pairs.len(), 3);
    // Verify scores.
    assert!(
        (all_pairs
            .iter()
            .find(|(m, _)| m == &Bytes::from("b"))
            .unwrap()
            .1
            - 2.0)
            .abs()
            < f64::EPSILON
    );

    conn.execute(Del::new(k)).await.unwrap();
}

// -- Blocking command tests --

#[tokio::test]
async fn blpop_with_data() {
    let mut conn = conn().await;
    let k = "blocking_test:blpop";
    conn.execute(Del::new(k)).await.unwrap();
    conn.execute(RPush::elements(k, ["a", "b"])).await.unwrap();

    let result = conn.execute(BLPop::new(k, 1.0)).await.unwrap();
    assert_eq!(result, Some((Bytes::from(k), Bytes::from("a"))));

    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn blpop_timeout() {
    let mut conn = conn().await;
    let k = "blocking_test:blpop_timeout";
    conn.execute(Del::new(k)).await.unwrap();

    // Should timeout and return None.
    let result = conn.execute(BLPop::new(k, 0.1)).await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn brpop_with_data() {
    let mut conn = conn().await;
    let k = "blocking_test:brpop";
    conn.execute(Del::new(k)).await.unwrap();
    conn.execute(RPush::elements(k, ["a", "b"])).await.unwrap();

    let result = conn.execute(BRPop::new(k, 1.0)).await.unwrap();
    assert_eq!(result, Some((Bytes::from(k), Bytes::from("b"))));

    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn bzpopmin_with_data() {
    let mut conn = conn().await;
    let k = "blocking_test:bzpopmin";
    conn.execute(Del::new(k)).await.unwrap();
    conn.execute(ZAdd::new(k).member(1.0, "a").member(2.0, "b"))
        .await
        .unwrap();

    let result = conn.execute(BZPopMin::new(k, 1.0)).await.unwrap();
    let (key, member, score) = result.unwrap();
    assert_eq!(key, Bytes::from(k));
    assert_eq!(member, Bytes::from("a"));
    assert!((score - 1.0).abs() < f64::EPSILON);

    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn bzpopmax_with_data() {
    let mut conn = conn().await;
    let k = "blocking_test:bzpopmax";
    conn.execute(Del::new(k)).await.unwrap();
    conn.execute(ZAdd::new(k).member(1.0, "a").member(2.0, "b"))
        .await
        .unwrap();

    let result = conn.execute(BZPopMax::new(k, 1.0)).await.unwrap();
    let (key, member, score) = result.unwrap();
    assert_eq!(key, Bytes::from(k));
    assert_eq!(member, Bytes::from("b"));
    assert!((score - 2.0).abs() < f64::EPSILON);

    conn.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn blmove_left_left() {
    let mut conn = conn().await;
    let src = "blocking_test:blmove_src";
    let dst = "blocking_test:blmove_dst";
    conn.execute(Del::new(src)).await.unwrap();
    conn.execute(Del::new(dst)).await.unwrap();
    conn.execute(RPush::elements(src, ["x", "y"]))
        .await
        .unwrap();

    let result = conn
        .execute(BLMove::new(src, dst, ListDir::Left, ListDir::Left, 1.0))
        .await
        .unwrap();
    assert_eq!(result, Some(Bytes::from("x")));

    // Source should have one element remaining.
    let src_len: i64 = conn.execute(LLen::new(src)).await.unwrap();
    assert_eq!(src_len, 1);

    // Destination should contain the moved element.
    let dst_items: Vec<Bytes> = conn.execute(LRange::new(dst, 0, -1)).await.unwrap();
    assert_eq!(dst_items, vec![Bytes::from("x")]);

    conn.execute(Del::new(src)).await.unwrap();
    conn.execute(Del::new(dst)).await.unwrap();
}

#[tokio::test]
async fn blmove_timeout() {
    let mut conn = conn().await;
    let src = "blocking_test:blmove_timeout_src";
    let dst = "blocking_test:blmove_timeout_dst";
    conn.execute(Del::new(src)).await.unwrap();
    conn.execute(Del::new(dst)).await.unwrap();

    // Source is empty -- should time out and return None.
    let result = conn
        .execute(BLMove::new(src, dst, ListDir::Left, ListDir::Left, 0.1))
        .await
        .unwrap();
    assert_eq!(result, None);
}

// -- Reconnection / resilience tests (#154) --

#[tokio::test]
async fn resilient_connection_service_call() {
    // Test ResilientConnection via Service trait (poll_ready + call)
    let addr = redis_addr().await;
    let mut conn =
        ResilientConnection::new(AddrConnectionFactory::new(addr), ReconnectConfig::default())
            .await
            .unwrap();
    let k = key("resilient_svc", "k");
    conn.execute(Set::new(&k, "via_service")).await.unwrap();
    let val: Option<Bytes> = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("via_service")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn resilient_connection_url_factory() {
    // Test UrlConnectionFactory
    let addr = redis_addr().await;
    let url = format!("redis://{addr}");
    let _conn =
        ResilientConnection::new(UrlConnectionFactory::new(&url), ReconnectConfig::default())
            .await
            .unwrap();
    // just verify it connects
}

#[tokio::test]
async fn resilient_connection_custom_config() {
    // Test custom backoff config
    use std::time::Duration;
    let addr = redis_addr().await;
    let config = ReconnectConfig::default()
        .max_retries(3)
        .base_delay(Duration::from_millis(50))
        .max_delay(Duration::from_secs(1));
    let mut conn = ResilientConnection::new(AddrConnectionFactory::new(addr), config)
        .await
        .unwrap();
    let pong = conn.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

// -- Pipeline / transaction edge case tests (#159) --

#[tokio::test]
async fn pipeline_mixed_types() {
    let mut conn = conn().await;
    let k1 = key("pipe_mixed", "str");
    let k2 = key("pipe_mixed", "num");
    conn.execute(Set::new(&k1, "hello")).await.unwrap();
    conn.execute(Set::new(&k2, "0")).await.unwrap();

    let results = Pipeline::new()
        .push(Get::new(&k1))
        .push(Incr::new(&k2))
        .push(Exists::new(&k1))
        .execute(&mut conn)
        .await
        .unwrap();

    let s: &Option<Bytes> = results.get(0).unwrap();
    assert_eq!(s.as_ref().unwrap(), &Bytes::from("hello"));
    let n: &i64 = results.get(1).unwrap();
    assert_eq!(*n, 1);
    let e: &i64 = results.get(2).unwrap();
    assert_eq!(*e, 1);

    conn.execute(Del::keys([&k1, &k2])).await.unwrap();
}

#[tokio::test]
async fn pipeline_with_redis_error_partial() {
    // One command errors but others succeed
    let mut conn = conn().await;
    let k = key("pipe_err_partial", "k");
    conn.execute(Set::new(&k, "not_a_list")).await.unwrap();

    let results = Pipeline::new()
        .push(Ping::new())
        .push(LPush::new(&k, "item")) // WRONGTYPE error
        .push(Ping::new())
        .execute(&mut conn)
        .await
        .unwrap();

    // First and third succeed
    assert!(results.get::<String>(0).is_ok());
    // Second is a Redis error
    assert!(results.get::<i64>(1).is_err());
    // Third still succeeds
    assert!(results.get::<String>(2).is_ok());

    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn pipeline_large() {
    let mut conn = conn().await;
    let mut pipeline = Pipeline::new();
    for i in 0..200 {
        pipeline = pipeline.push(Set::new(format!("pipe_large:{i}"), format!("v{i}")));
    }
    let results = pipeline.execute(&mut conn).await.unwrap();
    assert_eq!(results.len(), 200);

    // Cleanup
    for i in 0..200 {
        conn.execute(Del::new(format!("pipe_large:{i}")))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn transaction_multiple_watch_keys() {
    let mut conn = conn().await;
    let k1 = key("txn_multi_watch", "k1");
    let k2 = key("txn_multi_watch", "k2");
    conn.execute(Set::new(&k1, "a")).await.unwrap();
    conn.execute(Set::new(&k2, "b")).await.unwrap();

    let result = Transaction::new()
        .watch([&k1, &k2])
        .push(Set::new(&k1, "x"))
        .push(Set::new(&k2, "y"))
        .execute(&mut conn)
        .await
        .unwrap();

    assert!(matches!(result, TransactionResult::Committed(_)));

    conn.execute(Del::keys([&k1, &k2])).await.unwrap();
}

#[tokio::test]
async fn transaction_with_error_aborts_cleanly() {
    // A command that will error inside MULTI should not leave connection dirty
    let mut conn = conn().await;
    let k = key("txn_err_clean", "k");
    conn.execute(Set::new(&k, "not_a_list")).await.unwrap();

    // This should return an error because LPUSH on a string key
    // errors during EXEC
    let _result = Transaction::new()
        .push(LPush::new(&k, "item"))
        .execute(&mut conn)
        .await;

    // Connection should still be usable
    let pong = conn.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");

    conn.execute(Del::new(&k)).await.unwrap();
}

// -- Additional CSC edge-case tests (#155) --

#[tokio::test]
async fn csc_multiple_keys_cached() {
    let addr = redis_addr().await;
    let mut client = redis_tower::CachedClient::connect(addr).await.unwrap();

    let k1 = key("csc_multi", "k1");
    let k2 = key("csc_multi", "k2");
    let mut writer = conn().await;
    writer.execute(Set::new(&k1, "v1")).await.unwrap();
    writer.execute(Set::new(&k2, "v2")).await.unwrap();

    let _: Option<Bytes> = client.execute(Get::new(&k1)).await.unwrap();
    let _: Option<Bytes> = client.execute(Get::new(&k2)).await.unwrap();
    assert_eq!(client.cache_size().await, 2);

    writer.execute(Del::keys([&k1, &k2])).await.unwrap();
    client.clear_cache().await;
}

#[tokio::test]
async fn csc_clear_cache() {
    let addr = redis_addr().await;
    let mut client = redis_tower::CachedClient::connect(addr).await.unwrap();

    let k = "csc_clear:k";
    let mut writer = conn().await;
    writer.execute(Set::new(k, "val")).await.unwrap();

    let _: Option<Bytes> = client.execute(Get::new(k)).await.unwrap();
    assert_eq!(client.cache_size().await, 1);

    client.clear_cache().await;
    assert_eq!(client.cache_size().await, 0);

    writer.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn csc_set_bypasses_cache() {
    let addr = redis_addr().await;
    let mut client = redis_tower::CachedClient::connect(addr).await.unwrap();

    let k = "csc_set_bypass:k";
    client.execute(Set::new(k, "val")).await.unwrap();
    assert_eq!(client.cache_size().await, 0);

    client.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn csc_hgetall_cached() {
    let addr = redis_addr().await;
    let mut client = redis_tower::CachedClient::connect(addr).await.unwrap();

    let k = "csc_hgetall:k";
    let mut writer = conn().await;
    writer.execute(HSet::new(k, "f1", "v1")).await.unwrap();

    let _: Vec<(Bytes, Bytes)> = client.execute(HGetAll::new(k)).await.unwrap();
    assert!(client.cache_size().await >= 1);

    writer.execute(Del::new(k)).await.unwrap();
    client.clear_cache().await;
}

#[tokio::test]
async fn csc_hget_fields_do_not_collide() {
    // Regression for the cache-key collision: HGET h f1 and HGET h f2 share a
    // Redis key but must return their own values, not each other's.
    let addr = redis_addr().await;
    let mut client = redis_tower::CachedClient::connect(addr).await.unwrap();

    let k = "csc_hget_collision:h";
    let mut writer = conn().await;
    writer.execute(HSet::new(k, "f1", "v1")).await.unwrap();
    writer.execute(HSet::new(k, "f2", "v2")).await.unwrap();

    // Populate the cache with both fields.
    let a: Option<Bytes> = client.execute(HGet::new(k, "f1")).await.unwrap();
    let b: Option<Bytes> = client.execute(HGet::new(k, "f2")).await.unwrap();
    assert_eq!(a, Some(Bytes::from("v1")));
    assert_eq!(b, Some(Bytes::from("v2")));
    assert!(client.cache_size().await >= 2, "fields cached separately");

    // Cached reads must still return the correct, distinct values.
    let a2: Option<Bytes> = client.execute(HGet::new(k, "f1")).await.unwrap();
    let b2: Option<Bytes> = client.execute(HGet::new(k, "f2")).await.unwrap();
    assert_eq!(a2, Some(Bytes::from("v1")), "f1 must not return f2's value");
    assert_eq!(b2, Some(Bytes::from("v2")), "f2 must not return f1's value");

    writer.execute(Del::new(k)).await.unwrap();
    client.clear_cache().await;
}

#[tokio::test]
async fn csc_reports_healthy_caching() {
    // A fresh CachedClient has a live tracking connection, so caching is active.
    let addr = redis_addr().await;
    let client = redis_tower::CachedClient::connect(addr).await.unwrap();
    assert!(
        client.is_caching_healthy().await,
        "caching should be healthy on a fresh connection"
    );
}

// -- Additional PubSub edge-case tests (#156) --

#[tokio::test]
async fn pubsub_message_fields() {
    let channel = key("pubsub_fields", "ch");

    let sub_conn = conn().await;
    let mut pubsub = PubSubConnection::from_connection(sub_conn).unwrap();
    pubsub.subscribe(&[&channel]).await.unwrap();

    let mut pub_conn = conn().await;
    pub_conn
        .execute(Publish::new(&channel, "test_payload"))
        .await
        .unwrap();

    let msg = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub.next())
        .await
        .expect("timeout waiting for message")
        .expect("stream ended")
        .expect("parse error");

    assert_eq!(msg.channel, channel);
    assert_eq!(msg.payload, Bytes::from("test_payload"));
    assert_eq!(msg.kind, redis_tower::MessageKind::Message);
    assert!(msg.pattern.is_none());
}

#[tokio::test]
async fn pubsub_multiple_channels() {
    let ch1 = key("pubsub_multi_ch", "ch1");
    let ch2 = key("pubsub_multi_ch", "ch2");

    let sub_conn = conn().await;
    let mut pubsub = PubSubConnection::from_connection(sub_conn).unwrap();
    pubsub.subscribe(&[&ch1, &ch2]).await.unwrap();

    let mut pub_conn = conn().await;
    pub_conn.execute(Publish::new(&ch1, "msg1")).await.unwrap();
    pub_conn.execute(Publish::new(&ch2, "msg2")).await.unwrap();

    let m1 = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("parse error");
    let m2 = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("parse error");

    let payloads: Vec<&[u8]> = vec![m1.payload.as_ref(), m2.payload.as_ref()];
    assert!(payloads.contains(&b"msg1".as_ref()));
    assert!(payloads.contains(&b"msg2".as_ref()));
}

#[tokio::test]
async fn pubsub_subscribe_after_unsubscribe() {
    let ch = key("pubsub_resub", "ch");

    let sub_conn = conn().await;
    let mut pubsub = PubSubConnection::from_connection(sub_conn).unwrap();
    pubsub.subscribe(&[&ch]).await.unwrap();
    pubsub.unsubscribe(&[&ch]).await.unwrap();
    pubsub.subscribe(&[&ch]).await.unwrap();

    let mut pub_conn = conn().await;
    pub_conn
        .execute(Publish::new(&ch, "after_resub"))
        .await
        .unwrap();

    let msg = tokio::time::timeout(std::time::Duration::from_secs(2), pubsub.next())
        .await
        .expect("timeout waiting for message")
        .expect("stream ended")
        .expect("parse error");

    assert_eq!(msg.payload, Bytes::from("after_resub"));
}

// -- Auto-pipeline integration tests (#222) --

#[tokio::test]
async fn auto_pipeline_basic() {
    use redis_tower::{AutoPipelineConfig, AutoPipelineService, CommandAdapter};

    let conn = conn().await;
    let svc = AutoPipelineService::new(conn, AutoPipelineConfig::default());
    let mut svc = CommandAdapter::new(svc);

    let k = key("auto_pipe", "k");
    call_ready(&mut svc, Set::new(&k, "hello")).await.unwrap();
    let val: Option<Bytes> = call_ready(&mut svc, Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("hello")));
    call_ready(&mut svc, Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn auto_pipeline_sequential_commands() {
    use redis_tower::{AutoPipelineConfig, AutoPipelineService, CommandAdapter};

    let c = conn().await;
    let svc = AutoPipelineService::new(c, AutoPipelineConfig::default());
    let mut svc = CommandAdapter::new(svc);

    // Multiple sequential commands through auto-pipeline.
    for i in 0..5 {
        let k = format!("auto_pipe_seq:{i}");
        call_ready(&mut svc, Set::new(&k, format!("v{i}")))
            .await
            .unwrap();
    }

    // Verify all were set.
    let mut verify = conn().await;
    for i in 0..5 {
        let k = format!("auto_pipe_seq:{i}");
        let val: Option<Bytes> = verify.execute(Get::new(&k)).await.unwrap();
        assert_eq!(val, Some(Bytes::from(format!("v{i}"))));
        verify.execute(Del::new(&k)).await.unwrap();
    }
}

#[tokio::test]
async fn auto_pipeline_call_pipeline_atomic() {
    // Verify that call_pipeline emits frames contiguously on the wire: the
    // three commands in one call return matching responses in order.
    use redis_tower::{AutoPipelineConfig, AutoPipelineService};
    use redis_tower_core::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    let c = conn().await;
    let mut svc = AutoPipelineService::new(c, AutoPipelineConfig::default());

    let k = key("auto_pipe_atomic", "k");

    let frames = vec![
        array(vec![bulk("SET"), bulk(k.as_bytes()), bulk("one")]),
        array(vec![bulk("APPEND"), bulk(k.as_bytes()), bulk("-two")]),
        array(vec![bulk("GET"), bulk(k.as_bytes())]),
    ];
    let responses = svc.call_pipeline(frames).await.unwrap();
    assert_eq!(responses.len(), 3);

    // Third response should be the concatenated value.
    match &responses[2] {
        Frame::BulkString(Some(b)) => assert_eq!(b.as_ref(), b"one-two".as_slice()),
        other => panic!("expected bulk string, got {other:?}"),
    }

    // Cleanup.
    let mut cleanup = conn().await;
    cleanup.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn auto_pipeline_call_pipeline_empty() {
    // Empty call_pipeline is a no-op and returns an empty response vec.
    use redis_tower::{AutoPipelineConfig, AutoPipelineService};

    let c = conn().await;
    let mut svc = AutoPipelineService::new(c, AutoPipelineConfig::default());

    let responses = svc.call_pipeline(Vec::new()).await.unwrap();
    assert!(responses.is_empty());
}

// -- Connection pool integration tests (#222) --

#[tokio::test]
async fn pool_basic() {
    use redis_tower::pool::ConnectionPool;

    let addr = redis_addr().await;
    let pool = ConnectionPool::connect(2, || {
        let a = addr;
        async move { RedisConnection::connect(a).await }
    })
    .await
    .unwrap();

    let k = key("pool_basic", "k");
    pool.execute(Set::new(&k, "pooled")).await.unwrap();
    let val: Option<Bytes> = pool.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("pooled")));
    pool.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn pool_concurrent_tasks() {
    use redis_tower::pool::ConnectionPool;

    let addr = redis_addr().await;
    let pool = ConnectionPool::connect(4, || {
        let a = addr;
        async move { RedisConnection::connect(a).await }
    })
    .await
    .unwrap();

    let mut handles = Vec::new();
    for i in 0..20 {
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            let k = format!("pool_conc:{i}");
            p.execute(Set::new(&k, format!("v{i}"))).await.unwrap();
            let val: Option<Bytes> = p.execute(Get::new(&k)).await.unwrap();
            assert_eq!(val, Some(Bytes::from(format!("v{i}"))));
            p.execute(Del::new(&k)).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

// -- MultiplexedClient RESP3 tests --

#[tokio::test]
async fn multiplexed_client_connect_resp3() {
    let addr = redis_addr().await;
    let client = MultiplexedClient::connect_resp3(addr)
        .await
        .expect("failed to connect via RESP3");

    let k = key("mux_resp3", "k");
    let hk = key("mux_resp3", "h");
    let sk = key("mux_resp3", "s");

    // Basic SET/GET round-trip over the multiplexed RESP3 connection.
    client.execute(Set::new(&k, "resp3_value")).await.unwrap();
    let val: Option<Bytes> = client.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("resp3_value")));

    // HSET + HGETALL -- RESP3 returns a map type; the command adapter
    // normalises it to Vec<(Bytes, Bytes)> the same as RESP2.
    client
        .execute(HSet::new(&hk, "field1", "v1").field("field2", "v2"))
        .await
        .unwrap();
    let pairs = client.execute(HGetAll::new(&hk)).await.unwrap();
    assert_eq!(pairs.len(), 2);
    let has_f1 = pairs
        .iter()
        .any(|(f, v)| f == &Bytes::from("field1") && v == &Bytes::from("v1"));
    let has_f2 = pairs
        .iter()
        .any(|(f, v)| f == &Bytes::from("field2") && v == &Bytes::from("v2"));
    assert!(
        has_f1 && has_f2,
        "HGETALL missing expected fields via RESP3"
    );

    // SMEMBERS -- RESP3 returns a set type; the command adapter normalises
    // it to Vec<Bytes> the same as RESP2.
    client
        .execute(SAdd::members(&sk, ["alpha", "beta", "gamma"]))
        .await
        .unwrap();
    let members = client.execute(SMembers::new(&sk)).await.unwrap();
    assert_eq!(members.len(), 3);

    // Cleanup.
    client.execute(Del::keys([&k, &hk, &sk])).await.unwrap();
}

// -- MultiplexedClient factory-backed reconnect --

#[tokio::test]
async fn multiplexed_client_reconnects_after_server_restart() {
    // Dedicated standalone on a non-default port so we don't interfere
    // with the shared REDIS instance used by the rest of the suite.
    let server = RedisServer::new()
        .port(6401)
        .start()
        .await
        .expect("start standalone");
    let addr = server.addr();

    let client = MultiplexedClient::from_factory(
        AddrConnectionFactory::new(&addr),
        AutoPipelineConfig::default(),
        AutoPipelineReconnectConfig::new(
            ReconnectConfig::default()
                .base_delay(std::time::Duration::from_millis(20))
                .max_delay(std::time::Duration::from_millis(200)),
        ),
    )
    .await
    .expect("connect");

    // Baseline op before the outage.
    client
        .execute(Set::new("mux:reconnect:k", "before"))
        .await
        .unwrap();
    let before: Option<Bytes> = client.execute(Get::new("mux:reconnect:k")).await.unwrap();
    assert_eq!(before, Some(Bytes::from("before")));

    // Kill the server. The next request will fail, then the worker will
    // reconnect via the factory.
    server.stop();

    // One or more requests may error while the server is down / before
    // the worker has reconnected. That's expected -- we just want eventual
    // recovery after the server comes back.
    let _ = client
        .execute(Set::new("mux:reconnect:k", "during-outage"))
        .await;

    // Bring the server back up on the same port.
    let _restarted = RedisServer::new()
        .port(6401)
        .start()
        .await
        .expect("restart standalone");

    // Poll for recovery: give the worker up to ~3s to reconnect and
    // start serving requests again.
    let mut recovered = false;
    for _ in 0..60 {
        // Probe GET. The key was never written on the restarted server
        // (save is disabled), so a None response confirms both that the
        // client recovered AND that the server really did restart with a
        // fresh DB -- not that the original connection somehow survived.
        match client.execute(Get::new("mux:reconnect:k")).await {
            Ok(None) => {
                recovered = true;
                break;
            }
            Ok(Some(_)) => {
                panic!(
                    "key persisted across restart -- server did not actually \
                     restart or save was not disabled"
                );
            }
            Err(_) => {}
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        recovered,
        "client failed to recover within 3s of server restart"
    );

    // Confirm the recovered connection accepts writes too.
    client
        .execute(Set::new("mux:reconnect:k", "after"))
        .await
        .unwrap();
    let after: Option<Bytes> = client.execute(Get::new("mux:reconnect:k")).await.unwrap();
    assert_eq!(after, Some(Bytes::from("after")));

    client.execute(Del::new("mux:reconnect:k")).await.unwrap();
}

#[tokio::test]
async fn multiplexed_response_timeout_trips_on_slow_command() {
    let conn = RedisConnection::connect(redis_addr().await).await.unwrap();
    let config = AutoPipelineConfig {
        response_timeout: Some(std::time::Duration::from_millis(150)),
        ..Default::default()
    };
    let client = MultiplexedClient::from_connection_with_config(conn, config);

    // BLPOP on an empty key blocks this connection for up to 5s (it does not
    // block the whole server). The 150ms response deadline must trip first and
    // surface CommandTimeout rather than stalling the worker.
    let k = key("mux_timeout", "blpop");
    client.execute(Del::new(&k)).await.unwrap();
    let err = client
        .execute(RawCommand::new("BLPOP").arg(&k).arg("5"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, redis_tower::RedisError::CommandTimeout),
        "expected CommandTimeout, got {err:?}"
    );
}

#[tokio::test]
async fn multiplexed_pipeline_flushed_metric_fires() {
    use redis_tower::metrics_layer::{ErrorKind, MetricsRecorder};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingRecorder {
        flushes: AtomicUsize,
        frames: AtomicUsize,
    }
    impl MetricsRecorder for CountingRecorder {
        fn command_completed(
            &self,
            _command: &str,
            _duration: std::time::Duration,
            _error: Option<ErrorKind>,
        ) {
        }
        fn pipeline_flushed(&self, batch_size: usize) {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            self.frames.fetch_add(batch_size, Ordering::SeqCst);
        }
    }

    let recorder = Arc::new(CountingRecorder {
        flushes: AtomicUsize::new(0),
        frames: AtomicUsize::new(0),
    });
    let conn = RedisConnection::connect(redis_addr().await).await.unwrap();
    let config = AutoPipelineConfig {
        metrics_recorder: Some(Arc::clone(&recorder) as Arc<dyn MetricsRecorder>),
        ..Default::default()
    };
    let client = MultiplexedClient::from_connection_with_config(conn, config);

    let k = key("metrics_flush", "k");
    client.execute(Set::new(&k, "v")).await.unwrap();
    client.execute(Del::new(&k)).await.unwrap();

    assert!(
        recorder.flushes.load(Ordering::SeqCst) >= 1,
        "pipeline_flushed should fire on a worker flush"
    );
    assert!(
        recorder.frames.load(Ordering::SeqCst) >= 2,
        "the SET and DEL frames were flushed and counted"
    );
}
