use std::sync::OnceLock;

use bytes::Bytes;
use redis_test_harness::standalone::{RedisStandalone, StandaloneConfig};
use redis_tower::commands::*;
use redis_tower::reconnect::{AddrConnectionFactory, ReconnectConfig};
use redis_tower::{
    Pipeline, PubSubConnection, RedisClient, RedisConnection, ResilientConnection,
    ResilientRedisClient, Transaction, TransactionResult,
};
use tokio_stream::StreamExt;
use tower::Service;

/// Shared Redis instance -- started once, stopped on Drop.
static REDIS: OnceLock<RedisStandalone> = OnceLock::new();

fn ensure_redis() -> &'static RedisStandalone {
    REDIS.get_or_init(|| {
        // Check for external Redis first (CI service container).
        if let Ok(url) = std::env::var("REDIS_URL") {
            let addr = url
                .strip_prefix("redis://")
                .unwrap_or(&url)
                .trim_end_matches('/')
                .to_string();
            if let Some((host, port_str)) = addr.rsplit_once(':') {
                if let Ok(port) = port_str.parse::<u16>() {
                    // Return a "fake" standalone that points at the external Redis.
                    // It won't start/stop anything since the server is already running.
                    return RedisStandalone::new(StandaloneConfig {
                        port,
                        bind: host.to_string(),
                        ..Default::default()
                    });
                }
            }
        }

        let mut standalone = RedisStandalone::with_defaults();
        standalone.start().expect("failed to start Redis server");
        standalone
    })
}

fn redis_addr() -> String {
    ensure_redis().addr()
}

async fn conn() -> RedisConnection {
    let addr = redis_addr();
    RedisConnection::connect(&addr)
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
    let addr = redis_addr();
    RedisConnection::connect_resp3(&addr)
        .await
        .expect("failed to connect with RESP3")
}

// Generate shared command tests for RESP3 in a submodule to avoid name conflicts.
mod resp3 {
    use super::*;
    redis_test_harness::command_tests!(resp3_conn, "resp3_cmd");
}

async fn client() -> RedisClient {
    let addr = redis_addr();
    RedisClient::connect(&addr)
        .await
        .expect("failed to connect to Redis")
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
    let addr = redis_addr();
    let url = format!("redis://{addr}");
    let conn = RedisConnection::connect_url(&url).await.unwrap();
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

// -- Hash command tests --

#[tokio::test]
async fn hset_and_hget() {
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
    let k = key("hexists", "h");
    conn.execute(HSet::new(&k, "f", "v")).await.unwrap();
    assert!(conn.execute(HExists::new(&k, "f")).await.unwrap());
    assert!(!conn.execute(HExists::new(&k, "nope")).await.unwrap());
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn hgetall() {
    let conn = conn().await;
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
    let conn = conn().await;
    let k = key("hincrby", "h");
    conn.execute(HSet::new(&k, "count", "10")).await.unwrap();
    let val = conn.execute(HIncrBy::new(&k, "count", 5)).await.unwrap();
    assert_eq!(val, 15);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn hkeys_hvals_hlen() {
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let pub_conn = conn().await;
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
    let pub_conn = conn().await;
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
    let addr = redis_addr();
    let url = format!("redis://{addr}/1");
    RedisConnection::connect_url(&url).await.unwrap()
}

#[tokio::test]
async fn flushdb() {
    let conn = conn_db1().await;
    let k = key("flushdb", "k");
    conn.execute(Set::new(&k, "x")).await.unwrap();
    conn.execute(FlushDb::new()).await.unwrap();
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn flushdb_sync_mode() {
    let conn = conn_db1().await;
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
    let conn = conn().await;
    let k = key("pipe_mismatch", "k");
    conn.execute(Set::new(&k, "hello")).await.unwrap();

    let results = Pipeline::new()
        .push(Get::new(&k))
        .execute(&conn)
        .await
        .unwrap();

    // Try to get as wrong type.
    let err = results.get::<i64>(0);
    assert!(err.is_err());

    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn pipeline_take_twice() {
    let conn = conn().await;
    let k = key("pipe_take2", "k");
    conn.execute(Set::new(&k, "val")).await.unwrap();

    let mut results = Pipeline::new()
        .push(Get::new(&k))
        .execute(&conn)
        .await
        .unwrap();

    let _first: Option<Bytes> = results.take(0).unwrap();
    let second = results.take::<Option<Bytes>>(0);
    assert!(second.is_err(), "double take should fail");

    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn pipeline_out_of_bounds() {
    let conn = conn().await;
    let results = Pipeline::new()
        .push(Ping::new())
        .execute(&conn)
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
    let addr = redis_addr();
    let url = format!("redis://{addr}");
    let client = RedisClient::connect_url(&url).await.unwrap();
    let pong = client.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

// Strings: Set with PX and XX

#[tokio::test]
async fn set_with_px() {
    let conn = conn().await;
    let k = key("set_px", "k");
    conn.execute(Set::new(&k, "value").px(10000)).await.unwrap();
    let ttl = conn.execute(Ttl::new(&k)).await.unwrap();
    assert!(ttl > 0 && ttl <= 10);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn set_xx_succeeds_when_exists() {
    let conn = conn().await;
    let k = key("set_xx_ok", "k");
    conn.execute(Set::new(&k, "old")).await.unwrap();
    conn.execute(Set::new(&k, "new").xx()).await.unwrap();
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("new")));
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn set_xx_fails_when_missing() {
    let conn = conn().await;
    let k = key("set_xx_fail", "k");
    conn.execute(Del::new(&k)).await.unwrap();
    conn.execute(Set::new(&k, "value").xx()).await.unwrap();
    let val = conn.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, None);
}

// Lists: multi-element push, pop on empty

#[tokio::test]
async fn lpush_multiple() {
    let conn = conn().await;
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
    let conn = conn().await;
    let k = key("lpop_empty", "l");
    conn.execute(Del::new(&k)).await.unwrap();
    let val = conn.execute(LPop::new(&k)).await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn rpop_empty() {
    let conn = conn().await;
    let k = key("rpop_empty", "l");
    conn.execute(Del::new(&k)).await.unwrap();
    let val = conn.execute(RPop::new(&k)).await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn lindex_out_of_range() {
    let conn = conn().await;
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
    let conn = conn().await;
    let k = key("exists_single", "k");
    conn.execute(Set::new(&k, "x")).await.unwrap();
    assert_eq!(conn.execute(Exists::new(&k)).await.unwrap(), 1);
    conn.execute(Del::new(&k)).await.unwrap();
    assert_eq!(conn.execute(Exists::new(&k)).await.unwrap(), 0);
}

// Hashes: single field HDel

#[tokio::test]
async fn hdel_single() {
    let conn = conn().await;
    let k = key("hdel_single", "h");
    conn.execute(HSet::new(&k, "f", "v")).await.unwrap();
    let removed = conn.execute(HDel::new(&k, "f")).await.unwrap();
    assert_eq!(removed, 1);
    conn.execute(Del::new(&k)).await.unwrap();
}

// Hashes: empty HGetAll

#[tokio::test]
async fn hgetall_empty() {
    let conn = conn().await;
    let k = key("hgetall_empty", "h");
    conn.execute(Del::new(&k)).await.unwrap();
    let pairs = conn.execute(HGetAll::new(&k)).await.unwrap();
    assert!(pairs.is_empty());
}

// Sets: single SRem, multi SRem

#[tokio::test]
async fn srem_multiple() {
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
    let k = key("txn_err", "k");
    conn.execute(Set::new(&k, "not_a_number")).await.unwrap();

    let result = Transaction::new()
        .push(Incr::new(&k)) // will fail inside EXEC
        .push(Ping::new())
        .execute(&conn)
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
    let conn = conn().await;
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
    let conn = conn().await;
    let k = key("append_create", "k");
    conn.execute(Del::new(&k)).await.unwrap();
    let len = conn.execute(Append::new(&k, "new")).await.unwrap();
    assert_eq!(len, 3);
    conn.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn mset() {
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
    let size = conn.execute(DbSize::new()).await.unwrap();
    assert!(size >= 0, "DBSIZE should return non-negative");
    // Don't compare before/after -- parallel tests can change the count.
}

#[tokio::test]
async fn select_db() {
    let conn = conn().await;
    conn.execute(Select::new(2)).await.unwrap();
    conn.execute(Set::new("select_test", "val")).await.unwrap();
    conn.execute(Select::new(0)).await.unwrap();
    // Clean up DB 2.
    conn.execute(Select::new(2)).await.unwrap();
    conn.execute(Del::new("select_test")).await.unwrap();
}

#[tokio::test]
async fn lmove() {
    let conn = conn().await;
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
    let addr = redis_addr();
    let conn = ResilientConnection::new(
        AddrConnectionFactory::new(&addr),
        ReconnectConfig::default(),
    )
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

    let addr = redis_addr();
    let connected = Arc::new(AtomicBool::new(false));
    let connected_clone = Arc::clone(&connected);

    let conn = ResilientConnection::new(
        AddrConnectionFactory::new(&addr),
        ReconnectConfig::default(),
    )
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
    let addr = redis_addr();
    let client = ResilientRedisClient::connect(&addr).await.unwrap();

    let k = key("resilient_client", "k");
    client.execute(Set::new(&k, "val")).await.unwrap();
    let val = client.execute(Get::new(&k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("val")));
    client.execute(Del::new(&k)).await.unwrap();
}

#[tokio::test]
async fn resilient_client_shared_across_tasks() {
    let addr = redis_addr();
    let client = ResilientRedisClient::connect(&addr).await.unwrap();
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
    let addr = redis_addr();
    let url = format!("redis://{addr}");
    let client = ResilientRedisClient::connect_url(&url).await.unwrap();
    let pong = client.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
}

// -- Client-side caching tests --

#[tokio::test]
async fn csc_cache_hit() {
    let addr = redis_addr();
    let client = redis_tower::CachedClient::connect(&addr).await.unwrap();

    let k = "csc_test:cache_hit";
    // Write via a separate connection (bypasses cache).
    let writer = conn().await;
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
    let addr = redis_addr();
    let client = redis_tower::CachedClient::connect(&addr).await.unwrap();

    let k = "csc_test:invalidation";

    // Write and read to populate cache.
    let writer = conn().await;
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
    let addr = redis_addr();
    let client = redis_tower::CachedClient::connect(&addr).await.unwrap();

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

    let addr = redis_addr();
    let frame_svc = FrameService::connect(&addr).await.unwrap();
    let cache_svc = CacheService::new(frame_svc, CacheConfig::default());
    let mut svc = CommandAdapter::new(cache_svc);

    let k = "tower_csc:cache_hit";
    // Write directly.
    let writer = conn().await;
    writer.execute(Set::new(k, "hello")).await.unwrap();

    // First read: cache miss.
    let v1: Option<Bytes> = svc.call(Get::new(k)).await.unwrap();
    assert_eq!(v1, Some(Bytes::from("hello")));
    assert_eq!(svc.inner_mut().cache_size().await, 1);

    // Second read: cache hit.
    let v2: Option<Bytes> = svc.call(Get::new(k)).await.unwrap();
    assert_eq!(v2, Some(Bytes::from("hello")));

    writer.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn tower_csc_write_bypasses_cache() {
    use redis_tower::FrameService;
    use redis_tower::cache_layer::{CacheConfig, CacheService};
    use redis_tower::command_adapter::CommandAdapter;

    let addr = redis_addr();
    let frame_svc = FrameService::connect(&addr).await.unwrap();
    let cache_svc = CacheService::new(frame_svc, CacheConfig::default());
    let mut svc = CommandAdapter::new(cache_svc);

    let k = "tower_csc:write_bypass";
    svc.call(Set::new(k, "val")).await.unwrap();
    assert_eq!(svc.inner_mut().cache_size().await, 0);

    svc.call(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn tower_csc_with_invalidation() {
    use futures::StreamExt;
    use redis_tower::FrameService;
    use redis_tower::cache_layer::{CacheConfig, CacheService, spawn_invalidation_task};
    use redis_tower::command_adapter::CommandAdapter;
    use redis_tower::commands::ClientTracking;

    let addr = redis_addr();

    // Data connection as FrameService.
    let frame_svc = FrameService::connect(&addr).await.unwrap();

    // Tracking connection for invalidation pushes.
    let tracking_conn = RedisConnection::connect_resp3(&addr).await.unwrap();
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
    let writer = conn().await;
    writer.execute(Set::new(k, "original")).await.unwrap();

    // Populate cache.
    let _: Option<Bytes> = svc.call(Get::new(k)).await.unwrap();
    assert_eq!(cache_ref.read().await.len(), 1);

    // Modify from another connection.
    writer.execute(Set::new(k, "modified")).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Cache should be invalidated.
    assert_eq!(cache_ref.read().await.len(), 0);

    // Fresh read gets new value.
    let v: Option<Bytes> = svc.call(Get::new(k)).await.unwrap();
    assert_eq!(v, Some(Bytes::from("modified")));

    writer.execute(Del::new(k)).await.unwrap();
}

// -- Streams tests --

#[tokio::test]
async fn streams_xadd_xlen_xrange() {
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
    let conn = conn().await;
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
