//! Integration tests for core Redis commands
//!
//! These tests run against a real Redis instance using testcontainers.
//! They verify that commands work correctly end-to-end.
//!
//! Run with: cargo test --test integration_core

mod helpers;

use bytes::Bytes;
use helpers::standalone::setup_redis;
use redis_tower::commands::*;
use std::collections::HashMap;

#[tokio::test]
async fn test_string_set_get() {
    let client = setup_redis().await;

    // SET and GET
    client
        .call(Set::new("test_key", "test_value"))
        .await
        .unwrap();

    let get_result: Option<Bytes> = client.call(Get::new("test_key")).await.unwrap();
    assert_eq!(
        get_result.as_ref().map(|b| b.as_ref()),
        Some(b"test_value".as_ref())
    );

    // GET non-existent key
    let none_result: Option<Bytes> = client.call(Get::new("nonexistent")).await.unwrap();
    assert_eq!(none_result, None);
}

#[tokio::test]
async fn test_string_incr_decr() {
    let client = setup_redis().await;

    // INCR
    let incr1: i64 = client.call(Incr::new("counter")).await.unwrap();
    assert_eq!(incr1, 1);

    let incr2: i64 = client.call(Incr::new("counter")).await.unwrap();
    assert_eq!(incr2, 2);

    // INCRBY
    let incrby: i64 = client.call(IncrBy::new("counter", 5)).await.unwrap();
    assert_eq!(incrby, 7);

    // DECR
    let decr: i64 = client.call(Decr::new("counter")).await.unwrap();
    assert_eq!(decr, 6);

    // DECRBY
    let decrby: i64 = client.call(DecrBy::new("counter", 3)).await.unwrap();
    assert_eq!(decrby, 3);
}

#[tokio::test]
async fn test_string_mget_mset() {
    let client = setup_redis().await;

    // MSET
    client
        .call(
            Mset::new()
                .pair("key1", b"value1".to_vec())
                .pair("key2", b"value2".to_vec())
                .pair("key3", b"value3".to_vec()),
        )
        .await
        .unwrap();

    // MGET
    let mget_result: Vec<Option<Bytes>> = client
        .call(MGet::new(vec![
            "key1".to_string(),
            "key2".to_string(),
            "key3".to_string(),
            "nonexistent".to_string(),
        ]))
        .await
        .unwrap();

    assert_eq!(mget_result.len(), 4);
    assert_eq!(
        mget_result[0].as_ref().map(|b| b.as_ref()),
        Some(b"value1".as_ref())
    );
    assert_eq!(
        mget_result[1].as_ref().map(|b| b.as_ref()),
        Some(b"value2".as_ref())
    );
    assert_eq!(
        mget_result[2].as_ref().map(|b| b.as_ref()),
        Some(b"value3".as_ref())
    );
    assert_eq!(mget_result[3], None);
}

#[tokio::test]
async fn test_hash_operations() {
    let client = setup_redis().await;

    // HSET with multiple fields - call individually since HSet only does one field
    client
        .call(HSet::new("user:1", "name", b"Alice".to_vec()))
        .await
        .unwrap();
    client
        .call(HSet::new("user:1", "age", b"30".to_vec()))
        .await
        .unwrap();
    client
        .call(HSet::new("user:1", "city", b"NYC".to_vec()))
        .await
        .unwrap();

    // HGET
    let name: Option<Bytes> = client.call(HGet::new("user:1", "name")).await.unwrap();
    assert_eq!(name.as_ref().map(|b| b.as_ref()), Some(b"Alice".as_ref()));

    // HGETALL
    let all: HashMap<String, Bytes> = client.call(HGetAll::new("user:1")).await.unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(all.get("name").map(|b| b.as_ref()), Some(b"Alice".as_ref()));
    assert_eq!(all.get("age").map(|b| b.as_ref()), Some(b"30".as_ref()));

    // HINCRBY
    let new_age: i64 = client.call(HIncrBy::new("user:1", "age", 1)).await.unwrap();
    assert_eq!(new_age, 31);
}

#[tokio::test]
async fn test_list_operations() {
    let client = setup_redis().await;

    // LPUSH
    let lpush_result: i64 = client
        .call(LPush::new(
            "mylist",
            vec![Bytes::from("three"), Bytes::from("two"), Bytes::from("one")],
        ))
        .await
        .unwrap();
    assert_eq!(lpush_result, 3);

    // LRANGE
    let range: Vec<Bytes> = client.call(LRange::new("mylist", 0, -1)).await.unwrap();
    assert_eq!(range.len(), 3);
    assert_eq!(range[0].as_ref(), b"one");
    assert_eq!(range[1].as_ref(), b"two");
    assert_eq!(range[2].as_ref(), b"three");

    // LPOP
    let popped: Option<Bytes> = client.call(LPop::new("mylist")).await.unwrap();
    assert_eq!(popped.as_ref().map(|b| b.as_ref()), Some(b"one".as_ref()));

    // LLEN
    let len: i64 = client.call(LLen::new("mylist")).await.unwrap();
    assert_eq!(len, 2);
}

#[tokio::test]
async fn test_set_operations() {
    let client = setup_redis().await;

    // SADD
    let sadd_result: i64 = client
        .call(
            Sadd::new("myset", b"one".to_vec())
                .member(b"two".to_vec())
                .member(b"three".to_vec()),
        )
        .await
        .unwrap();
    assert_eq!(sadd_result, 3);

    // SISMEMBER
    let is_member: bool = client
        .call(Sismember::new("myset", b"two".to_vec()))
        .await
        .unwrap();
    assert!(is_member);

    let not_member: bool = client
        .call(Sismember::new("myset", b"four".to_vec()))
        .await
        .unwrap();
    assert!(!not_member);

    // SCARD
    let card: i64 = client.call(Scard::new("myset")).await.unwrap();
    assert_eq!(card, 3);

    // SMEMBERS
    let members: Vec<Bytes> = client.call(Smembers::new("myset")).await.unwrap();
    assert_eq!(members.len(), 3);
}

#[tokio::test]
async fn test_sorted_set_operations() {
    let client = setup_redis().await;

    // ZADD
    let zadd_result: i64 = client
        .call(
            Zadd::new("leaderboard")
                .member(100.0, "player1")
                .member(200.0, "player2")
                .member(150.0, "player3"),
        )
        .await
        .unwrap();
    assert_eq!(zadd_result, 3);

    // ZCARD
    let card: i64 = client.call(Zcard::new("leaderboard")).await.unwrap();
    assert_eq!(card, 3);

    // ZSCORE
    let score: Option<f64> = client
        .call(Zscore::new("leaderboard", "player2"))
        .await
        .unwrap();
    assert_eq!(score, Some(200.0));

    // ZRANGE (returns ZrangeResult, not Vec<String>)
    let range = client
        .call(Zrange::new("leaderboard", 0, -1))
        .await
        .unwrap();
    assert_eq!(range.members.len(), 3);
    // Members should be sorted by score: player1 (100), player3 (150), player2 (200)
    assert_eq!(range.members[0].0.as_ref(), b"player1");
    assert_eq!(range.members[1].0.as_ref(), b"player3");
    assert_eq!(range.members[2].0.as_ref(), b"player2");

    // ZREVRANGE
    let revrange = client
        .call(Zrevrange::new("leaderboard", 0, -1))
        .await
        .unwrap();
    assert_eq!(revrange.members.len(), 3);
    // Reverse order: player2 (200), player3 (150), player1 (100)
    assert_eq!(revrange.members[0].0.as_ref(), b"player2");
    assert_eq!(revrange.members[1].0.as_ref(), b"player3");
    assert_eq!(revrange.members[2].0.as_ref(), b"player1");
}

#[tokio::test]
async fn test_key_operations() {
    let client = setup_redis().await;

    // Set up some keys
    client.call(Set::new("key1", "value1")).await.unwrap();
    client.call(Set::new("key2", "value2")).await.unwrap();
    client.call(Set::new("key3", "value3")).await.unwrap();

    // EXISTS - takes single key
    let exists1: i64 = client.call(Exists::new("key1")).await.unwrap();
    assert_eq!(exists1, 1);

    let exists_none: i64 = client.call(Exists::new("nonexistent")).await.unwrap();
    assert_eq!(exists_none, 0);

    // DEL
    let del: i64 = client
        .call(Del::new(vec!["key1".to_string(), "key2".to_string()]))
        .await
        .unwrap();
    assert_eq!(del, 2);

    // EXPIRE and TTL
    client.call(Set::new("expiring", "value")).await.unwrap();
    let expire_result: bool = client.call(Expire::new("expiring", 10)).await.unwrap();
    assert!(expire_result);

    let ttl: i64 = client.call(Ttl::new("expiring")).await.unwrap();
    assert!(ttl > 0 && ttl <= 10);
}

#[tokio::test]
async fn test_expiration() {
    let client = setup_redis().await;

    // SETEX - returns () not String
    client
        .call(Setex::new("tempkey", 10, "tempvalue"))
        .await
        .unwrap();

    let value: Option<Bytes> = client.call(Get::new("tempkey")).await.unwrap();
    assert_eq!(
        value.as_ref().map(|b| b.as_ref()),
        Some(b"tempvalue".as_ref())
    );

    let ttl: i64 = client.call(Ttl::new("tempkey")).await.unwrap();
    assert!(ttl > 0 && ttl <= 10);

    // PERSIST
    let persist: bool = client.call(Persist::new("tempkey")).await.unwrap();
    assert!(persist);

    let ttl_after: i64 = client.call(Ttl::new("tempkey")).await.unwrap();
    assert_eq!(ttl_after, -1); // -1 means no expiration
}

#[tokio::test]
async fn test_string_manipulation() {
    let client = setup_redis().await;

    // APPEND
    client.call(Set::new("message", "Hello")).await.unwrap();
    let len: i64 = client.call(Append::new("message", " World")).await.unwrap();
    assert_eq!(len, 11);

    let value: Option<Bytes> = client.call(Get::new("message")).await.unwrap();
    assert_eq!(
        value.as_ref().map(|b| b.as_ref()),
        Some(b"Hello World".as_ref())
    );

    // STRLEN
    let strlen: i64 = client.call(StrLen::new("message")).await.unwrap();
    assert_eq!(strlen, 11);

    // GETRANGE
    let range: Bytes = client.call(GetRange::new("message", 0, 4)).await.unwrap();
    assert_eq!(range.as_ref(), b"Hello");
}

#[tokio::test]
async fn test_set_intersect_union_diff() {
    let client = setup_redis().await;

    // Set up two sets
    client
        .call(
            Sadd::new("set1", b"a".to_vec())
                .member(b"b".to_vec())
                .member(b"c".to_vec()),
        )
        .await
        .unwrap();
    client
        .call(
            Sadd::new("set2", b"b".to_vec())
                .member(b"c".to_vec())
                .member(b"d".to_vec()),
        )
        .await
        .unwrap();

    // SINTER - takes single key, use .key() to add more
    let inter: Vec<Bytes> = client.call(Sinter::new("set1").key("set2")).await.unwrap();
    assert_eq!(inter.len(), 2); // b and c

    // SUNION
    let union: Vec<Bytes> = client.call(Sunion::new("set1").key("set2")).await.unwrap();
    assert_eq!(union.len(), 4); // a, b, c, d

    // SDIFF
    let diff: Vec<Bytes> = client.call(Sdiff::new("set1").key("set2")).await.unwrap();
    assert_eq!(diff.len(), 1); // a
    assert_eq!(diff[0].as_ref(), b"a");
}

#[tokio::test]
async fn test_ping_echo() {
    let client = setup_redis().await;

    // PING
    let ping: String = client.call(Ping::new()).await.unwrap();
    assert_eq!(ping, "PONG");

    // ECHO - returns String not Bytes
    let echo: String = client.call(Echo::new("Hello Redis")).await.unwrap();
    assert_eq!(echo, "Hello Redis");
}

#[tokio::test]
async fn test_dbsize() {
    let client = setup_redis().await;

    // Add some keys
    client.call(Set::new("test1", "value")).await.unwrap();
    client.call(Set::new("test2", "value")).await.unwrap();

    // DBSIZE
    let dbsize: i64 = client.call(DbSize).await.unwrap();
    assert!(dbsize >= 2);
}
