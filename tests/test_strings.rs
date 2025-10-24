//! Integration tests for String commands.
//!
//! These tests require a running Redis instance on localhost:6379.
//! Run with: cargo test --test test_strings

mod common;

use bytes::Bytes;
use common::{connect, test_key};
use redis_tower::commands::{
    Append, Decr, DecrBy, Del, Echo, Exists, Expire, Get, GetDel, GetEx, GetRange, Incr, IncrBy,
    IncrByFloat, Ping, Set, SetRange, StrLen, Ttl,
};

#[tokio::test]
async fn test_ping() {
    let client = connect().await.expect("Failed to connect to Redis");

    // PING without message
    let response = client.execute(Ping::new()).await.unwrap();
    assert_eq!(response, "PONG");

    // PING with message
    let response = client.execute(Ping::with_message("hello")).await.unwrap();
    assert_eq!(response, "hello");
}

#[tokio::test]
async fn test_echo() {
    let client = connect().await.expect("Failed to connect to Redis");

    let response = client.execute(Echo::new("Hello, Redis!")).await.unwrap();
    assert_eq!(response, Bytes::from_static(b"Hello, Redis!"));
}

#[tokio::test]
async fn test_set_get() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("set_get");

    // SET a value
    client.execute(Set::new(&key, "test_value")).await.unwrap();

    // GET the value back
    let value: Option<Bytes> = client.execute(Get::new(&key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"test_value")));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_incrby_decrby() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("incrby_decrby");

    // INCRBY on non-existent key starts at 0
    let value = client.execute(IncrBy::new(&key, 5)).await.unwrap();
    assert_eq!(value, 5);

    // INCRBY again
    let value = client.execute(IncrBy::new(&key, 10)).await.unwrap();
    assert_eq!(value, 15);

    // DECRBY
    let value = client.execute(DecrBy::new(&key, 3)).await.unwrap();
    assert_eq!(value, 12);

    // DECRBY again
    let value = client.execute(DecrBy::new(&key, 7)).await.unwrap();
    assert_eq!(value, 5);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_incrbyfloat() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("incrbyfloat");

    // INCRBYFLOAT on non-existent key starts at 0.0
    let value = client.execute(IncrByFloat::new(&key, 2.5)).await.unwrap();
    assert!((value - 2.5).abs() < 0.001);

    // INCRBYFLOAT again
    let value = client.execute(IncrByFloat::new(&key, 3.7)).await.unwrap();
    assert!((value - 6.2).abs() < 0.001);

    // INCRBYFLOAT with negative value (decrement)
    let value = client.execute(IncrByFloat::new(&key, -1.2)).await.unwrap();
    assert!((value - 5.0).abs() < 0.001);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_append() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("append");

    // APPEND to non-existent key creates it
    let length = client.execute(Append::new(&key, "Hello")).await.unwrap();
    assert_eq!(length, 5);

    // APPEND again
    let length = client.execute(Append::new(&key, " World")).await.unwrap();
    assert_eq!(length, 11);

    // Verify the value
    let value: Option<Bytes> = client.execute(Get::new(&key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"Hello World")));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_strlen() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("strlen");

    // STRLEN on non-existent key returns 0
    let length = client.execute(StrLen::new(&key)).await.unwrap();
    assert_eq!(length, 0);

    // Set a value
    client.execute(Set::new(&key, "Hello")).await.unwrap();

    // STRLEN should return 5
    let length = client.execute(StrLen::new(&key)).await.unwrap();
    assert_eq!(length, 5);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_getrange() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("getrange");

    // Set a value
    client.execute(Set::new(&key, "Hello World")).await.unwrap();

    // GETRANGE entire string
    let value = client.execute(GetRange::new(&key, 0, -1)).await.unwrap();
    assert_eq!(value.as_ref(), b"Hello World");

    // GETRANGE first 5 characters
    let value = client.execute(GetRange::new(&key, 0, 4)).await.unwrap();
    assert_eq!(value.as_ref(), b"Hello");

    // GETRANGE last 5 characters
    let value = client.execute(GetRange::new(&key, 6, 10)).await.unwrap();
    assert_eq!(value.as_ref(), b"World");

    // GETRANGE with negative indices
    let value = client.execute(GetRange::new(&key, -5, -1)).await.unwrap();
    assert_eq!(value.as_ref(), b"World");

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_setrange() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("setrange");

    // SETRANGE on non-existent key
    let length = client
        .execute(SetRange::new(&key, 0, "Hello"))
        .await
        .unwrap();
    assert_eq!(length, 5);

    // SETRANGE to replace part of the string
    let length = client
        .execute(SetRange::new(&key, 6, "World"))
        .await
        .unwrap();
    assert_eq!(length, 11);

    // Verify the value (should have null bytes between)
    let value: Option<Bytes> = client.execute(Get::new(&key)).await.unwrap();
    assert_eq!(value.unwrap().len(), 11);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_getex() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("getex");

    // Set a value
    client.execute(Set::new(&key, "value")).await.unwrap();

    // GETEX with EX (expire in seconds) - using builder pattern
    let value = client.execute(GetEx::new(&key).ex(10)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value")));

    // Verify TTL was set
    let ttl = client.execute(Ttl::new(&key)).await.unwrap();
    assert!(ttl > 0 && ttl <= 10);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_getex_persist() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("getex_persist");

    // Set a value
    client.execute(Set::new(&key, "value")).await.unwrap();

    // Set expiration on the key
    client.execute(Expire::new(&key, 10)).await.unwrap();

    // Verify it has TTL
    let ttl = client.execute(Ttl::new(&key)).await.unwrap();
    assert!(ttl > 0);

    // GETEX with PERSIST removes expiration - using builder pattern
    let value = client.execute(GetEx::new(&key).persist()).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value")));

    // Verify TTL was removed
    let ttl = client.execute(Ttl::new(&key)).await.unwrap();
    assert_eq!(ttl, -1);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_getdel() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("getdel");

    // Set a value
    client.execute(Set::new(&key, "value")).await.unwrap();

    // GETDEL returns value and deletes key
    let value = client.execute(GetDel::new(&key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value")));

    // Verify key was deleted
    let value: Option<Bytes> = client.execute(Get::new(&key)).await.unwrap();
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_getdel_nonexistent() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("getdel_nonexistent");

    // GETDEL on non-existent key returns None
    let value = client.execute(GetDel::new(&key)).await.unwrap();
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_get_nonexistent() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("nonexistent");

    let value: Option<Bytes> = client.execute(Get::new(&key)).await.unwrap();
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_expire_after_set() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("set_ex");

    // SET a value then EXPIRE it
    client.execute(Set::new(&key, "value")).await.unwrap();
    client.execute(Expire::new(&key, 2)).await.unwrap();

    // Value should exist
    let value: Option<Bytes> = client.execute(Get::new(&key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value")));

    // TTL should be approximately 2 seconds
    let ttl = client.execute(Ttl::new(&key)).await.unwrap();
    assert!(ttl > 0 && ttl <= 2);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_incr_decr() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("incr_decr");

    // INCR on non-existent key starts at 0
    let value = client.execute(Incr::new(&key)).await.unwrap();
    assert_eq!(value, 1);

    // INCR again
    let value = client.execute(Incr::new(&key)).await.unwrap();
    assert_eq!(value, 2);

    // INCR again
    let value = client.execute(Incr::new(&key)).await.unwrap();
    assert_eq!(value, 3);

    // DECR
    let value = client.execute(Decr::new(&key)).await.unwrap();
    assert_eq!(value, 2);

    // DECR
    let value = client.execute(Decr::new(&key)).await.unwrap();
    assert_eq!(value, 1);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_exists() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key1 = test_key("exists1");
    let key2 = test_key("exists2");
    let key3 = test_key("exists3");

    // No keys exist initially
    let count = client
        .execute(Exists::multiple(vec![key1.clone(), key2.clone()]))
        .await
        .unwrap();
    assert_eq!(count, 0);

    // Set one key
    client.execute(Set::new(&key1, "value")).await.unwrap();

    // One key exists
    let count = client
        .execute(Exists::multiple(vec![key1.clone(), key2.clone()]))
        .await
        .unwrap();
    assert_eq!(count, 1);

    // Set another key
    client.execute(Set::new(&key2, "value")).await.unwrap();

    // Two keys exist
    let count = client
        .execute(Exists::multiple(vec![key1.clone(), key2.clone()]))
        .await
        .unwrap();
    assert_eq!(count, 2);

    // Check with non-existent key
    let count = client
        .execute(Exists::multiple(vec![
            key1.clone(),
            key2.clone(),
            key3.clone(),
        ]))
        .await
        .unwrap();
    assert_eq!(count, 2);

    // Clean up
    client.execute(Del::new(vec![key1, key2])).await.unwrap();
}

#[tokio::test]
async fn test_del() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key1 = test_key("del1");
    let key2 = test_key("del2");
    let key3 = test_key("del3");

    // Set some keys
    client.execute(Set::new(&key1, "value")).await.unwrap();
    client.execute(Set::new(&key2, "value")).await.unwrap();

    // DEL should return number of keys deleted
    let deleted = client
        .execute(Del::new(vec![key1.clone(), key2.clone(), key3.clone()]))
        .await
        .unwrap();
    assert_eq!(deleted, 2); // Only 2 existed

    // Keys should be gone
    let count = client
        .execute(Exists::multiple(vec![key1, key2]))
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_expire_ttl() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("expire_ttl");

    // Set a key
    client.execute(Set::new(&key, "value")).await.unwrap();

    // Key should have no expiration (-1)
    let ttl = client.execute(Ttl::new(&key)).await.unwrap();
    assert_eq!(ttl, -1);

    // Set expiration to 10 seconds
    let result = client.execute(Expire::new(&key, 10)).await.unwrap();
    assert!(result); // true = expiration set

    // TTL should be approximately 10 seconds
    let ttl = client.execute(Ttl::new(&key)).await.unwrap();
    assert!(ttl > 0 && ttl <= 10);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_ttl_nonexistent() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("ttl_nonexistent");

    // TTL on non-existent key should return -2
    let ttl = client.execute(Ttl::new(&key)).await.unwrap();
    assert_eq!(ttl, -2);
}

#[tokio::test]
async fn test_set_get_binary_data() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("binary");

    // Binary data with null bytes
    let binary_data = Bytes::from_static(&[0x00, 0x01, 0xFF, 0xFE, 0x42, 0x00]);

    // SET binary data
    client
        .execute(Set::new(&key, binary_data.clone()))
        .await
        .unwrap();

    // GET binary data back
    let value: Option<Bytes> = client.execute(Get::new(&key)).await.unwrap();
    assert_eq!(value, Some(binary_data));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_large_value() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("large");

    // Create a large value (1 MB)
    let large_value = Bytes::from("x".repeat(1024 * 1024));

    // SET large value
    client
        .execute(Set::new(&key, large_value.clone()))
        .await
        .unwrap();

    // GET large value back
    let value: Option<Bytes> = client.execute(Get::new(&key)).await.unwrap();
    assert_eq!(value, Some(large_value));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}
