//! Integration tests for Transaction commands (MULTI/EXEC/WATCH).
//!
//! These tests require a running Redis instance on localhost:6379.
//! Run with: cargo test --test test_transactions

mod common;

use bytes::Bytes;
use common::{connect, test_key};
use redis_tower::commands::{Del, Get, Incr, Set};
use redis_tower::{RedisValue, Transaction};

#[tokio::test]
async fn test_basic_transaction() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("tx_basic");

    // Create transaction and queue commands
    let mut tx = Transaction::new(&client);
    tx.queue(Set::new(&key, "value1")).await.unwrap();
    tx.queue(Get::new(&key)).await.unwrap();
    tx.queue(Set::new(&key, "value2")).await.unwrap();
    tx.queue(Get::new(&key)).await.unwrap();

    // Execute transaction
    let results = tx.exec().await.unwrap().expect("Transaction succeeded");

    // Verify results
    assert_eq!(results.len(), 4);
    // SET returns nil/OK in transaction
    // GET should return the values
    if let RedisValue::BulkString(ref bytes) = results[1] {
        assert_eq!(bytes, &Bytes::from_static(b"value1"));
    } else {
        panic!("Expected BulkString, got {:?}", results[1]);
    }

    if let RedisValue::BulkString(ref bytes) = results[3] {
        assert_eq!(bytes, &Bytes::from_static(b"value2"));
    } else {
        panic!("Expected BulkString, got {:?}", results[3]);
    }

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_transaction_incr() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("tx_incr");

    // Set initial value
    client.execute(Set::new(&key, "10")).await.unwrap();

    // Transaction with multiple increments
    let mut tx = Transaction::new(&client);
    tx.queue(Incr::new(&key)).await.unwrap();
    tx.queue(Incr::new(&key)).await.unwrap();
    tx.queue(Incr::new(&key)).await.unwrap();

    let results = tx.exec().await.unwrap().expect("Transaction succeeded");

    assert_eq!(results.len(), 3);
    // Each INCR should return the incremented value
    assert_eq!(results[0], RedisValue::Integer(11));
    assert_eq!(results[1], RedisValue::Integer(12));
    assert_eq!(results[2], RedisValue::Integer(13));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_empty_transaction() {
    let client = connect().await.expect("Failed to connect to Redis");

    // Execute empty transaction - should error since no commands queued
    let tx = Transaction::new(&client);
    let result = tx.exec().await;

    // Empty transactions are not allowed (this is by design)
    assert!(result.is_err());
}

#[tokio::test]
async fn test_transaction_multiple_keys() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key1 = test_key("tx_multi1");
    let key2 = test_key("tx_multi2");
    let key3 = test_key("tx_multi3");

    // Transaction affecting multiple keys
    let mut tx = Transaction::new(&client);
    tx.queue(Set::new(&key1, "a")).await.unwrap();
    tx.queue(Set::new(&key2, "b")).await.unwrap();
    tx.queue(Set::new(&key3, "c")).await.unwrap();
    tx.queue(Get::new(&key1)).await.unwrap();
    tx.queue(Get::new(&key2)).await.unwrap();
    tx.queue(Get::new(&key3)).await.unwrap();

    let results = tx.exec().await.unwrap().expect("Transaction succeeded");

    assert_eq!(results.len(), 6);

    // Verify the GETs returned correct values
    if let RedisValue::BulkString(ref bytes) = results[3] {
        assert_eq!(bytes, &Bytes::from_static(b"a"));
    }
    if let RedisValue::BulkString(ref bytes) = results[4] {
        assert_eq!(bytes, &Bytes::from_static(b"b"));
    }
    if let RedisValue::BulkString(ref bytes) = results[5] {
        assert_eq!(bytes, &Bytes::from_static(b"c"));
    }

    // Clean up
    client
        .execute(Del::new(vec![key1, key2, key3]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_transaction_atomicity() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("tx_atomic");

    // Set initial value
    client.execute(Set::new(&key, "0")).await.unwrap();

    // Run transaction
    let mut tx = Transaction::new(&client);
    for _ in 0..10 {
        tx.queue(Incr::new(&key)).await.unwrap();
    }
    let results = tx.exec().await.unwrap().expect("Transaction succeeded");

    assert_eq!(results.len(), 10);
    // Last result should be 10
    assert_eq!(results[9], RedisValue::Integer(10));

    // Verify final value
    let value: Option<Bytes> = client.execute(Get::new(&key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"10")));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_transaction_with_get() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("tx_get");

    // Set a value first
    client.execute(Set::new(&key, "initial")).await.unwrap();

    // Transaction that reads and writes
    let mut tx = Transaction::new(&client);
    tx.queue(Get::new(&key)).await.unwrap();
    tx.queue(Set::new(&key, "updated")).await.unwrap();
    tx.queue(Get::new(&key)).await.unwrap();

    let results = tx.exec().await.unwrap().expect("Transaction succeeded");

    assert_eq!(results.len(), 3);

    // First GET should return "initial"
    if let RedisValue::BulkString(ref bytes) = results[0] {
        assert_eq!(bytes, &Bytes::from_static(b"initial"));
    }

    // Last GET should return "updated"
    if let RedisValue::BulkString(ref bytes) = results[2] {
        assert_eq!(bytes, &Bytes::from_static(b"updated"));
    }

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_transaction_nil_values() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("tx_nil");

    // Transaction that reads non-existent key
    let mut tx = Transaction::new(&client);
    tx.queue(Get::new(&key)).await.unwrap();
    tx.queue(Set::new(&key, "exists")).await.unwrap();
    tx.queue(Get::new(&key)).await.unwrap();

    let results = tx.exec().await.unwrap().expect("Transaction succeeded");

    assert_eq!(results.len(), 3);

    // First GET should return Nil (key doesn't exist)
    assert_eq!(results[0], RedisValue::Nil);

    // Last GET should return the value
    if let RedisValue::BulkString(ref bytes) = results[2] {
        assert_eq!(bytes, &Bytes::from_static(b"exists"));
    }

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_multiple_sequential_transactions() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("tx_sequential");

    // First transaction
    let mut tx1 = Transaction::new(&client);
    tx1.queue(Set::new(&key, "tx1")).await.unwrap();
    tx1.exec().await.unwrap().expect("Transaction 1 succeeded");

    // Second transaction
    let mut tx2 = Transaction::new(&client);
    tx2.queue(Get::new(&key)).await.unwrap();
    tx2.queue(Set::new(&key, "tx2")).await.unwrap();
    let results2 = tx2.exec().await.unwrap().expect("Transaction 2 succeeded");

    // Verify second transaction saw first transaction's result
    if let RedisValue::BulkString(ref bytes) = results2[0] {
        assert_eq!(bytes, &Bytes::from_static(b"tx1"));
    }

    // Third transaction
    let mut tx3 = Transaction::new(&client);
    tx3.queue(Get::new(&key)).await.unwrap();
    let results3 = tx3.exec().await.unwrap().expect("Transaction 3 succeeded");

    // Verify third transaction saw second transaction's result
    if let RedisValue::BulkString(ref bytes) = results3[0] {
        assert_eq!(bytes, &Bytes::from_static(b"tx2"));
    }

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}
