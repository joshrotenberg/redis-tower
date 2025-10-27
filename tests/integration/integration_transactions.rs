mod helpers;

use bytes::Bytes;
use helpers::standalone::setup_redis_connection;
use redis_tower::commands::strings::{Del, Get, Incr, Set};
use redis_tower::transaction::{Transaction, Unwatch, Watch};

#[tokio::test]
async fn test_basic_transaction() {
    let conn = setup_redis_connection().await;

    // Create transaction and queue commands
    let mut tx = Transaction::new(&conn);
    tx.queue(Set::new("tx_key1", "value1")).await.unwrap();
    tx.queue(Set::new("tx_key2", "value2")).await.unwrap();

    // Execute atomically
    let results = tx.exec().await.unwrap();
    assert!(results.is_some());
    let results = results.unwrap();
    assert_eq!(results.len(), 2);

    // Verify both keys were set
    let val1: Option<Bytes> = conn.execute(Get::new("tx_key1")).await.unwrap();
    assert_eq!(val1.as_ref().map(|b| b.as_ref()), Some(b"value1".as_ref()));

    let val2: Option<Bytes> = conn.execute(Get::new("tx_key2")).await.unwrap();
    assert_eq!(val2.as_ref().map(|b| b.as_ref()), Some(b"value2".as_ref()));
}

#[tokio::test]
async fn test_discard_transaction() {
    let conn = setup_redis_connection().await;

    // Start transaction and queue command
    let mut tx = Transaction::new(&conn);
    tx.queue(Set::new("tx_discard_key", "should_not_exist"))
        .await
        .unwrap();

    // Discard the transaction
    tx.discard().await.unwrap();

    // Key should not exist
    let val: Option<Bytes> = conn.execute(Get::new("tx_discard_key")).await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn test_transaction_with_incr() {
    let conn = setup_redis_connection().await;

    // Set initial value
    conn.execute(Set::new("tx_counter", "0")).await.unwrap();

    // Transaction with multiple increments
    let mut tx = Transaction::new(&conn);
    tx.queue(Incr::new("tx_counter")).await.unwrap();
    tx.queue(Incr::new("tx_counter")).await.unwrap();
    tx.queue(Incr::new("tx_counter")).await.unwrap();

    let results = tx.exec().await.unwrap();
    assert!(results.is_some());
    assert_eq!(results.unwrap().len(), 3);

    // All increments should have been atomic
    let final_val: Option<Bytes> = conn.execute(Get::new("tx_counter")).await.unwrap();
    assert_eq!(
        final_val
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).parse::<i64>().unwrap()),
        Some(3)
    );
}

#[tokio::test]
async fn test_transaction_return_values() {
    let conn = setup_redis_connection().await;

    let mut tx = Transaction::new(&conn);
    tx.queue(Set::new("k1", "v1")).await.unwrap();
    tx.queue(Set::new("k2", "v2")).await.unwrap();
    tx.queue(Get::new("k1")).await.unwrap();

    // EXEC returns all results
    let results = tx.exec().await.unwrap();
    assert!(results.is_some());
    let results = results.unwrap();
    assert_eq!(results.len(), 3);

    // Third result should be the GET result
    if let Some(last) = results.get(2) {
        let bytes = last.as_bytes().unwrap().unwrap();
        assert_eq!(bytes.as_ref(), b"v1");
    }
}

#[tokio::test]
async fn test_empty_transaction() {
    let conn = setup_redis_connection().await;

    // Execute empty transaction - should fail
    let tx = Transaction::new(&conn);
    let result = tx.exec().await;

    // Should return error since no commands were queued
    assert!(result.is_err());
}

#[tokio::test]
async fn test_multiple_transactions() {
    let conn = setup_redis_connection().await;

    // First transaction
    let mut tx1 = Transaction::new(&conn);
    tx1.queue(Set::new("tx_seq_1", "first")).await.unwrap();
    tx1.exec().await.unwrap();

    // Second transaction
    let mut tx2 = Transaction::new(&conn);
    tx2.queue(Set::new("tx_seq_2", "second")).await.unwrap();
    tx2.exec().await.unwrap();

    // Verify both executed
    let val1: Option<Bytes> = conn.execute(Get::new("tx_seq_1")).await.unwrap();
    assert_eq!(val1.as_ref().map(|b| b.as_ref()), Some(b"first".as_ref()));

    let val2: Option<Bytes> = conn.execute(Get::new("tx_seq_2")).await.unwrap();
    assert_eq!(val2.as_ref().map(|b| b.as_ref()), Some(b"second".as_ref()));
}

#[tokio::test]
async fn test_transaction_with_watch() {
    let conn = setup_redis_connection().await;

    // Set up a key to watch
    conn.execute(Set::new("watched_key", "initial"))
        .await
        .unwrap();

    // Watch the key
    conn.execute(Watch::new("watched_key")).await.unwrap();

    // Modify it outside the transaction (this will cause EXEC to abort)
    conn.execute(Set::new("watched_key", "modified"))
        .await
        .unwrap();

    // Try to execute transaction - should return None (aborted)
    let mut tx = Transaction::new(&conn);
    tx.queue(Set::new("watched_key", "from_tx")).await.unwrap();
    let results = tx.exec().await.unwrap();

    // Transaction should be aborted (returns None)
    assert!(results.is_none());

    // Value should be the one set outside the transaction
    let val: Option<Bytes> = conn.execute(Get::new("watched_key")).await.unwrap();
    assert_eq!(val.as_ref().map(|b| b.as_ref()), Some(b"modified".as_ref()));
}

#[tokio::test]
async fn test_transaction_with_successful_watch() {
    let conn = setup_redis_connection().await;

    // Set up a key to watch
    conn.execute(Set::new("watched_key2", "initial"))
        .await
        .unwrap();

    // Watch the key
    conn.execute(Watch::new("watched_key2")).await.unwrap();

    // Don't modify it - transaction should succeed
    let mut tx = Transaction::new(&conn);
    tx.queue(Set::new("watched_key2", "from_tx")).await.unwrap();
    let results = tx.exec().await.unwrap();

    // Transaction should succeed
    assert!(results.is_some());

    // Value should be the one from the transaction
    let val: Option<Bytes> = conn.execute(Get::new("watched_key2")).await.unwrap();
    assert_eq!(val.as_ref().map(|b| b.as_ref()), Some(b"from_tx".as_ref()));
}

#[tokio::test]
async fn test_unwatch() {
    let conn = setup_redis_connection().await;

    // Set up and watch a key
    conn.execute(Set::new("watch_unwatch", "initial"))
        .await
        .unwrap();
    conn.execute(Watch::new("watch_unwatch")).await.unwrap();

    // Unwatch it
    conn.execute(Unwatch).await.unwrap();

    // Modify outside transaction (should NOT abort now)
    conn.execute(Set::new("watch_unwatch", "modified"))
        .await
        .unwrap();

    // Transaction should succeed since we unwatched
    let mut tx = Transaction::new(&conn);
    tx.queue(Set::new("watch_unwatch", "from_tx"))
        .await
        .unwrap();
    let results = tx.exec().await.unwrap();

    assert!(results.is_some());
}

#[tokio::test]
async fn test_transaction_atomicity() {
    let conn = setup_redis_connection().await;

    // Set up two counters
    conn.execute(Set::new("counter_a", "0")).await.unwrap();
    conn.execute(Set::new("counter_b", "0")).await.unwrap();

    // Atomically increment both
    let mut tx = Transaction::new(&conn);
    tx.queue(Incr::new("counter_a")).await.unwrap();
    tx.queue(Incr::new("counter_b")).await.unwrap();
    tx.exec().await.unwrap();

    // Both should be 1
    let a: Option<Bytes> = conn.execute(Get::new("counter_a")).await.unwrap();
    let b: Option<Bytes> = conn.execute(Get::new("counter_b")).await.unwrap();

    assert_eq!(
        a.as_ref()
            .map(|v| String::from_utf8_lossy(v).parse::<i64>().unwrap()),
        Some(1)
    );
    assert_eq!(
        b.as_ref()
            .map(|v| String::from_utf8_lossy(v).parse::<i64>().unwrap()),
        Some(1)
    );
}

#[tokio::test]
async fn test_transaction_with_del() {
    let conn = setup_redis_connection().await;

    // Set up some keys
    conn.execute(Set::new("del_key1", "value1")).await.unwrap();
    conn.execute(Set::new("del_key2", "value2")).await.unwrap();

    // Delete them in a transaction
    let mut tx = Transaction::new(&conn);
    tx.queue(Del::new(vec![
        "del_key1".to_string(),
        "del_key2".to_string(),
    ]))
    .await
    .unwrap();
    let results = tx.exec().await.unwrap();

    assert!(results.is_some());

    // Both should be gone
    let val1: Option<Bytes> = conn.execute(Get::new("del_key1")).await.unwrap();
    let val2: Option<Bytes> = conn.execute(Get::new("del_key2")).await.unwrap();
    assert_eq!(val1, None);
    assert_eq!(val2, None);
}

#[tokio::test]
async fn test_discard_clears_watched_keys() {
    let conn = setup_redis_connection().await;

    // Watch a key
    conn.execute(Set::new("discard_watch", "initial"))
        .await
        .unwrap();
    conn.execute(Watch::new("discard_watch")).await.unwrap();

    // Start and discard transaction
    let mut tx = Transaction::new(&conn);
    tx.queue(Set::new("discard_watch", "from_tx"))
        .await
        .unwrap();
    tx.discard().await.unwrap();

    // DISCARD clears watched keys, so modifying should NOT abort next transaction
    conn.execute(Set::new("discard_watch", "modified"))
        .await
        .unwrap();

    // This transaction should succeed
    let mut tx2 = Transaction::new(&conn);
    tx2.queue(Set::new("discard_watch", "final")).await.unwrap();
    let results = tx2.exec().await.unwrap();

    assert!(results.is_some());
}

#[tokio::test]
async fn test_watch_multiple_keys() {
    let conn = setup_redis_connection().await;

    // Set up multiple keys
    conn.execute(Set::new("multi_watch_1", "v1")).await.unwrap();
    conn.execute(Set::new("multi_watch_2", "v2")).await.unwrap();

    // Watch both
    conn.execute(Watch::new("multi_watch_1").key("multi_watch_2"))
        .await
        .unwrap();

    // Modify one of them
    conn.execute(Set::new("multi_watch_1", "modified"))
        .await
        .unwrap();

    // Transaction should abort
    let mut tx = Transaction::new(&conn);
    tx.queue(Set::new("multi_watch_2", "from_tx"))
        .await
        .unwrap();
    let results = tx.exec().await.unwrap();

    assert!(results.is_none());
}

#[tokio::test]
async fn test_transaction_mixed_commands() {
    let conn = setup_redis_connection().await;

    // Mix of different command types in one transaction
    let mut tx = Transaction::new(&conn);
    tx.queue(Set::new("mixed_1", "value1")).await.unwrap();
    tx.queue(Incr::new("mixed_counter")).await.unwrap();
    tx.queue(Get::new("mixed_1")).await.unwrap();
    tx.queue(Del::new(vec!["mixed_temp".to_string()]))
        .await
        .unwrap();

    let results = tx.exec().await.unwrap();
    assert!(results.is_some());
    assert_eq!(results.unwrap().len(), 4);
}

#[tokio::test]
async fn test_large_transaction() {
    let conn = setup_redis_connection().await;

    // Queue many commands
    let mut tx = Transaction::new(&conn);
    for i in 0..50 {
        tx.queue(Set::new(format!("large_tx_{}", i), format!("value_{}", i)))
            .await
            .unwrap();
    }

    let results = tx.exec().await.unwrap();
    assert!(results.is_some());
    assert_eq!(results.unwrap().len(), 50);

    // Verify a few keys
    let val0: Option<Bytes> = conn.execute(Get::new("large_tx_0")).await.unwrap();
    assert_eq!(val0.as_ref().map(|b| b.as_ref()), Some(b"value_0".as_ref()));

    let val49: Option<Bytes> = conn.execute(Get::new("large_tx_49")).await.unwrap();
    assert_eq!(
        val49.as_ref().map(|b| b.as_ref()),
        Some(b"value_49".as_ref())
    );
}
