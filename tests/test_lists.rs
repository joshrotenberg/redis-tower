//! Integration tests for List commands

mod common;

use bytes::Bytes;
use common::{connect, test_key};
use redis_tower::commands::{
    Del, InsertPosition, LIndex, LInsert, LLen, LPop, LPos, LPush, LRange, LRem, LSet, LTrim, RPop,
    RPush,
};

#[tokio::test]
async fn test_lpush_lpop() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("list_lpush_lpop");

    // LPUSH multiple values
    let len = client
        .execute(LPush::new(
            &key,
            vec![Bytes::from("value1"), Bytes::from("value2")],
        ))
        .await
        .unwrap();
    assert_eq!(len, 2);

    // LPOP
    let value: Option<Bytes> = client.execute(LPop::new(&key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value2"))); // LIFO order

    let value: Option<Bytes> = client.execute(LPop::new(&key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value1")));

    // Empty list
    let value: Option<Bytes> = client.execute(LPop::new(&key)).await.unwrap();
    assert_eq!(value, None);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_rpush_rpop() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("list_rpush_rpop");

    // RPUSH multiple values
    let len = client
        .execute(RPush::new(
            &key,
            vec![Bytes::from("value1"), Bytes::from("value2")],
        ))
        .await
        .unwrap();
    assert_eq!(len, 2);

    // RPOP
    let value: Option<Bytes> = client.execute(RPop::new(&key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value2"))); // LIFO from right

    let value: Option<Bytes> = client.execute(RPop::new(&key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value1")));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_llen() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("list_len");

    // Empty list
    let len = client.execute(LLen::new(&key)).await.unwrap();
    assert_eq!(len, 0);

    // Push values
    client
        .execute(LPush::new(
            &key,
            vec![
                Bytes::from("value1"),
                Bytes::from("value2"),
                Bytes::from("value3"),
            ],
        ))
        .await
        .unwrap();

    // Should have 3 elements
    let len = client.execute(LLen::new(&key)).await.unwrap();
    assert_eq!(len, 3);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_lindex() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("list_index");

    // Push values
    client
        .execute(RPush::new(
            &key,
            vec![
                Bytes::from("first"),
                Bytes::from("second"),
                Bytes::from("third"),
            ],
        ))
        .await
        .unwrap();

    // Index 0 (first element)
    let value: Option<Bytes> = client.execute(LIndex::new(&key, 0)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"first")));

    // Index 1 (second element)
    let value: Option<Bytes> = client.execute(LIndex::new(&key, 1)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"second")));

    // Index -1 (last element)
    let value: Option<Bytes> = client.execute(LIndex::new(&key, -1)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"third")));

    // Out of range
    let value: Option<Bytes> = client.execute(LIndex::new(&key, 10)).await.unwrap();
    assert_eq!(value, None);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_lset() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("list_set");

    // Push values
    client
        .execute(RPush::new(
            &key,
            vec![
                Bytes::from("first"),
                Bytes::from("second"),
                Bytes::from("third"),
            ],
        ))
        .await
        .unwrap();

    // Set index 1
    client
        .execute(LSet::new(&key, 1, "modified"))
        .await
        .unwrap();

    // Verify change
    let value: Option<Bytes> = client.execute(LIndex::new(&key, 1)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"modified")));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_linsert() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("list_insert");

    // Push values
    client
        .execute(RPush::new(
            &key,
            vec![Bytes::from("first"), Bytes::from("third")],
        ))
        .await
        .unwrap();

    // Insert before "third"
    let len = client
        .execute(LInsert::before(&key, "third", "second"))
        .await
        .unwrap();
    assert_eq!(len, 3);

    // Verify order
    let values: Vec<Bytes> = client.execute(LRange::new(&key, 0, -1)).await.unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_ref(), b"first");
    assert_eq!(values[1].as_ref(), b"second");
    assert_eq!(values[2].as_ref(), b"third");

    // Insert after "second"
    let len = client
        .execute(LInsert::after(&key, "second", "middle"))
        .await
        .unwrap();
    assert_eq!(len, 4);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_lrem() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("list_rem");

    // Push values with duplicates
    client
        .execute(RPush::new(
            &key,
            vec![
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("a"),
                Bytes::from("c"),
                Bytes::from("a"),
            ],
        ))
        .await
        .unwrap();

    // Remove 2 occurrences of "a" from the left
    let removed = client.execute(LRem::new(&key, 2, "a")).await.unwrap();
    assert_eq!(removed, 2);

    // Should have "b", "c", "a" left
    let values: Vec<Bytes> = client.execute(LRange::new(&key, 0, -1)).await.unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_ref(), b"b");
    assert_eq!(values[1].as_ref(), b"c");
    assert_eq!(values[2].as_ref(), b"a");

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_lrem_all() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("list_rem_all");

    // Push values with duplicates
    client
        .execute(RPush::new(
            &key,
            vec![
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("a"),
                Bytes::from("c"),
                Bytes::from("a"),
            ],
        ))
        .await
        .unwrap();

    // Remove all occurrences using convenience method
    let removed = client.execute(LRem::all(&key, "a")).await.unwrap();
    assert_eq!(removed, 3);

    // Should have "b", "c" left
    let values: Vec<Bytes> = client.execute(LRange::new(&key, 0, -1)).await.unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].as_ref(), b"b");
    assert_eq!(values[1].as_ref(), b"c");

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_ltrim() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("list_trim");

    // Push values
    client
        .execute(RPush::new(
            &key,
            vec![
                Bytes::from("one"),
                Bytes::from("two"),
                Bytes::from("three"),
                Bytes::from("four"),
                Bytes::from("five"),
            ],
        ))
        .await
        .unwrap();

    // Trim to keep only indices 1-3
    client.execute(LTrim::new(&key, 1, 3)).await.unwrap();

    // Should have "two", "three", "four"
    let values: Vec<Bytes> = client.execute(LRange::new(&key, 0, -1)).await.unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_ref(), b"two");
    assert_eq!(values[1].as_ref(), b"three");
    assert_eq!(values[2].as_ref(), b"four");

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_lpos() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("list_pos");

    // Push values with duplicates
    client
        .execute(RPush::new(
            &key,
            vec![
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("c"),
                Bytes::from("b"),
                Bytes::from("d"),
            ],
        ))
        .await
        .unwrap();

    // Find first occurrence of "b"
    let position: Option<i64> = client.execute(LPos::new(&key, "b")).await.unwrap();
    assert_eq!(position, Some(1));

    // Element not found
    let position: Option<i64> = client.execute(LPos::new(&key, "z")).await.unwrap();
    assert_eq!(position, None);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_lrange() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("list_range");

    // Push values
    client
        .execute(RPush::new(
            &key,
            vec![
                Bytes::from("one"),
                Bytes::from("two"),
                Bytes::from("three"),
                Bytes::from("four"),
                Bytes::from("five"),
            ],
        ))
        .await
        .unwrap();

    // Get all elements
    let values: Vec<Bytes> = client.execute(LRange::all(&key)).await.unwrap();
    assert_eq!(values.len(), 5);
    assert_eq!(values[0].as_ref(), b"one");
    assert_eq!(values[4].as_ref(), b"five");

    // Get range
    let values: Vec<Bytes> = client.execute(LRange::new(&key, 1, 3)).await.unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_ref(), b"two");
    assert_eq!(values[1].as_ref(), b"three");
    assert_eq!(values[2].as_ref(), b"four");

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_lpush_single() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("list_lpush_single");

    // Use convenience method for single value
    let len = client.execute(LPush::single(&key, "value")).await.unwrap();
    assert_eq!(len, 1);

    let value: Option<Bytes> = client.execute(LPop::new(&key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value")));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_rpush_single() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("list_rpush_single");

    // Use convenience method for single value
    let len = client.execute(RPush::single(&key, "value")).await.unwrap();
    assert_eq!(len, 1);

    let value: Option<Bytes> = client.execute(RPop::new(&key)).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value")));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}
