//! Integration tests for Hash commands

mod common;

use bytes::Bytes;
use common::{connect, test_key};
use redis_tower::commands::{
    Del, HDel, HExists, HGet, HGetAll, HIncrBy, HIncrByFloat, HKeys, HLen, HMGet, HSet, HStrLen,
    HVals,
};

#[tokio::test]
async fn test_hset_hget() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("hash_set_get");

    // HSET a field
    let result: i64 = client
        .execute(HSet::new(&key, "field1", "value1"))
        .await
        .unwrap();
    assert_eq!(result, 1); // 1 = new field created

    // HGET the field
    let value: Option<Bytes> = client.execute(HGet::new(&key, "field1")).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value1")));

    // HSET existing field
    let result: i64 = client
        .execute(HSet::new(&key, "field1", "value2"))
        .await
        .unwrap();
    assert_eq!(result, 0); // 0 = field updated

    // Verify updated value
    let value: Option<Bytes> = client.execute(HGet::new(&key, "field1")).await.unwrap();
    assert_eq!(value, Some(Bytes::from_static(b"value2")));

    // Clean up
    let _: i64 = client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_hexists() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("hash_exists");

    // Field doesn't exist yet
    let exists: bool = client.execute(HExists::new(&key, "field1")).await.unwrap();
    assert!(!exists);

    // Set field
    let _: i64 = client
        .execute(HSet::new(&key, "field1", "value1"))
        .await
        .unwrap();

    // Field exists now
    let exists: bool = client.execute(HExists::new(&key, "field1")).await.unwrap();
    assert!(exists);

    // Different field doesn't exist
    let exists: bool = client.execute(HExists::new(&key, "field2")).await.unwrap();
    assert!(!exists);

    // Clean up
    let _: i64 = client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_hlen() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("hash_len");

    // Empty hash
    let len = client.execute(HLen::new(&key)).await.unwrap();
    assert_eq!(len, 0);

    // Add fields
    client
        .execute(HSet::new(&key, "field1", "value1"))
        .await
        .unwrap();
    client
        .execute(HSet::new(&key, "field2", "value2"))
        .await
        .unwrap();
    client
        .execute(HSet::new(&key, "field3", "value3"))
        .await
        .unwrap();

    // Should have 3 fields
    let len = client.execute(HLen::new(&key)).await.unwrap();
    assert_eq!(len, 3);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_hkeys() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("hash_keys");

    // Add fields
    client
        .execute(HSet::new(&key, "field1", "value1"))
        .await
        .unwrap();
    client
        .execute(HSet::new(&key, "field2", "value2"))
        .await
        .unwrap();
    client
        .execute(HSet::new(&key, "field3", "value3"))
        .await
        .unwrap();

    // Get all keys
    let keys: Vec<String> = client.execute(HKeys::new(&key)).await.unwrap();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&"field1".to_string()));
    assert!(keys.contains(&"field2".to_string()));
    assert!(keys.contains(&"field3".to_string()));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_hvals() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("hash_vals");

    // Add fields
    client
        .execute(HSet::new(&key, "field1", "value1"))
        .await
        .unwrap();
    client
        .execute(HSet::new(&key, "field2", "value2"))
        .await
        .unwrap();
    client
        .execute(HSet::new(&key, "field3", "value3"))
        .await
        .unwrap();

    // Get all values
    let values: Vec<Bytes> = client.execute(HVals::new(&key)).await.unwrap();
    assert_eq!(values.len(), 3);
    assert!(values.iter().any(|v| v.as_ref() == b"value1"));
    assert!(values.iter().any(|v| v.as_ref() == b"value2"));
    assert!(values.iter().any(|v| v.as_ref() == b"value3"));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_hmget() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("hash_mget");

    // Add fields
    client
        .execute(HSet::new(&key, "field1", "value1"))
        .await
        .unwrap();
    client
        .execute(HSet::new(&key, "field2", "value2"))
        .await
        .unwrap();
    client
        .execute(HSet::new(&key, "field3", "value3"))
        .await
        .unwrap();

    // Get multiple fields
    let values: Vec<Option<Bytes>> = client
        .execute(HMGet::new(
            &key,
            vec![
                "field1".to_string(),
                "field2".to_string(),
                "nonexistent".to_string(),
            ],
        ))
        .await
        .unwrap();

    assert_eq!(values.len(), 3);
    assert_eq!(values[0], Some(Bytes::from_static(b"value1")));
    assert_eq!(values[1], Some(Bytes::from_static(b"value2")));
    assert_eq!(values[2], None); // nonexistent field

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_hmget_single() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("hash_mget_single");

    client
        .execute(HSet::new(&key, "field1", "value1"))
        .await
        .unwrap();

    // Use convenience method for single field
    let values: Vec<Option<Bytes>> = client.execute(HMGet::single(&key, "field1")).await.unwrap();

    assert_eq!(values.len(), 1);
    assert_eq!(values[0], Some(Bytes::from_static(b"value1")));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_hincrby() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("hash_incrby");

    // HINCRBY on non-existent field starts at 0
    let value = client
        .execute(HIncrBy::new(&key, "counter", 5))
        .await
        .unwrap();
    assert_eq!(value, 5);

    // HINCRBY again
    let value = client
        .execute(HIncrBy::new(&key, "counter", 10))
        .await
        .unwrap();
    assert_eq!(value, 15);

    // HINCRBY with negative value (decrement)
    let value = client
        .execute(HIncrBy::new(&key, "counter", -3))
        .await
        .unwrap();
    assert_eq!(value, 12);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_hincrbyfloat() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("hash_incrbyfloat");

    // HINCRBYFLOAT on non-existent field starts at 0.0
    let value = client
        .execute(HIncrByFloat::new(&key, "counter", 2.5))
        .await
        .unwrap();
    assert!((value - 2.5).abs() < 0.001);

    // HINCRBYFLOAT again
    let value = client
        .execute(HIncrByFloat::new(&key, "counter", 3.7))
        .await
        .unwrap();
    assert!((value - 6.2).abs() < 0.001);

    // HINCRBYFLOAT with negative value (decrement)
    let value = client
        .execute(HIncrByFloat::new(&key, "counter", -1.2))
        .await
        .unwrap();
    assert!((value - 5.0).abs() < 0.001);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_hstrlen() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("hash_strlen");

    // Non-existent field returns 0
    let len = client.execute(HStrLen::new(&key, "field1")).await.unwrap();
    assert_eq!(len, 0);

    // Set a field
    client
        .execute(HSet::new(&key, "field1", "Hello"))
        .await
        .unwrap();

    // Should return length of "Hello"
    let len = client.execute(HStrLen::new(&key, "field1")).await.unwrap();
    assert_eq!(len, 5);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_hdel() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("hash_del");

    // Add fields
    client
        .execute(HSet::new(&key, "field1", "value1"))
        .await
        .unwrap();
    client
        .execute(HSet::new(&key, "field2", "value2"))
        .await
        .unwrap();
    client
        .execute(HSet::new(&key, "field3", "value3"))
        .await
        .unwrap();

    // Delete multiple fields
    let deleted = client
        .execute(HDel::new(
            &key,
            vec!["field1".to_string(), "field2".to_string()],
        ))
        .await
        .unwrap();
    assert_eq!(deleted, 2);

    // Verify deletion
    let len = client.execute(HLen::new(&key)).await.unwrap();
    assert_eq!(len, 1);

    // field3 should still exist
    let exists = client.execute(HExists::new(&key, "field3")).await.unwrap();
    assert!(exists);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_hdel_single() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("hash_del_single");

    client
        .execute(HSet::new(&key, "field1", "value1"))
        .await
        .unwrap();

    // Use convenience method for single field
    let deleted = client.execute(HDel::single(&key, "field1")).await.unwrap();
    assert_eq!(deleted, 1);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_hgetall() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("hash_getall");

    // Add fields
    client
        .execute(HSet::new(&key, "field1", "value1"))
        .await
        .unwrap();
    client
        .execute(HSet::new(&key, "field2", "value2"))
        .await
        .unwrap();
    client
        .execute(HSet::new(&key, "field3", "value3"))
        .await
        .unwrap();

    // Get all fields and values
    let hash = client.execute(HGetAll::new(&key)).await.unwrap();
    assert_eq!(hash.len(), 3);
    assert_eq!(hash.get("field1").unwrap().as_ref(), b"value1");
    assert_eq!(hash.get("field2").unwrap().as_ref(), b"value2");
    assert_eq!(hash.get("field3").unwrap().as_ref(), b"value3");

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}
