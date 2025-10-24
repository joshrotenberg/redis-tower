//! Integration tests for OBJECT commands

mod common;

use common::{connect, test_key};
use redis_tower::commands::{Del, ObjectEncoding, ObjectFreq, ObjectIdleTime, ObjectRefCount, Set};

#[tokio::test]
async fn test_object_encoding() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("obj_encoding");

    // Set a string value
    client
        .execute(Set::new(&key, "hello"))
        .await
        .expect("SET should succeed");

    // Get encoding
    let encoding: Option<String> = client
        .execute(ObjectEncoding::new(&key))
        .await
        .expect("OBJECT ENCODING should succeed");

    assert!(encoding.is_some());
    // Common encodings: "embstr", "raw", "int"
    let enc = encoding.unwrap();
    assert!(
        enc == "embstr" || enc == "raw" || enc == "int",
        "Expected valid encoding, got: {}",
        enc
    );

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_object_refcount() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("obj_refcount");

    // Set a value
    client
        .execute(Set::new(&key, "value"))
        .await
        .expect("SET should succeed");

    // Get refcount
    let refcount: Option<i64> = client
        .execute(ObjectRefCount::new(&key))
        .await
        .expect("OBJECT REFCOUNT should succeed");

    // Redis usually returns 1 for single references
    assert!(refcount.is_some());
    assert!(refcount.unwrap() >= 1);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_object_idletime() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("obj_idletime");

    // Set a value
    client
        .execute(Set::new(&key, "value"))
        .await
        .expect("SET should succeed");

    // Get idletime (should be 0 since we just set it)
    let idletime: Option<i64> = client
        .execute(ObjectIdleTime::new(&key))
        .await
        .expect("OBJECT IDLETIME should succeed");

    assert!(idletime.is_some());
    // Should be very low since we just created it
    assert!(idletime.unwrap() <= 1);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_object_freq() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("obj_freq");

    // Note: OBJECT FREQ requires maxmemory-policy to be set to an LFU policy
    // This test might return None if the policy isn't LFU, or error if not supported
    client
        .execute(Set::new(&key, "value"))
        .await
        .expect("SET should succeed");

    // Get freq - may fail if Redis config doesn't use LFU eviction policy
    let result: Result<Option<i64>, _> = client.execute(ObjectFreq::new(&key)).await;

    // Either succeeds with Some/None, or fails with error about wrong eviction policy
    // Both are acceptable outcomes
    match result {
        Ok(_) => {
            // Success - LFU is enabled
        }
        Err(e) => {
            // Expected error if LFU not configured
            let err_str = format!("{:?}", e);
            assert!(
                err_str.contains("wrong")
                    || err_str.contains("LFU")
                    || err_str.contains("requires")
            );
        }
    }

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_object_nonexistent_key() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("obj_nonexistent");

    // OBJECT commands on nonexistent keys should return None
    let encoding: Option<String> = client
        .execute(ObjectEncoding::new(&key))
        .await
        .expect("OBJECT ENCODING should succeed");
    assert!(encoding.is_none());

    let refcount: Option<i64> = client
        .execute(ObjectRefCount::new(&key))
        .await
        .expect("OBJECT REFCOUNT should succeed");
    assert!(refcount.is_none());

    let idletime: Option<i64> = client
        .execute(ObjectIdleTime::new(&key))
        .await
        .expect("OBJECT IDLETIME should succeed");
    assert!(idletime.is_none());
}
