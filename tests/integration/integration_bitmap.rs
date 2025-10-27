//! Integration tests for bitmap operations
//!
//! Tests bit operations like SETBIT, GETBIT, BITCOUNT, BITOP, etc.
//!
//! Run with: cargo test --test integration_bitmap

mod helpers;

use bytes::Bytes;
use helpers::standalone::setup_redis;
use redis_tower::commands::*;

#[tokio::test]
async fn test_setbit_getbit() {
    let client = setup_redis().await;

    // Set bit at offset 7 to 1
    let old_value: bool = client.call(SetBit::new("bitkey", 7, true)).await.unwrap();
    assert!(!old_value); // Was previously false

    // Get the bit we just set
    let bit_value: bool = client.call(GetBit::new("bitkey", 7)).await.unwrap();
    assert!(bit_value);

    // Get a bit that wasn't set
    let bit_value: bool = client.call(GetBit::new("bitkey", 100)).await.unwrap();
    assert!(!bit_value);

    // Clean up
    client
        .call(Del::new(vec!["bitkey".to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_bitcount() {
    let client = setup_redis().await;

    // Set a value with known bit count
    // "a" = 0x61 = 0b01100001 (3 bits set)
    client.call(Set::new("bitcount_key", "a")).await.unwrap();

    let count: i64 = client.call(BitCount::new("bitcount_key")).await.unwrap();
    assert_eq!(count, 3);

    // Clean up
    client
        .call(Del::new(vec!["bitcount_key".to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_bitcount_range() {
    let client = setup_redis().await;

    // Set a longer value
    client
        .call(Set::new("bitcount_range", "foobar"))
        .await
        .unwrap();

    // Count bits in byte range
    let count: i64 = client
        .call(BitCount::new("bitcount_range").range(0, 0))
        .await
        .unwrap();
    // First byte is 'f' = 0x66 = 0b01100110 (4 bits set)
    assert_eq!(count, 4);

    // Clean up
    client
        .call(Del::new(vec!["bitcount_range".to_string()]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_bitop_and() {
    let client = setup_redis().await;

    // Set up source keys
    client.call(Set::new("bitop1", "foo")).await.unwrap();
    client.call(Set::new("bitop2", "bar")).await.unwrap();

    // Perform AND operation
    let result_len: i64 = client
        .call(BitOpCmd::new(
            BitOp::And,
            "bitop_result",
            vec!["bitop1", "bitop2"],
        ))
        .await
        .unwrap();
    assert_eq!(result_len, 3); // Result is 3 bytes long

    // Verify result exists
    let exists: i64 = client.call(Exists::new("bitop_result")).await.unwrap();
    assert_eq!(exists, 1);

    // Clean up
    client
        .call(Del::new(vec![
            "bitop1".to_string(),
            "bitop2".to_string(),
            "bitop_result".to_string(),
        ]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_bitop_or() {
    let client = setup_redis().await;

    // Set up source keys
    client.call(Set::new("or1", "abc")).await.unwrap();
    client.call(Set::new("or2", "def")).await.unwrap();

    // Perform OR operation
    let result_len: i64 = client
        .call(BitOpCmd::new(BitOp::Or, "or_result", vec!["or1", "or2"]))
        .await
        .unwrap();
    assert_eq!(result_len, 3);

    // Clean up
    client
        .call(Del::new(vec![
            "or1".to_string(),
            "or2".to_string(),
            "or_result".to_string(),
        ]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_bitop_xor() {
    let client = setup_redis().await;

    // Set up source keys with same value
    client.call(Set::new("xor1", "test")).await.unwrap();
    client.call(Set::new("xor2", "test")).await.unwrap();

    // XOR of same values should be all zeros
    let result_len: i64 = client
        .call(BitOpCmd::new(
            BitOp::Xor,
            "xor_result",
            vec!["xor1", "xor2"],
        ))
        .await
        .unwrap();
    assert_eq!(result_len, 4);

    // Result should be all zero bytes
    let result: Option<Bytes> = client.call(Get::new("xor_result")).await.unwrap();
    assert_eq!(result.as_ref().map(|b| b.as_ref()), Some(&[0, 0, 0, 0][..]));

    // Clean up
    client
        .call(Del::new(vec![
            "xor1".to_string(),
            "xor2".to_string(),
            "xor_result".to_string(),
        ]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_bitop_not() {
    let client = setup_redis().await;

    // Set a known value
    // 0xFF = 0b11111111
    client
        .call(Set::new("not_source", Bytes::from(vec![0xFF])))
        .await
        .unwrap();

    // NOT operation
    let result_len: i64 = client
        .call(BitOpCmd::new(BitOp::Not, "not_result", vec!["not_source"]))
        .await
        .unwrap();
    assert_eq!(result_len, 1);

    // Result should be 0x00
    let result: Option<Bytes> = client.call(Get::new("not_result")).await.unwrap();
    assert_eq!(result.as_ref().map(|b| b.as_ref()), Some(&[0][..]));

    // Clean up
    client
        .call(Del::new(vec![
            "not_source".to_string(),
            "not_result".to_string(),
        ]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_bitpos() {
    let client = setup_redis().await;

    // Set a value: 0xFF00 = 0b11111111 00000000
    client
        .call(Set::new("bitpos_key", Bytes::from(vec![0xFF, 0x00])))
        .await
        .unwrap();

    // Find first 0 bit
    let pos: i64 = client.call(BitPos::new("bitpos_key", false)).await.unwrap();
    assert_eq!(pos, 8); // First 0 bit is at position 8

    // Find first 1 bit
    let pos: i64 = client.call(BitPos::new("bitpos_key", true)).await.unwrap();
    assert_eq!(pos, 0); // First 1 bit is at position 0

    // Clean up
    client
        .call(Del::new(vec!["bitpos_key".to_string()]))
        .await
        .unwrap();
}
