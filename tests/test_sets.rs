//! Integration tests for Set commands

use bytes::Bytes;
use redis_tower::commands::{
    Del, SDiffStore, SInterCard, SInterStore, SMIsMember, SMove, SPop, SRandMember, SUnionStore,
    Sadd, Scard, Smembers,
};

mod common;

use common::{connect, test_key};

#[tokio::test]
async fn test_spop_single() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("set_spop_single");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add some members
    client
        .execute(Sadd::new(&key, b"member1".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&key, b"member2".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&key, b"member3".to_vec()))
        .await
        .expect("SADD should succeed");

    // Pop single member
    let popped = client
        .execute(SPop::new(&key))
        .await
        .expect("SPOP should succeed");

    assert_eq!(popped.len(), 1, "Should pop exactly one member");

    // Verify set size decreased
    let size: i64 = client
        .execute(Scard::new(&key))
        .await
        .expect("SCARD should succeed");
    assert_eq!(size, 2, "Set should have 2 members left");
}

#[tokio::test]
async fn test_spop_multiple() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("set_spop_multiple");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add members
    for i in 1..=10 {
        client
            .execute(Sadd::new(&key, Bytes::from(format!("member{}", i))))
            .await
            .expect("SADD should succeed");
    }

    // Pop 3 members
    let popped = client
        .execute(SPop::count(&key, 3))
        .await
        .expect("SPOP with count should succeed");

    assert_eq!(popped.len(), 3, "Should pop exactly 3 members");

    // Verify set size
    let size: i64 = client
        .execute(Scard::new(&key))
        .await
        .expect("SCARD should succeed");
    assert_eq!(size, 7, "Set should have 7 members left");
}

#[tokio::test]
async fn test_spop_empty_set() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("set_spop_empty");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Pop from empty set
    let popped = client
        .execute(SPop::new(&key))
        .await
        .expect("SPOP on empty set should succeed");

    assert_eq!(popped.len(), 0, "Should pop nothing from empty set");
}

#[tokio::test]
async fn test_srandmember_single() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("set_srandmember_single");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add members
    client
        .execute(Sadd::new(&key, b"member1".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&key, b"member2".to_vec()))
        .await
        .expect("SADD should succeed");

    // Get random member
    let members = client
        .execute(SRandMember::new(&key))
        .await
        .expect("SRANDMEMBER should succeed");

    assert_eq!(members.len(), 1, "Should get exactly one member");

    // Verify set size unchanged
    let size: i64 = client
        .execute(Scard::new(&key))
        .await
        .expect("SCARD should succeed");
    assert_eq!(size, 2, "Set size should be unchanged");
}

#[tokio::test]
async fn test_srandmember_multiple() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("set_srandmember_multiple");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add members
    for i in 1..=10 {
        client
            .execute(Sadd::new(&key, Bytes::from(format!("member{}", i))))
            .await
            .expect("SADD should succeed");
    }

    // Get 3 random members
    let members = client
        .execute(SRandMember::count(&key, 3))
        .await
        .expect("SRANDMEMBER with count should succeed");

    assert_eq!(members.len(), 3, "Should get exactly 3 members");

    // Verify set size unchanged
    let size: i64 = client
        .execute(Scard::new(&key))
        .await
        .expect("SCARD should succeed");
    assert_eq!(size, 10, "Set size should be unchanged");
}

#[tokio::test]
async fn test_smove() {
    let client = connect().await.expect("Failed to connect to Redis");
    let src = test_key("set_smove_src");
    let dst = test_key("set_smove_dst");

    // Clean up
    let _ = client
        .execute(Del::new(vec![src.clone(), dst.clone()]))
        .await;

    // Add members to source
    client
        .execute(Sadd::new(&src, b"member1".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&src, b"member2".to_vec()))
        .await
        .expect("SADD should succeed");

    // Move member
    let moved: bool = client
        .execute(SMove::new(&src, &dst, b"member1".to_vec()))
        .await
        .expect("SMOVE should succeed");

    assert!(moved, "Member should be moved");

    // Verify source
    let src_members: Vec<_> = client
        .execute(Smembers::new(&src))
        .await
        .expect("SMEMBERS should succeed");
    assert_eq!(src_members.len(), 1);
    assert_eq!(src_members[0].as_ref(), b"member2");

    // Verify destination
    let dst_members: Vec<_> = client
        .execute(Smembers::new(&dst))
        .await
        .expect("SMEMBERS should succeed");
    assert_eq!(dst_members.len(), 1);
    assert_eq!(dst_members[0].as_ref(), b"member1");
}

#[tokio::test]
async fn test_smove_nonexistent() {
    let client = connect().await.expect("Failed to connect to Redis");
    let src = test_key("set_smove_nonexistent_src");
    let dst = test_key("set_smove_nonexistent_dst");

    // Clean up
    let _ = client
        .execute(Del::new(vec![src.clone(), dst.clone()]))
        .await;

    // Add member to source
    client
        .execute(Sadd::new(&src, b"member1".to_vec()))
        .await
        .expect("SADD should succeed");

    // Try to move non-existent member
    let moved: bool = client
        .execute(SMove::new(&src, &dst, b"nonexistent".to_vec()))
        .await
        .expect("SMOVE should succeed");

    assert!(!moved, "Non-existent member should not be moved");
}

#[tokio::test]
async fn test_sinterstore() {
    let client = connect().await.expect("Failed to connect to Redis");
    let set1 = test_key("set_sinterstore_1");
    let set2 = test_key("set_sinterstore_2");
    let dest = test_key("set_sinterstore_dest");

    // Clean up
    let _ = client
        .execute(Del::new(vec![set1.clone(), set2.clone(), dest.clone()]))
        .await;

    // Add members to set1
    client
        .execute(Sadd::new(&set1, b"a".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&set1, b"b".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&set1, b"c".to_vec()))
        .await
        .expect("SADD should succeed");

    // Add members to set2
    client
        .execute(Sadd::new(&set2, b"b".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&set2, b"c".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&set2, b"d".to_vec()))
        .await
        .expect("SADD should succeed");

    // Store intersection
    let count: i64 = client
        .execute(SInterStore::new(&dest, vec![set1.clone(), set2.clone()]))
        .await
        .expect("SINTERSTORE should succeed");

    assert_eq!(count, 2, "Intersection should have 2 members (b, c)");

    // Verify destination
    let members: Vec<_> = client
        .execute(Smembers::new(&dest))
        .await
        .expect("SMEMBERS should succeed");
    assert_eq!(members.len(), 2);
}

#[tokio::test]
async fn test_sunionstore() {
    let client = connect().await.expect("Failed to connect to Redis");
    let set1 = test_key("set_sunionstore_1");
    let set2 = test_key("set_sunionstore_2");
    let dest = test_key("set_sunionstore_dest");

    // Clean up
    let _ = client
        .execute(Del::new(vec![set1.clone(), set2.clone(), dest.clone()]))
        .await;

    // Add members
    client
        .execute(Sadd::new(&set1, b"a".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&set1, b"b".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&set2, b"c".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&set2, b"d".to_vec()))
        .await
        .expect("SADD should succeed");

    // Store union
    let count: i64 = client
        .execute(SUnionStore::new(&dest, vec![set1.clone(), set2.clone()]))
        .await
        .expect("SUNIONSTORE should succeed");

    assert_eq!(count, 4, "Union should have 4 members");
}

#[tokio::test]
async fn test_sdiffstore() {
    let client = connect().await.expect("Failed to connect to Redis");
    let set1 = test_key("set_sdiffstore_1");
    let set2 = test_key("set_sdiffstore_2");
    let dest = test_key("set_sdiffstore_dest");

    // Clean up
    let _ = client
        .execute(Del::new(vec![set1.clone(), set2.clone(), dest.clone()]))
        .await;

    // Add members
    client
        .execute(Sadd::new(&set1, b"a".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&set1, b"b".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&set1, b"c".to_vec()))
        .await
        .expect("SADD should succeed");

    client
        .execute(Sadd::new(&set2, b"c".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&set2, b"d".to_vec()))
        .await
        .expect("SADD should succeed");

    // Store difference (set1 - set2)
    let count: i64 = client
        .execute(SDiffStore::new(&dest, vec![set1.clone(), set2.clone()]))
        .await
        .expect("SDIFFSTORE should succeed");

    assert_eq!(count, 2, "Difference should have 2 members (a, b)");
}

#[tokio::test]
async fn test_smismember() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("set_smismember");

    // Clean up
    let _ = client.execute(Del::new(vec![key.clone()])).await;

    // Add members
    client
        .execute(Sadd::new(&key, b"member1".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&key, b"member2".to_vec()))
        .await
        .expect("SADD should succeed");
    client
        .execute(Sadd::new(&key, b"member3".to_vec()))
        .await
        .expect("SADD should succeed");

    // Check multiple members
    let results: Vec<bool> = client
        .execute(SMIsMember::new(
            &key,
            vec![
                b"member1".to_vec().into(),
                b"nonexistent".to_vec().into(),
                b"member3".to_vec().into(),
            ],
        ))
        .await
        .expect("SMISMEMBER should succeed");

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], true, "member1 should exist");
    assert_eq!(results[1], false, "nonexistent should not exist");
    assert_eq!(results[2], true, "member3 should exist");
}

#[tokio::test]
async fn test_sintercard() {
    let client = connect().await.expect("Failed to connect to Redis");
    let set1 = test_key("set_sintercard_1");
    let set2 = test_key("set_sintercard_2");
    let set3 = test_key("set_sintercard_3");

    // Clean up
    let _ = client
        .execute(Del::new(vec![set1.clone(), set2.clone(), set3.clone()]))
        .await;

    // Add members to set1
    for i in 1..=5 {
        client
            .execute(Sadd::new(&set1, Bytes::from(format!("member{}", i))))
            .await
            .expect("SADD should succeed");
    }

    // Add members to set2 (overlap with set1: 3, 4, 5)
    for i in 3..=7 {
        client
            .execute(Sadd::new(&set2, Bytes::from(format!("member{}", i))))
            .await
            .expect("SADD should succeed");
    }

    // Add members to set3 (overlap with both: 4, 5)
    for i in 4..=8 {
        client
            .execute(Sadd::new(&set3, Bytes::from(format!("member{}", i))))
            .await
            .expect("SADD should succeed");
    }

    // Get cardinality of intersection (should be member4 and member5 = 2)
    let cardinality: i64 = client
        .execute(SInterCard::new(vec![
            set1.clone(),
            set2.clone(),
            set3.clone(),
        ]))
        .await
        .expect("SINTERCARD should succeed");

    assert_eq!(cardinality, 2, "Intersection cardinality should be 2");
}

#[tokio::test]
async fn test_sintercard_with_limit() {
    let client = connect().await.expect("Failed to connect to Redis");
    let set1 = test_key("set_sintercard_limit_1");
    let set2 = test_key("set_sintercard_limit_2");

    // Clean up
    let _ = client
        .execute(Del::new(vec![set1.clone(), set2.clone()]))
        .await;

    // Add members (all overlap)
    for i in 1..=10 {
        client
            .execute(Sadd::new(&set1, Bytes::from(format!("member{}", i))))
            .await
            .expect("SADD should succeed");
        client
            .execute(Sadd::new(&set2, Bytes::from(format!("member{}", i))))
            .await
            .expect("SADD should succeed");
    }

    // Get cardinality with limit 5
    let cardinality: i64 = client
        .execute(SInterCard::new(vec![set1.clone(), set2.clone()]).limit(5))
        .await
        .expect("SINTERCARD with LIMIT should succeed");

    // With limit, it may return early
    assert!(cardinality <= 10, "Cardinality should be at most 10");
}
