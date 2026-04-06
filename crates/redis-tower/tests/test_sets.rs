mod common;

use bytes::Bytes;
use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn cover_srandmember() {
    let mut c = conn().await;
    let k = "cover:sets:srandmember";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(SAdd::members(k, ["a", "b", "c"])).await.unwrap();
    let members = c.execute(SRandMember::new(k)).await.unwrap();
    assert!(!members.is_empty());
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_spop() {
    let mut c = conn().await;
    let k = "cover:sets:spop";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(SAdd::members(k, ["a", "b", "c"])).await.unwrap();
    let popped = c.execute(SPop::new(k)).await.unwrap();
    assert!(!popped.is_empty());
    let card = c.execute(SCard::new(k)).await.unwrap();
    assert_eq!(card, 2);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_sdiff() {
    let mut c = conn().await;
    let s1 = "cover:sets:sdiff:s1";
    let s2 = "cover:sets:sdiff:s2";
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
    c.execute(SAdd::members(s1, ["a", "b", "c"])).await.unwrap();
    c.execute(SAdd::members(s2, ["b", "c", "d"])).await.unwrap();
    let diff = c.execute(SDiff::keys([s1, s2])).await.unwrap();
    assert_eq!(diff, vec![Bytes::from("a")]);
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
}

#[tokio::test]
async fn cover_sdiffstore() {
    let mut c = conn().await;
    let s1 = "cover:sets:sdiffstore:s1";
    let s2 = "cover:sets:sdiffstore:s2";
    let dst = "cover:sets:sdiffstore:dst";
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
    c.execute(SAdd::members(s1, ["a", "b", "c"])).await.unwrap();
    c.execute(SAdd::members(s2, ["b", "c", "d"])).await.unwrap();
    let n = c.execute(SDiffStore::new(dst, [s1, s2])).await.unwrap();
    assert_eq!(n, 1);
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
}

#[tokio::test]
async fn cover_sunion() {
    let mut c = conn().await;
    let s1 = "cover:sets:sunion:s1";
    let s2 = "cover:sets:sunion:s2";
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
    c.execute(SAdd::members(s1, ["a", "b"])).await.unwrap();
    c.execute(SAdd::members(s2, ["b", "c"])).await.unwrap();
    let union = c.execute(SUnion::keys([s1, s2])).await.unwrap();
    assert_eq!(union.len(), 3);
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
}

#[tokio::test]
async fn cover_sunionstore() {
    let mut c = conn().await;
    let s1 = "cover:sets:sunionstore:s1";
    let s2 = "cover:sets:sunionstore:s2";
    let dst = "cover:sets:sunionstore:dst";
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
    c.execute(SAdd::members(s1, ["a", "b"])).await.unwrap();
    c.execute(SAdd::members(s2, ["b", "c"])).await.unwrap();
    let n = c.execute(SUnionStore::new(dst, [s1, s2])).await.unwrap();
    assert_eq!(n, 3);
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
}

#[tokio::test]
async fn cover_smove() {
    let mut c = conn().await;
    let src = "cover:sets:smove:src";
    let dst = "cover:sets:smove:dst";
    c.execute(Del::new(src)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
    c.execute(SAdd::members(src, ["a", "b"])).await.unwrap();
    c.execute(SAdd::new(dst, "c")).await.unwrap();
    let ok = c.execute(SMove::new(src, dst, "a")).await.unwrap();
    assert!(ok);
    c.execute(Del::new(src)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
}

#[tokio::test]
async fn cover_smismember() {
    let mut c = conn().await;
    let k = "cover:sets:smismember";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(SAdd::members(k, ["a", "b", "c"])).await.unwrap();
    let results = c
        .execute(SMisMember::members(k, ["a", "x", "b"]))
        .await
        .unwrap();
    assert_eq!(results, vec![true, false, true]);
    c.execute(Del::new(k)).await.unwrap();
}
