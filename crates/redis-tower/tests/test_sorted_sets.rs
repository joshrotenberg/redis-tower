mod common;

use bytes::Bytes;
use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn zpopmin() {
    let mut c = conn().await;
    let key = "cover2:zset:zpopmin";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        ZAdd::new(key)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let result = c.execute(ZPopMin::new(key)).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, Bytes::from("a"));
    assert!((result[0].1 - 1.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn zpopmax() {
    let mut c = conn().await;
    let key = "cover2:zset:zpopmax";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        ZAdd::new(key)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let result = c.execute(ZPopMax::new(key)).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, Bytes::from("c"));
    assert!((result[0].1 - 3.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn zcount() {
    let mut c = conn().await;
    let key = "cover2:zset:zcount";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        ZAdd::new(key)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let count = c.execute(ZCount::new(key, "1", "2")).await.unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn zlexcount() {
    let mut c = conn().await;
    let key = "cover2:zset:zlexcount";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        ZAdd::new(key)
            .member(0.0, "a")
            .member(0.0, "b")
            .member(0.0, "c"),
    )
    .await
    .unwrap();

    let count = c.execute(ZLexCount::new(key, "[a", "[c")).await.unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn zrandmember() {
    let mut c = conn().await;
    let key = "cover2:zset:zrandmember";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(ZAdd::new(key).member(1.0, "a").member(2.0, "b"))
        .await
        .unwrap();

    let result = c.execute(ZRandMember::new(key).count(2)).await.unwrap();
    assert!(!result.is_empty());
}

#[tokio::test]
async fn zmscore() {
    let mut c = conn().await;
    let key = "cover2:zset:zmscore";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(ZAdd::new(key).member(1.0, "a").member(2.0, "b"))
        .await
        .unwrap();

    let scores = c
        .execute(ZMScore::members(key, ["a", "b", "missing"]))
        .await
        .unwrap();
    assert_eq!(scores.len(), 3);
    assert!((scores[0].unwrap() - 1.0).abs() < f64::EPSILON);
    assert!((scores[1].unwrap() - 2.0).abs() < f64::EPSILON);
    assert!(scores[2].is_none());
}
