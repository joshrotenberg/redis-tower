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

#[tokio::test]
async fn zinterstore() {
    let mut c = conn().await;
    let key1 = "cover2:zset:zinterstore:1";
    let key2 = "cover2:zset:zinterstore:2";
    let dst = "cover2:zset:zinterstore:dst";

    c.execute(Del::new(key1)).await.unwrap();
    c.execute(Del::new(key2)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
    c.execute(
        ZAdd::new(key1)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();
    c.execute(
        ZAdd::new(key2)
            .member(1.0, "b")
            .member(2.0, "c")
            .member(3.0, "d"),
    )
    .await
    .unwrap();

    let count = c
        .execute(ZInterStore::new(dst, [key1, key2]))
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn zunionstore() {
    let mut c = conn().await;
    let key1 = "cover2:zset:zunionstore:1";
    let key2 = "cover2:zset:zunionstore:2";
    let dst = "cover2:zset:zunionstore:dst";

    c.execute(Del::new(key1)).await.unwrap();
    c.execute(Del::new(key2)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
    c.execute(
        ZAdd::new(key1)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();
    c.execute(
        ZAdd::new(key2)
            .member(1.0, "b")
            .member(2.0, "c")
            .member(3.0, "d"),
    )
    .await
    .unwrap();

    let count = c
        .execute(ZUnionStore::new(dst, [key1, key2]))
        .await
        .unwrap();
    assert_eq!(count, 4);
}

#[tokio::test]
async fn zdiffstore() {
    let mut c = conn().await;
    let key1 = "cover2:zset:zdiffstore:1";
    let key2 = "cover2:zset:zdiffstore:2";
    let dst = "cover2:zset:zdiffstore:dst";

    c.execute(Del::new(key1)).await.unwrap();
    c.execute(Del::new(key2)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
    c.execute(
        ZAdd::new(key1)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();
    c.execute(ZAdd::new(key2).member(1.0, "b").member(2.0, "c"))
        .await
        .unwrap();

    let count = c.execute(ZDiffStore::new(dst, [key1, key2])).await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn zintercard() {
    let mut c = conn().await;
    let key1 = "cover2:zset:zintercard:1";
    let key2 = "cover2:zset:zintercard:2";

    c.execute(Del::new(key1)).await.unwrap();
    c.execute(Del::new(key2)).await.unwrap();
    c.execute(
        ZAdd::new(key1)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();
    c.execute(
        ZAdd::new(key2)
            .member(1.0, "b")
            .member(2.0, "c")
            .member(3.0, "d"),
    )
    .await
    .unwrap();

    let count = c.execute(ZInterCard::new([key1, key2])).await.unwrap();
    assert_eq!(count, 2);

    let limited = c
        .execute(ZInterCard::new([key1, key2]).limit(1))
        .await
        .unwrap();
    assert_eq!(limited, 1);
}

#[tokio::test]
async fn zrangestore() {
    let mut c = conn().await;
    let src = "cover2:zset:zrangestore:src";
    let dst = "cover2:zset:zrangestore:dst";

    c.execute(Del::new(src)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
    c.execute(
        ZAdd::new(src)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let count = c
        .execute(ZRangeStore::new(dst, src, "0", "-1"))
        .await
        .unwrap();
    assert_eq!(count, 3);

    let stored = c.execute(ZCard::new(dst)).await.unwrap();
    assert_eq!(stored, 3);
}

#[tokio::test]
async fn zmpop() {
    let mut c = conn().await;
    let key = "cover2:zset:zmpop";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        ZAdd::new(key)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let result = c
        .execute(ZMPop::new([key], ZMPopDirection::Min))
        .await
        .unwrap();
    let (popped_key, members) = result.expect("expected a popped member");
    assert_eq!(popped_key, Bytes::from(key));
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].0, Bytes::from("a"));
    assert!((members[0].1 - 1.0).abs() < f64::EPSILON);

    let empty = "cover2:zset:zmpop:empty";
    c.execute(Del::new(empty)).await.unwrap();
    let none = c
        .execute(ZMPop::new([empty], ZMPopDirection::Min))
        .await
        .unwrap();
    assert!(none.is_none());
}

#[tokio::test]
async fn zremrangebyrank() {
    let mut c = conn().await;
    let key = "cover2:zset:zremrangebyrank";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        ZAdd::new(key)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let removed = c.execute(ZRemRangeByRank::new(key, 0, 0)).await.unwrap();
    assert_eq!(removed, 1);
    assert_eq!(c.execute(ZCard::new(key)).await.unwrap(), 2);
}

#[tokio::test]
async fn zremrangebyscore() {
    let mut c = conn().await;
    let key = "cover2:zset:zremrangebyscore";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        ZAdd::new(key)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let removed = c
        .execute(ZRemRangeByScore::new(key, "1", "2"))
        .await
        .unwrap();
    assert_eq!(removed, 2);
    assert_eq!(c.execute(ZCard::new(key)).await.unwrap(), 1);
}

#[tokio::test]
async fn zremrangebylex() {
    let mut c = conn().await;
    let key = "cover2:zset:zremrangebylex";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        ZAdd::new(key)
            .member(0.0, "a")
            .member(0.0, "b")
            .member(0.0, "c"),
    )
    .await
    .unwrap();

    let removed = c
        .execute(ZRemRangeByLex::new(key, "[a", "[c"))
        .await
        .unwrap();
    assert_eq!(removed, 3);
    assert_eq!(c.execute(ZCard::new(key)).await.unwrap(), 0);
}

#[tokio::test]
async fn zrevrank() {
    let mut c = conn().await;
    let key = "cover2:zset:zrevrank";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        ZAdd::new(key)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let rank = c.execute(ZRevRank::new(key, "a")).await.unwrap();
    assert_eq!(rank, Some(2));

    let missing = c.execute(ZRevRank::new(key, "missing")).await.unwrap();
    assert!(missing.is_none());
}
