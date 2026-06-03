mod common;

use bytes::Bytes;
use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn zinterstore() {
    let mut c = conn().await;
    let k1 = "cover2:zset:zinterstore:s1";
    let k2 = "cover2:zset:zinterstore:s2";
    let dst = "cover2:zset:zinterstore:dst";

    c.execute(Del::keys([k1, k2, dst])).await.unwrap();
    c.execute(
        ZAdd::new(k1)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();
    c.execute(
        ZAdd::new(k2)
            .member(10.0, "b")
            .member(20.0, "c")
            .member(30.0, "d"),
    )
    .await
    .unwrap();

    let count = c.execute(ZInterStore::new(dst, [k1, k2])).await.unwrap();
    // "b" and "c" are in both sets
    assert_eq!(count, 2);

    let members = c.execute(ZRange::new(dst, 0, -1)).await.unwrap();
    assert_eq!(members.len(), 2);
    assert!(members.contains(&Bytes::from("b")));
    assert!(members.contains(&Bytes::from("c")));

    c.execute(Del::keys([k1, k2, dst])).await.unwrap();
}

#[tokio::test]
async fn zunionstore() {
    let mut c = conn().await;
    let k1 = "cover2:zset:zunionstore:s1";
    let k2 = "cover2:zset:zunionstore:s2";
    let dst = "cover2:zset:zunionstore:dst";

    c.execute(Del::keys([k1, k2, dst])).await.unwrap();
    c.execute(ZAdd::new(k1).member(1.0, "a").member(2.0, "b"))
        .await
        .unwrap();
    c.execute(ZAdd::new(k2).member(10.0, "b").member(20.0, "c"))
        .await
        .unwrap();

    let count = c.execute(ZUnionStore::new(dst, [k1, k2])).await.unwrap();
    // "a", "b", "c" -- union of the two sets
    assert_eq!(count, 3);

    let members = c.execute(ZRange::new(dst, 0, -1)).await.unwrap();
    assert_eq!(members.len(), 3);
    assert!(members.contains(&Bytes::from("a")));
    assert!(members.contains(&Bytes::from("b")));
    assert!(members.contains(&Bytes::from("c")));

    c.execute(Del::keys([k1, k2, dst])).await.unwrap();
}

#[tokio::test]
async fn zdiffstore() {
    let mut c = conn().await;
    let k1 = "cover2:zset:zdiffstore:s1";
    let k2 = "cover2:zset:zdiffstore:s2";
    let dst = "cover2:zset:zdiffstore:dst";

    c.execute(Del::keys([k1, k2, dst])).await.unwrap();
    c.execute(
        ZAdd::new(k1)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();
    c.execute(ZAdd::new(k2).member(10.0, "b").member(20.0, "c"))
        .await
        .unwrap();

    let count = c.execute(ZDiffStore::new(dst, [k1, k2])).await.unwrap();
    // "a" is in k1 but not k2
    assert_eq!(count, 1);

    let members = c.execute(ZRange::new(dst, 0, -1)).await.unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0], Bytes::from("a"));

    c.execute(Del::keys([k1, k2, dst])).await.unwrap();
}

#[tokio::test]
async fn zintercard() {
    let mut c = conn().await;
    let k1 = "cover2:zset:zintercard:s1";
    let k2 = "cover2:zset:zintercard:s2";

    c.execute(Del::keys([k1, k2])).await.unwrap();
    c.execute(
        ZAdd::new(k1)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();
    c.execute(
        ZAdd::new(k2)
            .member(10.0, "b")
            .member(20.0, "c")
            .member(30.0, "d"),
    )
    .await
    .unwrap();

    // "b" and "c" are in both -- cardinality is 2
    let card = c.execute(ZInterCard::new([k1, k2])).await.unwrap();
    assert_eq!(card, 2);

    // with LIMIT 1, result is capped at 1
    let card_limited = c.execute(ZInterCard::new([k1, k2]).limit(1)).await.unwrap();
    assert_eq!(card_limited, 1);

    c.execute(Del::keys([k1, k2])).await.unwrap();
}

#[tokio::test]
async fn zrangestore() {
    let mut c = conn().await;
    let src = "cover2:zset:zrangestore:src";
    let dst = "cover2:zset:zrangestore:dst";

    c.execute(Del::keys([src, dst])).await.unwrap();
    c.execute(
        ZAdd::new(src)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c")
            .member(4.0, "d"),
    )
    .await
    .unwrap();

    // Copy rank range 1..2 (members "b" and "c") to dst.
    let count = c
        .execute(ZRangeStore::new(dst, src, "1", "2"))
        .await
        .unwrap();
    assert_eq!(count, 2);

    let members = c.execute(ZRange::new(dst, 0, -1)).await.unwrap();
    assert_eq!(members.len(), 2);
    assert_eq!(members[0], Bytes::from("b"));
    assert_eq!(members[1], Bytes::from("c"));

    c.execute(Del::keys([src, dst])).await.unwrap();
}

#[tokio::test]
async fn zmpop_min() {
    let mut c = conn().await;
    let k = "cover2:zset:zmpop:min";

    c.execute(Del::new(k)).await.unwrap();
    c.execute(
        ZAdd::new(k)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let result = c
        .execute(ZMPop::new([k], ZMPopDirection::Min))
        .await
        .unwrap();
    let (popped_key, members) = result.unwrap();
    assert_eq!(popped_key, Bytes::from(k));
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].0, Bytes::from("a"));
    assert!((members[0].1 - 1.0).abs() < f64::EPSILON);

    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn zmpop_max() {
    let mut c = conn().await;
    let k = "cover2:zset:zmpop:max";

    c.execute(Del::new(k)).await.unwrap();
    c.execute(
        ZAdd::new(k)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let result = c
        .execute(ZMPop::new([k], ZMPopDirection::Max))
        .await
        .unwrap();
    let (popped_key, members) = result.unwrap();
    assert_eq!(popped_key, Bytes::from(k));
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].0, Bytes::from("c"));
    assert!((members[0].1 - 3.0).abs() < f64::EPSILON);

    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn zmpop_empty() {
    let mut c = conn().await;
    let k = "cover2:zset:zmpop:empty";

    c.execute(Del::new(k)).await.unwrap();

    // ZMPOP on an empty/nonexistent key returns None.
    let result = c
        .execute(ZMPop::new([k], ZMPopDirection::Min))
        .await
        .unwrap();
    assert!(result.is_none());
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
            .member(3.0, "c")
            .member(4.0, "d"),
    )
    .await
    .unwrap();

    // Remove ranks 0 and 1 (members "a" and "b").
    let removed = c.execute(ZRemRangeByRank::new(key, 0, 1)).await.unwrap();
    assert_eq!(removed, 2);

    let remaining = c.execute(ZRange::new(key, 0, -1)).await.unwrap();
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0], Bytes::from("c"));
    assert_eq!(remaining[1], Bytes::from("d"));

    c.execute(Del::new(key)).await.unwrap();
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
            .member(3.0, "c")
            .member(4.0, "d"),
    )
    .await
    .unwrap();

    // Remove members with score between 2 and 3 inclusive.
    let removed = c
        .execute(ZRemRangeByScore::new(key, "2", "3"))
        .await
        .unwrap();
    assert_eq!(removed, 2);

    let remaining = c.execute(ZRange::new(key, 0, -1)).await.unwrap();
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0], Bytes::from("a"));
    assert_eq!(remaining[1], Bytes::from("d"));

    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn zremrangebylex() {
    let mut c = conn().await;
    let key = "cover2:zset:zremrangebylex";

    c.execute(Del::new(key)).await.unwrap();
    // All members must have the same score for lex range to be meaningful.
    c.execute(
        ZAdd::new(key)
            .member(0.0, "a")
            .member(0.0, "b")
            .member(0.0, "c")
            .member(0.0, "d"),
    )
    .await
    .unwrap();

    // Remove members in lex range [b, c] inclusive.
    let removed = c
        .execute(ZRemRangeByLex::new(key, "[b", "[c"))
        .await
        .unwrap();
    assert_eq!(removed, 2);

    let remaining = c.execute(ZRange::new(key, 0, -1)).await.unwrap();
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0], Bytes::from("a"));
    assert_eq!(remaining[1], Bytes::from("d"));

    c.execute(Del::new(key)).await.unwrap();
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

    // In reverse order: "c" (score 3) has revrank 0, "a" (score 1) has revrank 2.
    let rank_c = c.execute(ZRevRank::new(key, "c")).await.unwrap();
    assert_eq!(rank_c, Some(0));

    let rank_a = c.execute(ZRevRank::new(key, "a")).await.unwrap();
    assert_eq!(rank_a, Some(2));

    let rank_missing = c.execute(ZRevRank::new(key, "nope")).await.unwrap();
    assert_eq!(rank_missing, None);

    c.execute(Del::new(key)).await.unwrap();
}

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
async fn zmpop_min() {
    let mut c = conn().await;
    let key = "cover2:zset:zmpop_min";

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
    let (popped_key, members) = result.expect("expected Some result");
    assert_eq!(popped_key, Bytes::from(key));
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].0, Bytes::from("a"));
    assert!((members[0].1 - 1.0).abs() < f64::EPSILON);

    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn zmpop_max() {
    let mut c = conn().await;
    let key = "cover2:zset:zmpop_max";

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
        .execute(ZMPop::new([key], ZMPopDirection::Max))
        .await
        .unwrap();
    let (popped_key, members) = result.expect("expected Some result");
    assert_eq!(popped_key, Bytes::from(key));
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].0, Bytes::from("c"));
    assert!((members[0].1 - 3.0).abs() < f64::EPSILON);

    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn zmpop_with_count() {
    let mut c = conn().await;
    let key = "cover2:zset:zmpop_count";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        ZAdd::new(key)
            .member(10.0, "x")
            .member(20.0, "y")
            .member(30.0, "z")
            .member(40.0, "w"),
    )
    .await
    .unwrap();

    let result = c
        .execute(ZMPop::new([key], ZMPopDirection::Min).count(2))
        .await
        .unwrap();
    let (popped_key, members) = result.expect("expected Some result");
    assert_eq!(popped_key, Bytes::from(key));
    assert_eq!(members.len(), 2);
    assert_eq!(members[0].0, Bytes::from("x"));
    assert_eq!(members[1].0, Bytes::from("y"));

    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn zmpop_multiple_keys_first_nonempty() {
    let mut c = conn().await;
    let k1 = "cover2:zset:zmpop_multi:k1";
    let k2 = "cover2:zset:zmpop_multi:k2";

    c.execute(Del::new(k1)).await.unwrap();
    c.execute(Del::new(k2)).await.unwrap();

    // k1 is empty; k2 has members -- ZMPOP should pop from k2.
    c.execute(ZAdd::new(k2).member(5.0, "alpha").member(10.0, "beta"))
        .await
        .unwrap();

    let result = c
        .execute(ZMPop::new([k1, k2], ZMPopDirection::Min))
        .await
        .unwrap();
    let (popped_key, members) = result.expect("expected Some result");
    assert_eq!(popped_key, Bytes::from(k2));
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].0, Bytes::from("alpha"));

    c.execute(Del::new(k1)).await.unwrap();
    c.execute(Del::new(k2)).await.unwrap();
}

#[tokio::test]
async fn zmpop_empty_sources() {
    let mut c = conn().await;
    let k1 = "cover2:zset:zmpop_empty:k1";
    let k2 = "cover2:zset:zmpop_empty:k2";

    c.execute(Del::new(k1)).await.unwrap();
    c.execute(Del::new(k2)).await.unwrap();

    let result = c
        .execute(ZMPop::new([k1, k2], ZMPopDirection::Min))
        .await
        .unwrap();
    assert!(result.is_none());
}
