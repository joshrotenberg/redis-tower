mod common;

use bytes::Bytes;
use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn cover_lpushx() {
    let mut c = conn().await;
    let k = "cover:lists:lpushx";
    c.execute(Del::new(k)).await.unwrap();
    // On missing key, LPUSHX returns 0.
    let n = c.execute(LPushX::new(k, "a")).await.unwrap();
    assert_eq!(n, 0);
    // Create the list first.
    c.execute(LPush::new(k, "x")).await.unwrap();
    let n = c.execute(LPushX::new(k, "y")).await.unwrap();
    assert_eq!(n, 2);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_rpushx() {
    let mut c = conn().await;
    let k = "cover:lists:rpushx";
    c.execute(Del::new(k)).await.unwrap();
    let n = c.execute(RPushX::new(k, "a")).await.unwrap();
    assert_eq!(n, 0);
    c.execute(RPush::new(k, "x")).await.unwrap();
    let n = c.execute(RPushX::new(k, "y")).await.unwrap();
    assert_eq!(n, 2);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_linsert() {
    let mut c = conn().await;
    let k = "cover:lists:linsert";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(RPush::elements(k, ["a", "b", "c"]))
        .await
        .unwrap();
    let len = c
        .execute(LInsert::new(k, ListPosition::Before, "b", "x"))
        .await
        .unwrap();
    assert_eq!(len, 4);
    let items = c.execute(LRange::new(k, 0, -1)).await.unwrap();
    assert_eq!(
        items,
        vec![
            Bytes::from("a"),
            Bytes::from("x"),
            Bytes::from("b"),
            Bytes::from("c"),
        ]
    );
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_lrem() {
    let mut c = conn().await;
    let k = "cover:lists:lrem";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(RPush::elements(k, ["a", "b", "a", "c", "a"]))
        .await
        .unwrap();
    let removed = c.execute(LRem::new(k, 2, "a")).await.unwrap();
    assert_eq!(removed, 2);
    let items = c.execute(LRange::new(k, 0, -1)).await.unwrap();
    assert_eq!(
        items,
        vec![Bytes::from("b"), Bytes::from("c"), Bytes::from("a")]
    );
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_ltrim() {
    let mut c = conn().await;
    let k = "cover:lists:ltrim";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(RPush::elements(k, ["a", "b", "c", "d", "e"]))
        .await
        .unwrap();
    c.execute(LTrim::new(k, 1, 3)).await.unwrap();
    let items = c.execute(LRange::new(k, 0, -1)).await.unwrap();
    assert_eq!(
        items,
        vec![Bytes::from("b"), Bytes::from("c"), Bytes::from("d")]
    );
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_lpos() {
    let mut c = conn().await;
    let k = "cover:lists:lpos";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(RPush::elements(k, ["a", "b", "c", "b", "d"]))
        .await
        .unwrap();
    let pos = c.execute(LPos::new(k, "b")).await.unwrap();
    assert_eq!(pos, Some(1));
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn lmpop_left() {
    let mut c = conn().await;
    let k1 = "cover:lists:lmpop_left:k1";
    let k2 = "cover:lists:lmpop_left:k2";
    c.execute(Del::new(k1)).await.unwrap();
    c.execute(Del::new(k2)).await.unwrap();

    // k1 is empty; k2 has elements -- LMPOP should pop from k2.
    c.execute(RPush::elements(k2, ["x", "y", "z"]))
        .await
        .unwrap();

    let result = c
        .execute(LMPop::new([k1, k2], ListDirection::Left))
        .await
        .unwrap();
    let (key, elements) = result.expect("expected Some result");
    assert_eq!(key, Bytes::from(k2));
    assert_eq!(elements.len(), 1);
    assert_eq!(elements[0], Bytes::from("x"));

    c.execute(Del::new(k1)).await.unwrap();
    c.execute(Del::new(k2)).await.unwrap();
}

#[tokio::test]
async fn lmpop_right() {
    let mut c = conn().await;
    let k = "cover:lists:lmpop_right";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(RPush::elements(k, ["a", "b", "c"]))
        .await
        .unwrap();

    let result = c
        .execute(LMPop::new([k], ListDirection::Right))
        .await
        .unwrap();
    let (key, elements) = result.expect("expected Some result");
    assert_eq!(key, Bytes::from(k));
    assert_eq!(elements.len(), 1);
    assert_eq!(elements[0], Bytes::from("c"));

    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn lmpop_with_count() {
    let mut c = conn().await;
    let k = "cover:lists:lmpop_count";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(RPush::elements(k, ["a", "b", "c", "d"]))
        .await
        .unwrap();

    let result = c
        .execute(LMPop::new([k], ListDirection::Left).count(3))
        .await
        .unwrap();
    let (key, elements) = result.expect("expected Some result");
    assert_eq!(key, Bytes::from(k));
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[0], Bytes::from("a"));
    assert_eq!(elements[1], Bytes::from("b"));
    assert_eq!(elements[2], Bytes::from("c"));

    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn lmpop_empty_sources() {
    let mut c = conn().await;
    let k1 = "cover:lists:lmpop_empty:k1";
    let k2 = "cover:lists:lmpop_empty:k2";
    c.execute(Del::new(k1)).await.unwrap();
    c.execute(Del::new(k2)).await.unwrap();

    let result = c
        .execute(LMPop::new([k1, k2], ListDirection::Left))
        .await
        .unwrap();
    assert!(result.is_none());
}
