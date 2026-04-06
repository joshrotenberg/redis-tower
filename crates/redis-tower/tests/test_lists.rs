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
