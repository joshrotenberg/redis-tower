mod common;

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn cover_unlink() {
    let mut c = conn().await;
    let k = "cover:keys:unlink";
    c.execute(Set::new(k, "val")).await.unwrap();
    let removed = c.execute(Unlink::new(k)).await.unwrap();
    assert_eq!(removed, 1);
    let gone = c.execute(Get::new(k)).await.unwrap();
    assert_eq!(gone, None);
}

#[tokio::test]
async fn cover_persist() {
    let mut c = conn().await;
    let k = "cover:keys:persist";
    c.execute(Set::new(k, "val")).await.unwrap();
    c.execute(Expire::new(k, 60)).await.unwrap();
    let ok = c.execute(Persist::new(k)).await.unwrap();
    assert!(ok);
    let ttl = c.execute(Ttl::new(k)).await.unwrap();
    assert_eq!(ttl, -1);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_pexpire() {
    let mut c = conn().await;
    let k = "cover:keys:pexpire";
    c.execute(Set::new(k, "val")).await.unwrap();
    let ok = c.execute(PExpire::new(k, 60000)).await.unwrap();
    assert!(ok);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_pexpireat() {
    let mut c = conn().await;
    let k = "cover:keys:pexpireat";
    c.execute(Set::new(k, "val")).await.unwrap();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let ok = c.execute(PExpireAt::new(k, now_ms + 60000)).await.unwrap();
    assert!(ok);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_copy() {
    let mut c = conn().await;
    let src = "cover:keys:copy:src";
    let dst = "cover:keys:copy:dst";
    c.execute(Del::new(dst)).await.unwrap();
    c.execute(Set::new(src, "val")).await.unwrap();
    let ok = c.execute(Copy::new(src, dst)).await.unwrap();
    assert!(ok);
    let v = c.execute(Get::new(dst)).await.unwrap();
    assert_eq!(v, Some(Bytes::from("val")));
    c.execute(Del::new(src)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
}

#[tokio::test]
async fn cover_keys_pattern() {
    let mut c = conn().await;
    let ka = "cover:keys:pattern:a";
    let kb = "cover:keys:pattern:b";
    c.execute(Set::new(ka, "1")).await.unwrap();
    c.execute(Set::new(kb, "2")).await.unwrap();
    let keys = c.execute(Keys::new("cover:keys:pattern:*")).await.unwrap();
    assert_eq!(keys.len(), 2);
    c.execute(Del::new(ka)).await.unwrap();
    c.execute(Del::new(kb)).await.unwrap();
}

#[tokio::test]
async fn cover_randomkey() {
    let mut c = conn().await;
    let k = "cover:keys:randomkey";
    c.execute(Set::new(k, "val")).await.unwrap();
    let rk = c.execute(RandomKey::new()).await.unwrap();
    assert!(rk.is_some());
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_touch() {
    let mut c = conn().await;
    let k = "cover:keys:touch";
    c.execute(Set::new(k, "val")).await.unwrap();
    let n = c.execute(Touch::new(k)).await.unwrap();
    assert_eq!(n, 1);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_expiretime() {
    let mut c = conn().await;
    let k = "cover:keys:expiretime";
    c.execute(Set::new(k, "val")).await.unwrap();
    c.execute(Expire::new(k, 60)).await.unwrap();
    let ts = c.execute(ExpireTime::new(k)).await.unwrap();
    assert!(ts > 0);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_pexpiretime() {
    let mut c = conn().await;
    let k = "cover:keys:pexpiretime";
    c.execute(Set::new(k, "val")).await.unwrap();
    c.execute(PExpire::new(k, 60000)).await.unwrap();
    let ts = c.execute(PExpireTime::new(k)).await.unwrap();
    assert!(ts > 0);
    c.execute(Del::new(k)).await.unwrap();
}
