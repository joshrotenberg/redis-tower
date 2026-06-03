mod common;

use bytes::Bytes;
use common::conn;
use redis_tower::Frame;
use redis_tower::commands::*;

#[tokio::test]
async fn cover_getex() {
    let mut c = conn().await;
    let k = "cover:strings:getex";
    c.execute(Set::new(k, "val")).await.unwrap();
    let v = c.execute(GetEx::new(k).ex(10)).await.unwrap();
    assert_eq!(v, Some(Bytes::from("val")));
    let ttl = c.execute(Ttl::new(k)).await.unwrap();
    assert!(ttl > 0);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_getdel() {
    let mut c = conn().await;
    let k = "cover:strings:getdel";
    c.execute(Set::new(k, "val")).await.unwrap();
    let v = c.execute(GetDel::new(k)).await.unwrap();
    assert_eq!(v, Some(Bytes::from("val")));
    let gone = c.execute(Get::new(k)).await.unwrap();
    assert_eq!(gone, None);
}

#[tokio::test]
async fn cover_setex() {
    let mut c = conn().await;
    let k = "cover:strings:setex";
    c.execute(SetEx::new(k, 10, "val")).await.unwrap();
    let v = c.execute(Get::new(k)).await.unwrap();
    assert_eq!(v, Some(Bytes::from("val")));
    let ttl = c.execute(Ttl::new(k)).await.unwrap();
    assert!(ttl > 0);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_psetex() {
    let mut c = conn().await;
    let k = "cover:strings:psetex";
    c.execute(PSetEx::new(k, 10000, "val")).await.unwrap();
    let v = c.execute(Get::new(k)).await.unwrap();
    assert_eq!(v, Some(Bytes::from("val")));
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_setnx() {
    let mut c = conn().await;
    let k = "cover:strings:setnx";
    c.execute(Del::new(k)).await.unwrap();
    let ok = c.execute(SetNx::new(k, "val")).await.unwrap();
    assert!(ok);
    let fail = c.execute(SetNx::new(k, "val2")).await.unwrap();
    assert!(!fail);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_incrbyfloat() {
    let mut c = conn().await;
    let k = "cover:strings:incrbyfloat";
    c.execute(Set::new(k, "10.5")).await.unwrap();
    let v = c.execute(IncrByFloat::new(k, 0.5)).await.unwrap();
    assert!((v - 11.0).abs() < f64::EPSILON);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_decr() {
    let mut c = conn().await;
    let k = "cover:strings:decr";
    c.execute(Set::new(k, "10")).await.unwrap();
    let v = c.execute(Decr::new(k)).await.unwrap();
    assert_eq!(v, 9);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_decrby() {
    let mut c = conn().await;
    let k = "cover:strings:decrby";
    c.execute(Set::new(k, "10")).await.unwrap();
    let v = c.execute(DecrBy::new(k, 3)).await.unwrap();
    assert_eq!(v, 7);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_getrange() {
    let mut c = conn().await;
    let k = "cover:strings:getrange";
    c.execute(Set::new(k, "hello world")).await.unwrap();
    let v = c.execute(GetRange::new(k, 0, 4)).await.unwrap();
    assert_eq!(v, Bytes::from("hello"));
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_setrange() {
    let mut c = conn().await;
    let k = "cover:strings:setrange";
    c.execute(Set::new(k, "hello")).await.unwrap();
    let len = c.execute(SetRange::new(k, 6, "world")).await.unwrap();
    assert_eq!(len, 11); // "hello\0world"
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_strlen() {
    let mut c = conn().await;
    let k = "cover:strings:strlen";
    c.execute(Set::new(k, "hello")).await.unwrap();
    let len = c.execute(StrLen::new(k)).await.unwrap();
    assert_eq!(len, 5);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_lcs_basic() {
    let mut c = conn().await;
    let k1 = "cover:strings:lcs:k1";
    let k2 = "cover:strings:lcs:k2";
    c.execute(Del::new(k1)).await.unwrap();
    c.execute(Del::new(k2)).await.unwrap();
    c.execute(Set::new(k1, "ohmytext")).await.unwrap();
    c.execute(Set::new(k2, "mynewtext")).await.unwrap();
    let result = c.execute(Lcs::new(k1, k2)).await.unwrap();
    match result {
        Frame::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b).to_string();
            assert_eq!(s, "mytext");
        }
        other => panic!("expected bulk string from LCS, got: {other:?}"),
    }
    c.execute(Del::new(k1)).await.unwrap();
    c.execute(Del::new(k2)).await.unwrap();
}

#[tokio::test]
async fn cover_lcs_len() {
    let mut c = conn().await;
    let k1 = "cover:strings:lcs_len:k1";
    let k2 = "cover:strings:lcs_len:k2";
    c.execute(Del::new(k1)).await.unwrap();
    c.execute(Del::new(k2)).await.unwrap();
    c.execute(Set::new(k1, "ohmytext")).await.unwrap();
    c.execute(Set::new(k2, "mynewtext")).await.unwrap();
    let result = c.execute(Lcs::new(k1, k2).len()).await.unwrap();
    match result {
        Frame::Integer(n) => {
            assert_eq!(n, 6); // "mytext" has length 6
        }
        other => panic!("expected integer from LCS LEN, got: {other:?}"),
    }
    c.execute(Del::new(k1)).await.unwrap();
    c.execute(Del::new(k2)).await.unwrap();
}

#[tokio::test]
async fn cover_getset_basic() {
    let mut c = conn().await;
    let k = "cover:strings:getset";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(Set::new(k, "old_value")).await.unwrap();
    let old = c.execute(GetSet::new(k, "new_value")).await.unwrap();
    assert_eq!(old, Some(Bytes::from("old_value")));
    let current = c.execute(Get::new(k)).await.unwrap();
    assert_eq!(current, Some(Bytes::from("new_value")));
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_msetnx_all_new() {
    let mut c = conn().await;
    let k1 = "cover:strings:msetnx:k1";
    let k2 = "cover:strings:msetnx:k2";
    c.execute(Del::keys([k1, k2])).await.unwrap();
    let ok = c
        .execute(MSetNx::new([(k1, "v1"), (k2, "v2")]))
        .await
        .unwrap();
    assert!(ok, "MSETNX should return true when all keys are new");
    let v1 = c.execute(Get::new(k1)).await.unwrap();
    let v2 = c.execute(Get::new(k2)).await.unwrap();
    assert_eq!(v1, Some(Bytes::from("v1")));
    assert_eq!(v2, Some(Bytes::from("v2")));
    c.execute(Del::keys([k1, k2])).await.unwrap();
}

#[tokio::test]
async fn cover_msetnx_one_exists() {
    let mut c = conn().await;
    let k1 = "cover:strings:msetnx_exists:k1";
    let k2 = "cover:strings:msetnx_exists:k2";
    c.execute(Del::keys([k1, k2])).await.unwrap();
    // Pre-set k1 so MSETNX must refuse the whole operation.
    c.execute(Set::new(k1, "existing")).await.unwrap();
    let ok = c
        .execute(MSetNx::new([(k1, "new1"), (k2, "new2")]))
        .await
        .unwrap();
    assert!(
        !ok,
        "MSETNX should return false when any key already exists"
    );
    // k1 must still hold its original value.
    let v1 = c.execute(Get::new(k1)).await.unwrap();
    assert_eq!(v1, Some(Bytes::from("existing")));
    // k2 must not have been set.
    let v2 = c.execute(Get::new(k2)).await.unwrap();
    assert_eq!(v2, None);
    c.execute(Del::keys([k1, k2])).await.unwrap();
}
