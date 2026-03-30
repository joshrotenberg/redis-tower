mod common;

use bytes::Bytes;
use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn cover_getex() {
    let c = conn().await;
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
    let c = conn().await;
    let k = "cover:strings:getdel";
    c.execute(Set::new(k, "val")).await.unwrap();
    let v = c.execute(GetDel::new(k)).await.unwrap();
    assert_eq!(v, Some(Bytes::from("val")));
    let gone = c.execute(Get::new(k)).await.unwrap();
    assert_eq!(gone, None);
}

#[tokio::test]
async fn cover_setex() {
    let c = conn().await;
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
    let c = conn().await;
    let k = "cover:strings:psetex";
    c.execute(PSetEx::new(k, 10000, "val")).await.unwrap();
    let v = c.execute(Get::new(k)).await.unwrap();
    assert_eq!(v, Some(Bytes::from("val")));
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_setnx() {
    let c = conn().await;
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
    let c = conn().await;
    let k = "cover:strings:incrbyfloat";
    c.execute(Set::new(k, "10.5")).await.unwrap();
    let v = c.execute(IncrByFloat::new(k, 0.5)).await.unwrap();
    assert!((v - 11.0).abs() < f64::EPSILON);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_decr() {
    let c = conn().await;
    let k = "cover:strings:decr";
    c.execute(Set::new(k, "10")).await.unwrap();
    let v = c.execute(Decr::new(k)).await.unwrap();
    assert_eq!(v, 9);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_decrby() {
    let c = conn().await;
    let k = "cover:strings:decrby";
    c.execute(Set::new(k, "10")).await.unwrap();
    let v = c.execute(DecrBy::new(k, 3)).await.unwrap();
    assert_eq!(v, 7);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_getrange() {
    let c = conn().await;
    let k = "cover:strings:getrange";
    c.execute(Set::new(k, "hello world")).await.unwrap();
    let v = c.execute(GetRange::new(k, 0, 4)).await.unwrap();
    assert_eq!(v, Bytes::from("hello"));
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_setrange() {
    let c = conn().await;
    let k = "cover:strings:setrange";
    c.execute(Set::new(k, "hello")).await.unwrap();
    let len = c.execute(SetRange::new(k, 6, "world")).await.unwrap();
    assert_eq!(len, 11); // "hello\0world"
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_strlen() {
    let c = conn().await;
    let k = "cover:strings:strlen";
    c.execute(Set::new(k, "hello")).await.unwrap();
    let len = c.execute(StrLen::new(k)).await.unwrap();
    assert_eq!(len, 5);
    c.execute(Del::new(k)).await.unwrap();
}
