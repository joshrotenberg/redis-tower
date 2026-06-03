mod common;

use common::conn;
use redis_tower::commands::*;

// Note on ObjectFreq: it is intentionally not tested here. OBJECT FREQ only
// returns a meaningful value when the server's maxmemory-policy is set to an
// LFU policy (allkeys-lfu or volatile-lfu). Against a default server it errors,
// so covering it would require mutating server-wide config for the shared test
// instance, which the other tests rely on being left at defaults.

#[tokio::test]
async fn cover_object_encoding_int() {
    let mut c = conn().await;
    let key = "cover:object:encoding_int";
    c.execute(Del::new(key)).await.unwrap();
    c.execute(Set::new(key, "42")).await.unwrap();
    let encoding = c.execute(ObjectEncoding::new(key)).await.unwrap();
    assert_eq!(encoding, "int");
    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn cover_object_encoding_embstr() {
    let mut c = conn().await;
    let key = "cover:object:encoding_embstr";
    c.execute(Del::new(key)).await.unwrap();
    c.execute(Set::new(key, "hello")).await.unwrap();
    let encoding = c.execute(ObjectEncoding::new(key)).await.unwrap();
    assert_eq!(encoding, "embstr");
    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn cover_object_encoding_raw() {
    let mut c = conn().await;
    let key = "cover:object:encoding_raw";
    c.execute(Del::new(key)).await.unwrap();
    // A value longer than 44 bytes is stored as a raw string.
    let long = "x".repeat(64);
    c.execute(Set::new(key, long)).await.unwrap();
    let encoding = c.execute(ObjectEncoding::new(key)).await.unwrap();
    assert_eq!(encoding, "raw");
    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn cover_object_encoding_list() {
    let mut c = conn().await;
    let key = "cover:object:encoding_list";
    c.execute(Del::new(key)).await.unwrap();
    c.execute(LPush::elements(key, vec!["a", "b", "c"]))
        .await
        .unwrap();
    let encoding = c.execute(ObjectEncoding::new(key)).await.unwrap();
    // Redis 7.2+ uses listpack for small lists; older/larger lists use quicklist.
    assert!(
        encoding == "listpack" || encoding == "quicklist",
        "unexpected list encoding: {encoding}"
    );
    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn cover_object_refcount() {
    let mut c = conn().await;
    let key = "cover:object:refcount";
    c.execute(Del::new(key)).await.unwrap();
    c.execute(Set::new(key, "val")).await.unwrap();
    let refcount = c.execute(ObjectRefCount::new(key)).await.unwrap();
    assert!(refcount >= 1, "refcount should be >= 1, got {refcount}");
    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn cover_object_idletime() {
    let mut c = conn().await;
    let key = "cover:object:idletime";
    c.execute(Del::new(key)).await.unwrap();
    c.execute(Set::new(key, "val")).await.unwrap();
    let idletime = c.execute(ObjectIdleTime::new(key)).await.unwrap();
    assert!(idletime >= 0, "idletime should be >= 0, got {idletime}");
    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn cover_object_help() {
    let mut c = conn().await;
    let help = c.execute(ObjectHelp::new()).await.unwrap();
    assert!(
        !help.is_empty(),
        "OBJECT HELP should return a non-empty list"
    );
}
