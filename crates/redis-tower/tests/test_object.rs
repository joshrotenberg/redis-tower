mod common;

use common::conn;
use redis_server_wrapper::RedisServer;
use redis_tower::RedisConnection;
use redis_tower::commands::*;

// Note on ObjectFreq: OBJECT FREQ only returns a meaningful value when the
// server's maxmemory-policy is set to an LFU policy (allkeys-lfu or
// volatile-lfu). Against a default server it errors. Rather than mutate the
// shared default-policy test instance, `cover_object_freq` boots a dedicated
// `allkeys-lfu` server for the duration of the test, so it runs in the normal
// pass with no external infrastructure or `#[ignore]`.

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

// OBJECT FREQ only returns a value when maxmemory-policy is an LFU policy.
// This test boots a dedicated server configured with `allkeys-lfu` so it never
// touches the shared default-policy instance, and runs in the normal pass.
#[tokio::test]
async fn cover_object_freq() {
    // A dedicated LFU server on its own port; do not disturb the shared 6399
    // default-policy instance. Port 6388 is reserved for this fixture.
    let server = RedisServer::new()
        .port(6388)
        .maxmemory_policy("allkeys-lfu")
        .start()
        .await
        .expect("failed to start allkeys-lfu Redis server");
    let mut c = RedisConnection::connect(&server.addr())
        .await
        .expect("failed to connect to LFU server");

    let key = "cover:object:freq";

    // Confirm the dedicated server really runs an LFU policy.
    let policy = c.execute(ConfigGet::new("maxmemory-policy")).await.unwrap();
    let policy_val = policy
        .into_iter()
        .find(|(k, _)| k.as_ref() == b"maxmemory-policy")
        .map(|(_, v)| String::from_utf8_lossy(&v).into_owned())
        .unwrap_or_default();
    assert_eq!(policy_val, "allkeys-lfu", "server should run LFU policy");

    // Write the key and access it so it accrues a non-zero access frequency.
    c.execute(Del::new(key)).await.unwrap();
    c.execute(Set::new(key, "val")).await.unwrap();
    let _ = c.execute(Get::new(key)).await.unwrap();

    // OBJECT FREQ should report a numeric (>= 0) frequency counter.
    let freq = c.execute(ObjectFreq::new(key)).await.unwrap();
    assert!(freq >= 0, "object freq should be >= 0, got {freq}");

    c.execute(Del::new(key)).await.unwrap();
}
