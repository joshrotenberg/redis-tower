mod common;

use common::conn;
use redis_tower::commands::*;

// Note on ObjectFreq: OBJECT FREQ only returns a meaningful value when the
// server's maxmemory-policy is set to an LFU policy (allkeys-lfu or
// volatile-lfu). Against a default server it errors. The `cover_object_freq`
// fixture below exercises it, but it must mutate server-wide config and is
// therefore marked `#[ignore]` so the shared default-policy test instance is
// left untouched during normal runs. See that test for the run instruction.

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
// This fixture flips the server to `allkeys-lfu` for the duration of the test
// and restores the previous policy afterwards. Because it mutates server-wide
// config on the shared test instance, it is `#[ignore]`d by default. Run it
// explicitly and single-threaded so no concurrent test observes the LFU policy:
//
//   cargo test -p redis-tower --test test_object cover_object_freq \
//       -- --ignored --test-threads=1
#[tokio::test]
#[ignore = "mutates server-wide maxmemory-policy; requires LFU. Run with --ignored --test-threads=1"]
async fn cover_object_freq() {
    let mut c = conn().await;
    let key = "cover:object:freq";

    // Capture the current policy so we can restore it at the end.
    let prior = c.execute(ConfigGet::new("maxmemory-policy")).await.unwrap();
    let prior_policy = prior
        .into_iter()
        .find(|(k, _)| k.as_ref() == b"maxmemory-policy")
        .map(|(_, v)| String::from_utf8_lossy(&v).into_owned())
        .unwrap_or_else(|| "noeviction".to_string());

    // Switch to an LFU policy so OBJECT FREQ is meaningful.
    c.execute(ConfigSet::new("maxmemory-policy", "allkeys-lfu"))
        .await
        .unwrap();

    // Write the key and access it so it accrues a non-zero access frequency.
    c.execute(Del::new(key)).await.unwrap();
    c.execute(Set::new(key, "val")).await.unwrap();
    let _ = c.execute(Get::new(key)).await.unwrap();

    // OBJECT FREQ should report a numeric (>= 0) frequency counter.
    let freq = c.execute(ObjectFreq::new(key)).await.unwrap();
    assert!(freq >= 0, "object freq should be >= 0, got {freq}");

    // Clean up the key and restore the original eviction policy.
    c.execute(Del::new(key)).await.unwrap();
    c.execute(ConfigSet::new("maxmemory-policy", prior_policy))
        .await
        .unwrap();
}
