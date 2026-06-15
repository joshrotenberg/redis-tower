//! Big-value and big-pipeline edge tests (#479).
//!
//! Megabyte-scale server round-trips and deep pipelines that the rest of the
//! suite does not exercise -- cheap, deterministic codec-bug catchers that also
//! produce quotable numbers (round-trips 64 MB values and 10k-deep pipelines in
//! CI). Run across `RedisConnection` and `MultiplexedClient`; the cluster
//! variants live in the cluster integration suite (`#[ignore]`).

mod common;

use common::{conn, redis_addr};
use futures::future::join_all;
use redis_tower::commands::*;
use redis_tower::{MultiplexedClient, Pipeline, RedisExecutor};

const MB: usize = 1024 * 1024;

/// Round-trip a `size`-byte value through SET/GET on any executor and assert it
/// comes back byte-for-byte.
async fn assert_value_roundtrip<E: RedisExecutor>(c: &mut E, label: &str, size: usize) {
    let key = format!("cover2:large:{label}");
    let _ = c.execute(Del::new(key.clone())).await;

    let value = "v".repeat(size);
    c.execute(Set::new(key.clone(), value.clone()))
        .await
        .unwrap_or_else(|e| panic!("{label}: SET of {size} bytes failed: {e:?}"));

    let got = c
        .execute(Get::new(key.clone()))
        .await
        .unwrap()
        .expect("value should be present");
    assert_eq!(got.len(), size, "{label}: round-tripped length");
    assert_eq!(
        got.as_ref(),
        value.as_bytes(),
        "{label}: round-tripped bytes"
    );

    // Free the big value before the next step.
    let _ = c.execute(Del::new(key)).await;
}

/// Exercise the codec/executor at scale: MB-size values, a wide MGET, and a
/// large HGETALL. Shared across executor types.
async fn run_large_suite<E: RedisExecutor>(c: &mut E, tag: &str) {
    // Megabyte-scale value round-trips.
    assert_value_roundtrip(c, &format!("{tag}-1mb"), MB).await;
    assert_value_roundtrip(c, &format!("{tag}-16mb"), 16 * MB).await;
    assert_value_roundtrip(c, &format!("{tag}-64mb"), 64 * MB).await;

    // 10k-key MGET.
    let keys: Vec<String> = (0..10_000)
        .map(|i| format!("cover2:mget:{tag}:{i}"))
        .collect();
    c.execute(MSet::new(keys.iter().map(|k| (k.clone(), "1"))))
        .await
        .unwrap();
    let got = c.execute(MGet::new(keys.clone())).await.unwrap();
    assert_eq!(got.len(), 10_000, "{tag}: MGET should return 10k entries");
    assert!(
        got.iter().all(|v| v.is_some()),
        "{tag}: every MGET key should be present"
    );

    // 1000-member HGETALL.
    let hkey = format!("cover2:hgetall:{tag}");
    let _ = c.execute(Del::new(hkey.clone())).await;
    let fields = (0..1000).map(|i| (format!("f{i}"), format!("v{i}")));
    c.execute(HSet::from_fields(hkey.clone(), fields))
        .await
        .unwrap();
    let all = c.execute(HGetAll::new(hkey.clone())).await.unwrap();
    assert_eq!(all.len(), 1000, "{tag}: HGETALL should return 1000 members");
    let _ = c.execute(Del::new(hkey)).await;
}

#[tokio::test]
async fn large_values_redis_connection() {
    let mut c = conn().await;
    run_large_suite(&mut c, "rc").await;
}

#[tokio::test]
async fn large_values_multiplexed() {
    let addr = redis_addr().await;
    let mut c = MultiplexedClient::connect(addr).await.unwrap();
    run_large_suite(&mut c, "mux").await;
}

/// A 10k-command explicit pipeline on a single connection.
#[tokio::test]
async fn deep_pipeline_redis_connection() {
    let mut c = conn().await;

    let mut p = Pipeline::new();
    for i in 0..10_000 {
        p = p.push(Set::new(format!("cover2:pipe:{i}"), i.to_string()));
    }
    let results = p.execute(&mut c).await.unwrap();
    assert_eq!(results.len(), 10_000, "pipeline should return 10k results");

    // Confirm the writes actually landed.
    let mid = c.execute(Get::new("cover2:pipe:5000")).await.unwrap();
    assert_eq!(mid.as_deref(), Some(&b"5000"[..]));
}

/// 10k concurrent commands through the multiplexed client's auto-pipeline.
#[tokio::test]
async fn deep_concurrent_multiplexed() {
    let addr = redis_addr().await;
    let client = MultiplexedClient::connect(addr).await.unwrap();

    let tasks = (0..10_000).map(|i| {
        let c = client.clone();
        async move {
            c.execute(Set::new(format!("cover2:mux-pipe:{i}"), i.to_string()))
                .await
        }
    });
    let results = join_all(tasks).await;
    assert_eq!(results.len(), 10_000);
    assert!(
        results.iter().all(|r| r.is_ok()),
        "all 10k concurrent SETs should succeed"
    );

    let c = client.clone();
    let last = c.execute(Get::new("cover2:mux-pipe:9999")).await.unwrap();
    assert_eq!(last.as_deref(), Some(&b"9999"[..]));
}
