use std::sync::OnceLock;

use bytes::Bytes;
use redis_test_harness::standalone::{RedisStandalone, StandaloneConfig};
use redis_tower::commands::*;
use redis_tower::{Frame, RedisConnection};
use tower::Service;

/// Shared Redis instance -- started once, stopped on Drop.
static REDIS: OnceLock<RedisStandalone> = OnceLock::new();

fn ensure_redis() -> &'static RedisStandalone {
    REDIS.get_or_init(|| {
        if let Ok(url) = std::env::var("REDIS_URL") {
            let addr = url
                .strip_prefix("redis://")
                .unwrap_or(&url)
                .trim_end_matches('/')
                .to_string();
            if let Some((host, port_str)) = addr.rsplit_once(':') {
                if let Ok(port) = port_str.parse::<u16>() {
                    return RedisStandalone::new(StandaloneConfig {
                        port,
                        bind: host.to_string(),
                        ..Default::default()
                    });
                }
            }
        }

        let mut standalone = RedisStandalone::with_defaults();
        standalone.start().expect("failed to start Redis server");
        standalone
    })
}

fn redis_addr() -> String {
    ensure_redis().addr()
}

async fn conn() -> RedisConnection {
    let addr = redis_addr();
    RedisConnection::connect(&addr)
        .await
        .expect("failed to connect to Redis")
}

// ---------------------------------------------------------------------------
// Sorted Sets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zpopmin() {
    let mut c = conn().await;
    let key = "cover2:zset:zpopmin";

    c.call(Del::new(key)).await.unwrap();
    c.call(
        ZAdd::new(key)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let result = c.call(ZPopMin::new(key)).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, Bytes::from("a"));
    assert!((result[0].1 - 1.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn zpopmax() {
    let mut c = conn().await;
    let key = "cover2:zset:zpopmax";

    c.call(Del::new(key)).await.unwrap();
    c.call(
        ZAdd::new(key)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let result = c.call(ZPopMax::new(key)).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, Bytes::from("c"));
    assert!((result[0].1 - 3.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn zcount() {
    let mut c = conn().await;
    let key = "cover2:zset:zcount";

    c.call(Del::new(key)).await.unwrap();
    c.call(
        ZAdd::new(key)
            .member(1.0, "a")
            .member(2.0, "b")
            .member(3.0, "c"),
    )
    .await
    .unwrap();

    let count = c.call(ZCount::new(key, "1", "2")).await.unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn zlexcount() {
    let mut c = conn().await;
    let key = "cover2:zset:zlexcount";

    c.call(Del::new(key)).await.unwrap();
    c.call(
        ZAdd::new(key)
            .member(0.0, "a")
            .member(0.0, "b")
            .member(0.0, "c"),
    )
    .await
    .unwrap();

    let count = c.call(ZLexCount::new(key, "[a", "[c")).await.unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn zrandmember() {
    let mut c = conn().await;
    let key = "cover2:zset:zrandmember";

    c.call(Del::new(key)).await.unwrap();
    c.call(ZAdd::new(key).member(1.0, "a").member(2.0, "b"))
        .await
        .unwrap();

    let result = c.call(ZRandMember::new(key).count(2)).await.unwrap();
    assert!(!result.is_empty());
}

#[tokio::test]
async fn zmscore() {
    let mut c = conn().await;
    let key = "cover2:zset:zmscore";

    c.call(Del::new(key)).await.unwrap();
    c.call(ZAdd::new(key).member(1.0, "a").member(2.0, "b"))
        .await
        .unwrap();

    let scores = c
        .call(ZMScore::members(key, ["a", "b", "missing"]))
        .await
        .unwrap();
    assert_eq!(scores.len(), 3);
    assert!((scores[0].unwrap() - 1.0).abs() < f64::EPSILON);
    assert!((scores[1].unwrap() - 2.0).abs() < f64::EPSILON);
    assert!(scores[2].is_none());
}

// ---------------------------------------------------------------------------
// Hashes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hsetnx() {
    let mut c = conn().await;
    let key = "cover2:hash:hsetnx";

    c.call(Del::new(key)).await.unwrap();

    let first = c.call(HSetNx::new(key, "field1", "val1")).await.unwrap();
    assert!(first, "HSETNX should return true for a new field");

    let second = c.call(HSetNx::new(key, "field1", "val2")).await.unwrap();
    assert!(!second, "HSETNX should return false for an existing field");
}

#[tokio::test]
async fn hincrbyfloat() {
    let mut c = conn().await;
    let key = "cover2:hash:hincrbyfloat";

    c.call(Del::new(key)).await.unwrap();
    c.call(HSet::new(key, "field", "10.5")).await.unwrap();

    let result = c.call(HIncrByFloat::new(key, "field", 0.5)).await.unwrap();
    assert!((result - 11.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn hrandfield() {
    let mut c = conn().await;
    let key = "cover2:hash:hrandfield";

    c.call(Del::new(key)).await.unwrap();
    c.call(HSet::new(key, "f1", "v1").field("f2", "v2"))
        .await
        .unwrap();

    let result = c.call(HRandField::new(key).count(2)).await.unwrap();
    assert!(!result.is_empty());
}

// ---------------------------------------------------------------------------
// Geo
// ---------------------------------------------------------------------------

#[tokio::test]
async fn geoadd_geopos() {
    let mut c = conn().await;
    let key = "cover2:geo:geopos";

    c.call(Del::new(key)).await.unwrap();
    c.call(
        GeoAdd::new(key)
            .member(-122.4194, 37.7749, "San Francisco")
            .member(-73.9857, 40.7484, "New York"),
    )
    .await
    .unwrap();

    let positions = c
        .call(GeoPos::members(key, ["San Francisco", "New York"]))
        .await
        .unwrap();
    assert_eq!(positions.len(), 2);

    let sf = positions[0].unwrap();
    assert!((sf.0 - (-122.4194)).abs() < 0.01);
    assert!((sf.1 - 37.7749).abs() < 0.01);

    let ny = positions[1].unwrap();
    assert!((ny.0 - (-73.9857)).abs() < 0.01);
    assert!((ny.1 - 40.7484).abs() < 0.01);
}

#[tokio::test]
async fn geodist() {
    let mut c = conn().await;
    let key = "cover2:geo:geodist";

    c.call(Del::new(key)).await.unwrap();
    c.call(
        GeoAdd::new(key)
            .member(-122.4194, 37.7749, "San Francisco")
            .member(-73.9857, 40.7484, "New York"),
    )
    .await
    .unwrap();

    let dist = c
        .call(GeoDist::new(key, "San Francisco", "New York").unit(GeoUnit::Kilometers))
        .await
        .unwrap();
    assert!(dist.unwrap() > 0.0);
}

#[tokio::test]
async fn geohash() {
    let mut c = conn().await;
    let key = "cover2:geo:geohash";

    c.call(Del::new(key)).await.unwrap();
    c.call(
        GeoAdd::new(key)
            .member(-122.4194, 37.7749, "San Francisco")
            .member(-73.9857, 40.7484, "New York"),
    )
    .await
    .unwrap();

    let hashes = c
        .call(GeoHash::members(key, ["San Francisco", "New York"]))
        .await
        .unwrap();
    assert_eq!(hashes.len(), 2);
    assert!(!hashes[0].as_ref().unwrap().is_empty());
    assert!(!hashes[1].as_ref().unwrap().is_empty());
}

#[tokio::test]
async fn geosearch() {
    let mut c = conn().await;
    let key = "cover2:geo:geosearch";

    c.call(Del::new(key)).await.unwrap();
    c.call(
        GeoAdd::new(key)
            .member(13.361389, 38.115556, "Palermo")
            .member(15.087269, 37.502669, "Catania")
            .member(2.349014, 48.864716, "Paris"),
    )
    .await
    .unwrap();

    // Search within 200 km of Palermo -- should find Palermo and Catania.
    let members = c
        .call(
            GeoSearch::from_member(key, "Palermo")
                .by_radius(200.0, GeoUnit::Kilometers)
                .asc(),
        )
        .await
        .unwrap();
    assert!(members.len() >= 2);
    assert!(members.contains(&Bytes::from("Palermo")));
    assert!(members.contains(&Bytes::from("Catania")));
}

// ---------------------------------------------------------------------------
// HyperLogLog
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pfadd_pfcount() {
    let mut c = conn().await;
    let key = "cover2:hll:pfadd_pfcount";

    c.call(Del::new(key)).await.unwrap();
    c.call(PfAdd::elements(key, ["a", "b", "c", "d"]))
        .await
        .unwrap();

    let count = c.call(PfCount::new(key)).await.unwrap();
    assert!(
        (3..=5).contains(&count),
        "PFCOUNT should be approximately 4, got {count}"
    );
}

#[tokio::test]
async fn pfmerge() {
    let mut c = conn().await;
    let key1 = "cover2:hll:pfmerge:1";
    let key2 = "cover2:hll:pfmerge:2";
    let dest = "cover2:hll:pfmerge:dest";

    c.call(Del::new(key1)).await.unwrap();
    c.call(Del::new(key2)).await.unwrap();
    c.call(Del::new(dest)).await.unwrap();

    c.call(PfAdd::elements(key1, ["a", "b", "c"]))
        .await
        .unwrap();
    c.call(PfAdd::elements(key2, ["c", "d", "e"]))
        .await
        .unwrap();

    c.call(PfMerge::new(dest, [key1, key2])).await.unwrap();

    let merged_count = c.call(PfCount::new(dest)).await.unwrap();
    let count1 = c.call(PfCount::new(key1)).await.unwrap();
    let count2 = c.call(PfCount::new(key2)).await.unwrap();
    assert!(
        merged_count >= count1 && merged_count >= count2,
        "merged count ({merged_count}) should be >= individual counts ({count1}, {count2})"
    );
}

#[tokio::test]
async fn pfadd_returns_bool() {
    let mut c = conn().await;
    let key = "cover2:hll:pfadd_bool";

    c.call(Del::new(key)).await.unwrap();

    let first = c.call(PfAdd::new(key, "new_element")).await.unwrap();
    assert!(first, "PFADD should return true when cardinality changes");

    let second = c.call(PfAdd::new(key, "new_element")).await.unwrap();
    assert!(
        !second,
        "PFADD should return false when cardinality does not change"
    );
}

// ---------------------------------------------------------------------------
// Bitmap
// ---------------------------------------------------------------------------

#[tokio::test]
async fn setbit_getbit() {
    let mut c = conn().await;
    let key = "cover2:bitmap:setbit_getbit";

    c.call(Del::new(key)).await.unwrap();

    let old = c.call(SetBit::new(key, 7, 1)).await.unwrap();
    assert_eq!(old, 0, "SETBIT should return the old bit value (0)");

    let bit = c.call(GetBit::new(key, 7)).await.unwrap();
    assert_eq!(bit, 1);
}

#[tokio::test]
async fn bitcount() {
    let mut c = conn().await;
    let key = "cover2:bitmap:bitcount";

    c.call(Del::new(key)).await.unwrap();
    // Set all 8 bits via SETBIT.
    for i in 0..8 {
        c.call(SetBit::new(key, i, 1)).await.unwrap();
    }

    let count = c.call(BitCount::new(key)).await.unwrap();
    assert_eq!(count, 8);
}

#[tokio::test]
async fn bitpos() {
    let mut c = conn().await;
    let key = "cover2:bitmap:bitpos";

    c.call(Del::new(key)).await.unwrap();
    // First byte all zeros, second byte: set bits 8..16 via SETBIT.
    for i in 8..16 {
        c.call(SetBit::new(key, i, 1)).await.unwrap();
    }

    let pos = c.call(BitPos::new(key, 1)).await.unwrap();
    assert_eq!(pos, 8, "first set bit should be at position 8");
}

#[tokio::test]
async fn bitop() {
    let mut c = conn().await;
    let key1 = "cover2:bitmap:bitop:1";
    let key2 = "cover2:bitmap:bitop:2";
    let dest = "cover2:bitmap:bitop:dest";

    c.call(Del::new(key1)).await.unwrap();
    c.call(Del::new(key2)).await.unwrap();
    c.call(Del::new(dest)).await.unwrap();

    // key1: all 8 bits set (0xFF)
    for i in 0..8 {
        c.call(SetBit::new(key1, i, 1)).await.unwrap();
    }
    // key2: lower 4 bits set (0x0F = bits 4,5,6,7 in Redis bit ordering)
    for i in 4..8 {
        c.call(SetBit::new(key2, i, 1)).await.unwrap();
    }

    let len = c
        .call(BitOp::new(BitOperation::And, dest, [key1, key2]))
        .await
        .unwrap();
    assert_eq!(len, 1, "BITOP AND should return the length of the result");

    // AND of 0xFF and 0x0F should be 0x0F -- 4 bits set.
    let count = c.call(BitCount::new(dest)).await.unwrap();
    assert_eq!(count, 4);
}

// ---------------------------------------------------------------------------
// Scripting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn eval_basic() {
    let mut c = conn().await;

    let result = c.call(Eval::new("return 42")).await.unwrap();
    assert_eq!(result, Frame::Integer(42));
}

#[tokio::test]
async fn eval_with_keys() {
    let mut c = conn().await;
    let key = "cover2:scripting:eval_keys";

    c.call(Del::new(key)).await.unwrap();
    c.call(Set::new(key, "hello")).await.unwrap();

    let result = c
        .call(Eval::new("return redis.call('GET', KEYS[1])").key(key))
        .await
        .unwrap();
    assert_eq!(result, Frame::BulkString(Some(Bytes::from("hello"))));
}

#[tokio::test]
async fn script_load_evalsha() {
    let mut c = conn().await;

    let script = "return 99";
    let sha = c.call(ScriptLoad::new(script)).await.unwrap();
    assert!(!sha.is_empty(), "SCRIPT LOAD should return a SHA1 hash");

    let result = c.call(EvalSha::new(&sha)).await.unwrap();
    assert_eq!(result, Frame::Integer(99));
}
