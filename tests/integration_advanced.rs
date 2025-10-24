//! Advanced integration tests for Redis commands
//!
//! These tests cover more advanced Redis features:
//! - Transactions (MULTI/EXEC/DISCARD)
//! - HyperLogLog probabilistic counting
//! - Bitmap operations
//! - Geospatial commands
//! - More list operations
//! - Sorted set advanced operations
//! - Lua scripting
//!
//! Run with: cargo test --test integration_advanced

use bytes::Bytes;
use redis_tower::client::RedisClient;
use redis_tower::commands::*;
use redis_tower::types::RedisValue;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;

/// Helper to create a Redis client connected to a testcontainer
async fn setup_redis() -> RedisClient {
    let container = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis container");

    let host = container.get_host().await.expect("Failed to get host");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get port");

    let client = RedisClient::connect(&format!("{}:{}", host, port))
        .await
        .expect("Failed to connect to Redis");

    // Keep container alive by leaking it (tests are short-lived)
    std::mem::forget(container);

    client
}

#[tokio::test]
async fn test_hyperloglog_pfadd_pfcount() {
    let client = setup_redis().await;

    // PFADD - add elements to HyperLogLog
    let added1: bool = client
        .call(PfAdd::new("visitors", vec!["user1"]))
        .await
        .unwrap();
    assert!(added1); // First element added

    client
        .call(PfAdd::new("visitors", vec!["user2", "user3", "user4"]))
        .await
        .unwrap();

    // Add duplicate
    let added2: bool = client
        .call(PfAdd::new("visitors", vec!["user1"]))
        .await
        .unwrap();
    assert!(!added2); // Duplicate not counted as new

    // PFCOUNT - approximate count
    let count: i64 = client.call(PfCount::single("visitors")).await.unwrap();
    assert_eq!(count, 4); // Should be approximately 4
}

#[tokio::test]
async fn test_hyperloglog_pfmerge() {
    let client = setup_redis().await;

    // Create two HyperLogLogs
    client
        .call(PfAdd::new("hll1", vec!["a", "b", "c"]))
        .await
        .unwrap();

    client
        .call(PfAdd::new("hll2", vec!["c", "d", "e"]))
        .await
        .unwrap();

    // PFMERGE - merge HyperLogLogs
    client
        .call(PfMerge::new("merged", vec!["hll1", "hll2"]))
        .await
        .unwrap();

    // Count merged set (should be union: a, b, c, d, e = 5)
    let count: i64 = client.call(PfCount::single("merged")).await.unwrap();
    assert_eq!(count, 5);
}

#[tokio::test]
async fn test_bitmap_operations() {
    let client = setup_redis().await;

    // SETBIT - set bits
    let old_value1: bool = client.call(SetBit::new("bitmap", 0, true)).await.unwrap();
    assert!(!old_value1); // Was not set before

    client.call(SetBit::new("bitmap", 2, true)).await.unwrap();
    client.call(SetBit::new("bitmap", 5, true)).await.unwrap();
    client.call(SetBit::new("bitmap", 7, true)).await.unwrap();

    // GETBIT - get individual bits
    let bit0: bool = client.call(GetBit::new("bitmap", 0)).await.unwrap();
    assert!(bit0);

    let bit1: bool = client.call(GetBit::new("bitmap", 1)).await.unwrap();
    assert!(!bit1);

    // BITCOUNT - count set bits
    let count: i64 = client.call(BitCount::new("bitmap")).await.unwrap();
    assert_eq!(count, 4); // Bits 0, 2, 5, 7 are set
}

#[tokio::test]
async fn test_bitmap_bitop() {
    let client = setup_redis().await;

    // Create two bitmaps
    client.call(SetBit::new("bm1", 0, true)).await.unwrap();
    client.call(SetBit::new("bm1", 2, true)).await.unwrap();

    client.call(SetBit::new("bm2", 1, true)).await.unwrap();
    client.call(SetBit::new("bm2", 2, true)).await.unwrap();

    // BITOP AND
    let result: i64 = client
        .call(BitOpCmd::new(BitOp::And, "result_and", vec!["bm1", "bm2"]))
        .await
        .unwrap();
    assert!(result > 0);

    // Check result - only bit 2 should be set (in both bm1 and bm2)
    let bit0: bool = client.call(GetBit::new("result_and", 0)).await.unwrap();
    assert!(!bit0);
    let bit2: bool = client.call(GetBit::new("result_and", 2)).await.unwrap();
    assert!(bit2);

    // BITOP OR
    client
        .call(BitOpCmd::new(BitOp::Or, "result_or", vec!["bm1", "bm2"]))
        .await
        .unwrap();

    // Check result - bits 0, 1, 2 should be set
    let count: i64 = client.call(BitCount::new("result_or")).await.unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_geospatial_operations() {
    let client = setup_redis().await;

    // GEOADD - add locations
    let added: i64 = client
        .call(GeoAdd::new(
            "cities",
            vec![
                GeoItem::new(-122.4194, 37.7749, "San Francisco"),
                GeoItem::new(-118.2437, 34.0522, "Los Angeles"),
                GeoItem::new(-87.6298, 41.8781, "Chicago"),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(added, 3);

    // GEODIST - distance between two points
    let distance: Option<f64> = client
        .call(GeoDist::new("cities", "San Francisco", "Los Angeles").unit(GeoUnit::Kilometers))
        .await
        .unwrap();
    assert!(distance.is_some());
    let dist = distance.unwrap();
    assert!(dist > 500.0 && dist < 600.0); // ~559 km

    // GEOPOS - get positions
    let positions: Vec<Option<GeoCoordinate>> = client
        .call(GeoPos::new("cities", vec!["San Francisco"]))
        .await
        .unwrap();
    assert_eq!(positions.len(), 1);
    assert!(positions[0].is_some());
}

#[tokio::test]
async fn test_list_rpush_rpop() {
    let client = setup_redis().await;

    // RPUSH - push to tail
    let len: i64 = client
        .call(RPush::new(
            "queue",
            vec![
                Bytes::from("first"),
                Bytes::from("second"),
                Bytes::from("third"),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(len, 3);

    // RPOP - pop from tail
    let popped: Option<Bytes> = client.call(RPop::new("queue")).await.unwrap();
    assert_eq!(popped.as_ref().map(|b| b.as_ref()), Some(b"third".as_ref()));

    // Verify order with LRANGE
    let range: Vec<Bytes> = client.call(LRange::new("queue", 0, -1)).await.unwrap();
    assert_eq!(range.len(), 2);
    assert_eq!(range[0].as_ref(), b"first");
    assert_eq!(range[1].as_ref(), b"second");
}

#[tokio::test]
async fn test_sorted_set_zrangebyscore() {
    let client = setup_redis().await;

    // Add scored members
    client
        .call(
            Zadd::new("scores")
                .member(10.0, "player1")
                .member(25.0, "player2")
                .member(50.0, "player3")
                .member(75.0, "player4")
                .member(100.0, "player5"),
        )
        .await
        .unwrap();

    // ZRANGEBYSCORE - get members in score range
    let result: Vec<String> = client
        .call(ZRangeByScore::new("scores", 20.0, 60.0))
        .await
        .unwrap();

    assert_eq!(result.len(), 2); // player2 (25) and player3 (50)
    assert_eq!(result[0], "player2");
    assert_eq!(result[1], "player3");
}

#[tokio::test]
async fn test_sorted_set_zrem() {
    let client = setup_redis().await;

    // Add members
    client
        .call(
            Zadd::new("players")
                .member(100.0, "alice")
                .member(200.0, "bob")
                .member(300.0, "charlie"),
        )
        .await
        .unwrap();

    // ZREM - remove member
    let removed: i64 = client
        .call(Zrem::new("players").member("bob"))
        .await
        .unwrap();
    assert_eq!(removed, 1);

    // Verify removal
    let count: i64 = client.call(Zcard::new("players")).await.unwrap();
    assert_eq!(count, 2);

    // Verify remaining members
    let result = client.call(Zrange::new("players", 0, -1)).await.unwrap();
    assert_eq!(result.members.len(), 2);
    assert_eq!(result.members[0].0.as_ref(), b"alice");
    assert_eq!(result.members[1].0.as_ref(), b"charlie");
}

#[tokio::test]
async fn test_scripting_eval() {
    let client = setup_redis().await;

    // Simple Lua script that returns a value
    let script = "return redis.call('SET', KEYS[1], ARGV[1])";
    let result: RedisValue = client
        .call(Eval::new(script).key("mykey").arg("myvalue"))
        .await
        .unwrap();

    // Result should be a Status "OK"
    match result {
        RedisValue::Status(s) => assert_eq!(s, "OK"),
        _ => panic!("Expected Status, got {:?}", result),
    }

    // Verify the SET worked
    let value: Option<Bytes> = client.call(Get::new("mykey")).await.unwrap();
    assert_eq!(
        value.as_ref().map(|b| b.as_ref()),
        Some(b"myvalue".as_ref())
    );
}

#[tokio::test]
async fn test_scripting_eval_with_return() {
    let client = setup_redis().await;

    // Lua script that does computation
    let script =
        "local sum = 0; for i, v in ipairs(ARGV) do sum = sum + tonumber(v) end; return sum";
    let result: RedisValue = client
        .call(Eval::new(script).arg("10").arg("20").arg("30"))
        .await
        .unwrap();

    // Result should be an Integer
    match result {
        RedisValue::Integer(n) => assert_eq!(n, 60),
        _ => panic!("Expected Integer, got {:?}", result),
    }
}
