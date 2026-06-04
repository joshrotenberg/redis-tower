mod common;

use std::time::{SystemTime, UNIX_EPOCH};

use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn hsetnx() {
    let mut c = conn().await;
    let key = "cover2:hash:hsetnx";

    c.execute(Del::new(key)).await.unwrap();

    let first = c.execute(HSetNx::new(key, "field1", "val1")).await.unwrap();
    assert!(first, "HSETNX should return true for a new field");

    let second = c.execute(HSetNx::new(key, "field1", "val2")).await.unwrap();
    assert!(!second, "HSETNX should return false for an existing field");
}

#[tokio::test]
async fn hincrbyfloat() {
    let mut c = conn().await;
    let key = "cover2:hash:hincrbyfloat";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(HSet::new(key, "field", "10.5")).await.unwrap();

    let result = c
        .execute(HIncrByFloat::new(key, "field", 0.5))
        .await
        .unwrap();
    assert!((result - 11.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn hrandfield() {
    let mut c = conn().await;
    let key = "cover2:hash:hrandfield";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(HSet::new(key, "f1", "v1").field("f2", "v2"))
        .await
        .unwrap();

    let result = c.execute(HRandField::new(key).count(2)).await.unwrap();
    assert!(!result.is_empty());
}

// ---------------------------------------------------------------------------
// Hash field expiry commands (issue #321, Redis 7.4+)
// ---------------------------------------------------------------------------

/// HEXPIRE sets a field-level TTL; HTTL reads it back; HEXPIRETIME returns the
/// absolute expiry timestamp.
#[tokio::test]
async fn hexpire_and_httl() {
    let mut c = conn().await;
    let key = "cover2:hash:hexpire";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(HSet::new(key, "f1", "v1")).await.unwrap();

    // Set a 60-second TTL on the field.
    let codes = c.execute(HExpire::new(key, 60, ["f1"])).await.unwrap();
    // Status code 1 = TTL was set.
    assert_eq!(codes[0], 1, "HEXPIRE should return 1 (set)");

    // HTTL should now return a positive value.
    let ttls = c.execute(HTtl::new(key, ["f1"])).await.unwrap();
    assert!(ttls[0] >= 1, "HTTL should be >= 1 after HEXPIRE");

    // HEXPIRETIME should return a future Unix timestamp (seconds).
    let times = c.execute(HExpireTime::new(key, ["f1"])).await.unwrap();
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!(
        times[0] > now_secs,
        "HEXPIRETIME should be a future timestamp"
    );

    c.execute(Del::new(key)).await.unwrap();
}

/// HPERSIST removes the field-level TTL; HTTL should then return -1.
#[tokio::test]
async fn hpersist_removes_expiry() {
    let mut c = conn().await;
    let key = "cover2:hash:hpersist";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(HSet::new(key, "f1", "v1")).await.unwrap();
    c.execute(HExpire::new(key, 60, ["f1"])).await.unwrap();

    // Verify TTL is set.
    let ttls_before = c.execute(HTtl::new(key, ["f1"])).await.unwrap();
    assert!(ttls_before[0] >= 1, "HTTL should be set before HPERSIST");

    // Remove the TTL.
    let codes = c.execute(HPersist::new(key, ["f1"])).await.unwrap();
    // Status code 1 = TTL was removed.
    assert_eq!(codes[0], 1, "HPERSIST should return 1 (removed)");

    // HTTL should now return -1 (no TTL).
    let ttls_after = c.execute(HTtl::new(key, ["f1"])).await.unwrap();
    assert_eq!(
        ttls_after[0], -1,
        "HTTL should be -1 after HPERSIST (no expiry)"
    );

    c.execute(Del::new(key)).await.unwrap();
}

/// HPEXPIRE sets a millisecond TTL; HPTTL reads it back; HPEXPIRETIME returns
/// the absolute expiry in milliseconds.
#[tokio::test]
async fn hpexpire_and_hpttl() {
    let mut c = conn().await;
    let key = "cover2:hash:hpexpire";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(HSet::new(key, "f1", "v1")).await.unwrap();

    // Set a 60-second TTL in milliseconds.
    let codes = c.execute(HPExpire::new(key, 60_000, ["f1"])).await.unwrap();
    assert_eq!(codes[0], 1, "HPEXPIRE should return 1 (set)");

    // HPTTL should return a positive millisecond value.
    let pttls = c.execute(HPTtl::new(key, ["f1"])).await.unwrap();
    assert!(pttls[0] >= 1, "HPTTL should be >= 1 after HPEXPIRE");

    // HPEXPIRETIME should return a future Unix timestamp in milliseconds.
    let ptimes = c.execute(HPExpireTime::new(key, ["f1"])).await.unwrap();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert!(
        ptimes[0] > now_ms,
        "HPEXPIRETIME should be a future ms timestamp"
    );

    c.execute(Del::new(key)).await.unwrap();
}

/// HEXPIREAT sets a field TTL using an absolute Unix timestamp (seconds).
#[tokio::test]
async fn hexpireat_absolute_timestamp() {
    let mut c = conn().await;
    let key = "cover2:hash:hexpireat";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(HSet::new(key, "f1", "v1")).await.unwrap();

    let future_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 60;

    let codes = c
        .execute(HExpireAt::new(key, future_ts, ["f1"]))
        .await
        .unwrap();
    assert_eq!(codes[0], 1, "HEXPIREAT should return 1 (set)");

    let ttls = c.execute(HTtl::new(key, ["f1"])).await.unwrap();
    assert!(ttls[0] >= 1, "HTTL should be >= 1 after HEXPIREAT");

    c.execute(Del::new(key)).await.unwrap();
}

/// HPEXPIREAT sets a field TTL using an absolute Unix timestamp (milliseconds).
#[tokio::test]
async fn hpexpireat_absolute_timestamp_ms() {
    let mut c = conn().await;
    let key = "cover2:hash:hpexpireat";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(HSet::new(key, "f1", "v1")).await.unwrap();

    let future_ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 60_000;

    let codes = c
        .execute(HPExpireAt::new(key, future_ts_ms, ["f1"]))
        .await
        .unwrap();
    assert_eq!(codes[0], 1, "HPEXPIREAT should return 1 (set)");

    let pttls = c.execute(HPTtl::new(key, ["f1"])).await.unwrap();
    assert!(pttls[0] >= 1, "HPTTL should be >= 1 after HPEXPIREAT");

    c.execute(Del::new(key)).await.unwrap();
}

/// Per-field status codes: -2 means the field does not exist.
#[tokio::test]
async fn hexpire_nonexistent_field_status_code() {
    let mut c = conn().await;
    let key = "cover2:hash:hexpire_nofield";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(HSet::new(key, "real_field", "v")).await.unwrap();

    // Apply expiry to a field that doesn't exist.
    let codes = c
        .execute(HExpire::new(key, 60, ["ghost_field"]))
        .await
        .unwrap();
    // Status code -2 = field does not exist.
    assert_eq!(
        codes[0], -2,
        "HEXPIRE on nonexistent field should return -2"
    );

    // HTTL on a nonexistent field also returns -2.
    let ttls = c.execute(HTtl::new(key, ["ghost_field"])).await.unwrap();
    assert_eq!(ttls[0], -2, "HTTL on nonexistent field should return -2");

    c.execute(Del::new(key)).await.unwrap();
}
