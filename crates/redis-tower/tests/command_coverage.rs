use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use redis_test_harness::standalone::{RedisStandalone, StandaloneConfig};
use redis_tower::RedisConnection;
use redis_tower::commands::*;

/// Shared Redis instance -- started once, stopped on Drop.
static REDIS: OnceLock<RedisStandalone> = OnceLock::new();

fn ensure_redis() -> &'static RedisStandalone {
    REDIS.get_or_init(|| {
        // Check for external Redis first (CI service container).
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
// Strings
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cover_unlink() {
    let c = conn().await;
    let k = "cover:keys:unlink";
    c.execute(Set::new(k, "val")).await.unwrap();
    let removed = c.execute(Unlink::new(k)).await.unwrap();
    assert_eq!(removed, 1);
    let gone = c.execute(Get::new(k)).await.unwrap();
    assert_eq!(gone, None);
}

#[tokio::test]
async fn cover_persist() {
    let c = conn().await;
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
    let c = conn().await;
    let k = "cover:keys:pexpire";
    c.execute(Set::new(k, "val")).await.unwrap();
    let ok = c.execute(PExpire::new(k, 60000)).await.unwrap();
    assert!(ok);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_pexpireat() {
    let c = conn().await;
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
    let c = conn().await;
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
    let c = conn().await;
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
    let c = conn().await;
    let k = "cover:keys:randomkey";
    c.execute(Set::new(k, "val")).await.unwrap();
    let rk = c.execute(RandomKey::new()).await.unwrap();
    assert!(rk.is_some());
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_touch() {
    let c = conn().await;
    let k = "cover:keys:touch";
    c.execute(Set::new(k, "val")).await.unwrap();
    let n = c.execute(Touch::new(k)).await.unwrap();
    assert_eq!(n, 1);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_expiretime() {
    let c = conn().await;
    let k = "cover:keys:expiretime";
    c.execute(Set::new(k, "val")).await.unwrap();
    c.execute(Expire::new(k, 60)).await.unwrap();
    let ts = c.execute(ExpireTime::new(k)).await.unwrap();
    assert!(ts > 0);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_pexpiretime() {
    let c = conn().await;
    let k = "cover:keys:pexpiretime";
    c.execute(Set::new(k, "val")).await.unwrap();
    c.execute(PExpire::new(k, 60000)).await.unwrap();
    let ts = c.execute(PExpireTime::new(k)).await.unwrap();
    assert!(ts > 0);
    c.execute(Del::new(k)).await.unwrap();
}

// ---------------------------------------------------------------------------
// Lists
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cover_lpushx() {
    let c = conn().await;
    let k = "cover:lists:lpushx";
    c.execute(Del::new(k)).await.unwrap();
    // On missing key, LPUSHX returns 0.
    let n = c.execute(LPushX::new(k, "a")).await.unwrap();
    assert_eq!(n, 0);
    // Create the list first.
    c.execute(LPush::new(k, "x")).await.unwrap();
    let n = c.execute(LPushX::new(k, "y")).await.unwrap();
    assert_eq!(n, 2);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_rpushx() {
    let c = conn().await;
    let k = "cover:lists:rpushx";
    c.execute(Del::new(k)).await.unwrap();
    let n = c.execute(RPushX::new(k, "a")).await.unwrap();
    assert_eq!(n, 0);
    c.execute(RPush::new(k, "x")).await.unwrap();
    let n = c.execute(RPushX::new(k, "y")).await.unwrap();
    assert_eq!(n, 2);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_linsert() {
    let c = conn().await;
    let k = "cover:lists:linsert";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(RPush::elements(k, ["a", "b", "c"]))
        .await
        .unwrap();
    let len = c
        .execute(LInsert::new(k, ListPosition::Before, "b", "x"))
        .await
        .unwrap();
    assert_eq!(len, 4);
    let items = c.execute(LRange::new(k, 0, -1)).await.unwrap();
    assert_eq!(
        items,
        vec![
            Bytes::from("a"),
            Bytes::from("x"),
            Bytes::from("b"),
            Bytes::from("c"),
        ]
    );
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_lrem() {
    let c = conn().await;
    let k = "cover:lists:lrem";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(RPush::elements(k, ["a", "b", "a", "c", "a"]))
        .await
        .unwrap();
    let removed = c.execute(LRem::new(k, 2, "a")).await.unwrap();
    assert_eq!(removed, 2);
    let items = c.execute(LRange::new(k, 0, -1)).await.unwrap();
    assert_eq!(
        items,
        vec![Bytes::from("b"), Bytes::from("c"), Bytes::from("a")]
    );
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_ltrim() {
    let c = conn().await;
    let k = "cover:lists:ltrim";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(RPush::elements(k, ["a", "b", "c", "d", "e"]))
        .await
        .unwrap();
    c.execute(LTrim::new(k, 1, 3)).await.unwrap();
    let items = c.execute(LRange::new(k, 0, -1)).await.unwrap();
    assert_eq!(
        items,
        vec![Bytes::from("b"), Bytes::from("c"), Bytes::from("d")]
    );
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_lpos() {
    let c = conn().await;
    let k = "cover:lists:lpos";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(RPush::elements(k, ["a", "b", "c", "b", "d"]))
        .await
        .unwrap();
    let pos = c.execute(LPos::new(k, "b")).await.unwrap();
    assert_eq!(pos, Some(1));
    c.execute(Del::new(k)).await.unwrap();
}

// ---------------------------------------------------------------------------
// Sets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cover_srandmember() {
    let c = conn().await;
    let k = "cover:sets:srandmember";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(SAdd::members(k, ["a", "b", "c"])).await.unwrap();
    let members = c.execute(SRandMember::new(k)).await.unwrap();
    assert!(!members.is_empty());
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_spop() {
    let c = conn().await;
    let k = "cover:sets:spop";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(SAdd::members(k, ["a", "b", "c"])).await.unwrap();
    let popped = c.execute(SPop::new(k)).await.unwrap();
    assert!(!popped.is_empty());
    let card = c.execute(SCard::new(k)).await.unwrap();
    assert_eq!(card, 2);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_sdiff() {
    let c = conn().await;
    let s1 = "cover:sets:sdiff:s1";
    let s2 = "cover:sets:sdiff:s2";
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
    c.execute(SAdd::members(s1, ["a", "b", "c"])).await.unwrap();
    c.execute(SAdd::members(s2, ["b", "c", "d"])).await.unwrap();
    let diff = c.execute(SDiff::keys([s1, s2])).await.unwrap();
    assert_eq!(diff, vec![Bytes::from("a")]);
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
}

#[tokio::test]
async fn cover_sdiffstore() {
    let c = conn().await;
    let s1 = "cover:sets:sdiffstore:s1";
    let s2 = "cover:sets:sdiffstore:s2";
    let dst = "cover:sets:sdiffstore:dst";
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
    c.execute(SAdd::members(s1, ["a", "b", "c"])).await.unwrap();
    c.execute(SAdd::members(s2, ["b", "c", "d"])).await.unwrap();
    let n = c.execute(SDiffStore::new(dst, [s1, s2])).await.unwrap();
    assert_eq!(n, 1);
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
}

#[tokio::test]
async fn cover_sunion() {
    let c = conn().await;
    let s1 = "cover:sets:sunion:s1";
    let s2 = "cover:sets:sunion:s2";
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
    c.execute(SAdd::members(s1, ["a", "b"])).await.unwrap();
    c.execute(SAdd::members(s2, ["b", "c"])).await.unwrap();
    let union = c.execute(SUnion::keys([s1, s2])).await.unwrap();
    assert_eq!(union.len(), 3);
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
}

#[tokio::test]
async fn cover_sunionstore() {
    let c = conn().await;
    let s1 = "cover:sets:sunionstore:s1";
    let s2 = "cover:sets:sunionstore:s2";
    let dst = "cover:sets:sunionstore:dst";
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
    c.execute(SAdd::members(s1, ["a", "b"])).await.unwrap();
    c.execute(SAdd::members(s2, ["b", "c"])).await.unwrap();
    let n = c.execute(SUnionStore::new(dst, [s1, s2])).await.unwrap();
    assert_eq!(n, 3);
    c.execute(Del::new(s1)).await.unwrap();
    c.execute(Del::new(s2)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
}

#[tokio::test]
async fn cover_smove() {
    let c = conn().await;
    let src = "cover:sets:smove:src";
    let dst = "cover:sets:smove:dst";
    c.execute(Del::new(src)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
    c.execute(SAdd::members(src, ["a", "b"])).await.unwrap();
    c.execute(SAdd::new(dst, "c")).await.unwrap();
    let ok = c.execute(SMove::new(src, dst, "a")).await.unwrap();
    assert!(ok);
    c.execute(Del::new(src)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
}

#[tokio::test]
async fn cover_smismember() {
    let c = conn().await;
    let k = "cover:sets:smismember";
    c.execute(Del::new(k)).await.unwrap();
    c.execute(SAdd::members(k, ["a", "b", "c"])).await.unwrap();
    let results = c
        .execute(SMisMember::members(k, ["a", "x", "b"]))
        .await
        .unwrap();
    assert_eq!(results, vec![true, false, true]);
    c.execute(Del::new(k)).await.unwrap();
}
