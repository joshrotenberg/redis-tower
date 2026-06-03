mod common;

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use common::conn;
use redis_tower::Frame;
use redis_tower::commands::*;

#[tokio::test]
async fn cover_unlink() {
    let mut c = conn().await;
    let k = "cover:keys:unlink";
    c.execute(Set::new(k, "val")).await.unwrap();
    let removed = c.execute(Unlink::new(k)).await.unwrap();
    assert_eq!(removed, 1);
    let gone = c.execute(Get::new(k)).await.unwrap();
    assert_eq!(gone, None);
}

#[tokio::test]
async fn cover_persist() {
    let mut c = conn().await;
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
    let mut c = conn().await;
    let k = "cover:keys:pexpire";
    c.execute(Set::new(k, "val")).await.unwrap();
    let ok = c.execute(PExpire::new(k, 60000)).await.unwrap();
    assert!(ok);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_pexpireat() {
    let mut c = conn().await;
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
    let mut c = conn().await;
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
    let mut c = conn().await;
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
    let mut c = conn().await;
    let k = "cover:keys:randomkey";
    c.execute(Set::new(k, "val")).await.unwrap();
    let rk = c.execute(RandomKey::new()).await.unwrap();
    assert!(rk.is_some());
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_touch() {
    let mut c = conn().await;
    let k = "cover:keys:touch";
    c.execute(Set::new(k, "val")).await.unwrap();
    let n = c.execute(Touch::new(k)).await.unwrap();
    assert_eq!(n, 1);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_expiretime() {
    let mut c = conn().await;
    let k = "cover:keys:expiretime";
    c.execute(Set::new(k, "val")).await.unwrap();
    c.execute(Expire::new(k, 60)).await.unwrap();
    let ts = c.execute(ExpireTime::new(k)).await.unwrap();
    assert!(ts > 0);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_pexpiretime() {
    let mut c = conn().await;
    let k = "cover:keys:pexpiretime";
    c.execute(Set::new(k, "val")).await.unwrap();
    c.execute(PExpire::new(k, 60000)).await.unwrap();
    let ts = c.execute(PExpireTime::new(k)).await.unwrap();
    assert!(ts > 0);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_dump_restore() {
    let mut c = conn().await;
    let src = "cover:keys:dump_restore:src";
    let dst = "cover:keys:dump_restore:dst";
    c.execute(Del::new(src)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
    c.execute(Set::new(src, "hello")).await.unwrap();
    let dump_data = c.execute(Dump::new(src)).await.unwrap();
    assert!(dump_data.is_some(), "DUMP should return serialized data");
    let serialized = dump_data.unwrap();
    c.execute(Restore::new(dst, 0, serialized)).await.unwrap();
    let restored = c.execute(Get::new(dst)).await.unwrap();
    assert_eq!(restored, Some(Bytes::from("hello")));
    c.execute(Del::new(src)).await.unwrap();
    c.execute(Del::new(dst)).await.unwrap();
}

fn extract_sort_strings(frame: Frame) -> Vec<String> {
    match frame {
        Frame::Array(Some(frames)) => frames
            .into_iter()
            .filter_map(|f| match f {
                Frame::BulkString(Some(b)) => Some(String::from_utf8_lossy(&b).to_string()),
                _ => None,
            })
            .collect(),
        _ => panic!("expected array from SORT"),
    }
}

#[tokio::test]
async fn cover_sort_basic() {
    let mut c = conn().await;
    let k = "cover:keys:sort_basic";
    c.execute(Del::new(k)).await.unwrap();
    for v in ["3", "1", "4", "1", "5", "9", "2", "6"] {
        c.execute(RPush::new(k, v)).await.unwrap();
    }

    let result = c.execute(Sort::new(k)).await.unwrap();
    let items: Vec<i64> = extract_sort_strings(result)
        .into_iter()
        .map(|s| s.parse().unwrap())
        .collect();
    assert_eq!(items, vec![1, 1, 2, 3, 4, 5, 6, 9]);

    let result_desc = c
        .execute(Sort::new(k).order(SortOrder::Desc))
        .await
        .unwrap();
    let items_desc: Vec<i64> = extract_sort_strings(result_desc)
        .into_iter()
        .map(|s| s.parse().unwrap())
        .collect();
    assert_eq!(items_desc, vec![9, 6, 5, 4, 3, 2, 1, 1]);

    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_sort_limit() {
    let mut c = conn().await;
    let k = "cover:keys:sort_limit";
    c.execute(Del::new(k)).await.unwrap();
    for v in 1..=10_i32 {
        c.execute(RPush::new(k, v.to_string())).await.unwrap();
    }
    let result = c.execute(Sort::new(k).limit(0, 3)).await.unwrap();
    let items = extract_sort_strings(result);
    assert_eq!(items.len(), 3);
    // SORT with LIMIT 0 3 on [1..10] ascending returns ["1", "2", "3"]
    assert_eq!(items, vec!["1", "2", "3"]);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_sort_alpha() {
    let mut c = conn().await;
    let k = "cover:keys:sort_alpha";
    c.execute(Del::new(k)).await.unwrap();
    for v in ["banana", "apple", "cherry"] {
        c.execute(RPush::new(k, v)).await.unwrap();
    }
    let result = c.execute(Sort::new(k).alpha()).await.unwrap();
    let items = extract_sort_strings(result);
    assert_eq!(items, vec!["apple", "banana", "cherry"]);
    c.execute(Del::new(k)).await.unwrap();
}

#[tokio::test]
async fn cover_sort_ro_basic() {
    let mut c = conn().await;
    let k = "cover:keys:sort_ro_basic";
    c.execute(Del::new(k)).await.unwrap();
    for v in ["3", "1", "2"] {
        c.execute(RPush::new(k, v)).await.unwrap();
    }
    let result = c.execute(SortRo::new(k)).await.unwrap();
    let items: Vec<i64> = result
        .into_iter()
        .filter_map(|opt| opt.map(|b| String::from_utf8_lossy(&b).parse().unwrap()))
        .collect();
    assert_eq!(items, vec![1, 2, 3]);
    c.execute(Del::new(k)).await.unwrap();
}
