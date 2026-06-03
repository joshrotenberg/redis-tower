mod common;

use bytes::Bytes;
use common::conn;
use redis_tower::ScanStream;
use redis_tower::commands::*;
use tokio_stream::StreamExt;

#[tokio::test]
async fn scan_all_keys() {
    let mut c = conn().await;
    let prefix = "scan_stream_test:key:";
    let keys: Vec<String> = (1..=5).map(|i| format!("{}{}", prefix, i)).collect();

    for k in &keys {
        c.execute(Del::new(k.as_str())).await.unwrap();
        c.execute(Set::new(k.as_str(), "val")).await.unwrap();
    }

    let pattern = format!("{}*", prefix);
    let found = {
        let stream = ScanStream::scan(&mut c, &pattern);
        tokio::pin!(stream);
        let mut acc: Vec<Bytes> = Vec::new();
        while let Some(item) = stream.next().await {
            acc.push(item.unwrap());
        }
        acc
    };

    for k in &keys {
        assert!(
            found.iter().any(|b| b == k.as_bytes()),
            "expected key {} in scan results",
            k
        );
        c.execute(Del::new(k.as_str())).await.unwrap();
    }
}

#[tokio::test]
async fn scan_with_match() {
    let mut c = conn().await;
    let prefix_a = "scan_stream_test:match_a:";
    let prefix_b = "scan_stream_test:match_b:";
    let keys_a: Vec<String> = (1..=5).map(|i| format!("{}{}", prefix_a, i)).collect();
    let keys_b: Vec<String> = (1..=5).map(|i| format!("{}{}", prefix_b, i)).collect();

    for k in keys_a.iter().chain(keys_b.iter()) {
        c.execute(Del::new(k.as_str())).await.unwrap();
        c.execute(Set::new(k.as_str(), "val")).await.unwrap();
    }

    let pattern = format!("{}*", prefix_a);
    let found = {
        let stream = ScanStream::scan(&mut c, &pattern);
        tokio::pin!(stream);
        let mut acc: Vec<Bytes> = Vec::new();
        while let Some(item) = stream.next().await {
            acc.push(item.unwrap());
        }
        acc
    };

    for k in &keys_a {
        assert!(
            found.iter().any(|b| b == k.as_bytes()),
            "expected key {} in scan results",
            k
        );
    }
    for k in &keys_b {
        assert!(
            !found.iter().any(|b| b == k.as_bytes()),
            "unexpected key {} in scan results",
            k
        );
    }

    for k in keys_a.iter().chain(keys_b.iter()) {
        c.execute(Del::new(k.as_str())).await.unwrap();
    }
}

#[tokio::test]
async fn scan_with_count() {
    let mut c = conn().await;
    let prefix = "scan_stream_test:count:";
    let keys: Vec<String> = (1..=5).map(|i| format!("{}{}", prefix, i)).collect();

    for k in &keys {
        c.execute(Del::new(k.as_str())).await.unwrap();
        c.execute(Set::new(k.as_str(), "val")).await.unwrap();
    }

    let pattern = format!("{}*", prefix);
    let found = {
        let stream = ScanStream::scan_with_count(&mut c, &pattern, 2);
        tokio::pin!(stream);
        let mut acc: Vec<Bytes> = Vec::new();
        while let Some(item) = stream.next().await {
            acc.push(item.unwrap());
        }
        acc
    };

    for k in &keys {
        assert!(
            found.iter().any(|b| b == k.as_bytes()),
            "expected key {} in scan_with_count results",
            k
        );
        c.execute(Del::new(k.as_str())).await.unwrap();
    }
}

#[tokio::test]
async fn hscan() {
    let mut c = conn().await;
    let key = "scan_stream_test:hscan";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        HSet::new(key, "field1", "value1")
            .field("field2", "value2")
            .field("field3", "value3")
            .field("field4", "value4")
            .field("field5", "value5"),
    )
    .await
    .unwrap();

    let found = {
        let stream = ScanStream::hscan(&mut c, key, "*");
        tokio::pin!(stream);
        let mut acc: Vec<(Bytes, Bytes)> = Vec::new();
        while let Some(item) = stream.next().await {
            acc.push(item.unwrap());
        }
        acc
    };

    let expected_pairs = [
        ("field1", "value1"),
        ("field2", "value2"),
        ("field3", "value3"),
        ("field4", "value4"),
        ("field5", "value5"),
    ];
    for (f, v) in expected_pairs {
        assert!(
            found
                .iter()
                .any(|(fb, vb)| fb == f.as_bytes() && vb == v.as_bytes()),
            "expected field {} => {} in hscan results",
            f,
            v
        );
    }

    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn sscan() {
    let mut c = conn().await;
    let key = "scan_stream_test:sscan";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(SAdd::members(
        key,
        ["member1", "member2", "member3", "member4", "member5"],
    ))
    .await
    .unwrap();

    let found = {
        let stream = ScanStream::sscan(&mut c, key, "*");
        tokio::pin!(stream);
        let mut acc: Vec<Bytes> = Vec::new();
        while let Some(item) = stream.next().await {
            acc.push(item.unwrap());
        }
        acc
    };

    for m in ["member1", "member2", "member3", "member4", "member5"] {
        assert!(
            found.iter().any(|b| b == m.as_bytes()),
            "expected member {} in sscan results",
            m
        );
    }

    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn zscan() {
    let mut c = conn().await;
    let key = "scan_stream_test:zscan";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        ZAdd::new(key)
            .member(1.0, "member1")
            .member(2.0, "member2")
            .member(3.0, "member3")
            .member(4.0, "member4")
            .member(5.0, "member5"),
    )
    .await
    .unwrap();

    let found = {
        let stream = ScanStream::zscan(&mut c, key, "*");
        tokio::pin!(stream);
        let mut acc: Vec<(Bytes, f64)> = Vec::new();
        while let Some(item) = stream.next().await {
            acc.push(item.unwrap());
        }
        acc
    };

    let expected_entries = [
        ("member1", 1.0f64),
        ("member2", 2.0),
        ("member3", 3.0),
        ("member4", 4.0),
        ("member5", 5.0),
    ];
    for (member, score) in expected_entries {
        assert!(
            found
                .iter()
                .any(|(mb, s)| mb == member.as_bytes() && (s - score).abs() < f64::EPSILON),
            "expected member {} with score {} in zscan results",
            member,
            score
        );
    }

    c.execute(Del::new(key)).await.unwrap();
}
