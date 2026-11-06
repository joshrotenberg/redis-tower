//! Live-server integration tests for [`JsonClient`].
//!
//! These exercise RedisJSON (`JSON.*`) commands against a real server, so they
//! require a module-enabled build. They are `#[ignore]`d by default and run by
//! the path-filtered module gate plus the broader nightly matrix:
//!
//! ```sh
//! cargo test -p redis-tower-modules --test json_integration --features json -- --ignored
//! ```
//!
//! The server defaults to `redis://127.0.0.1:6399` (the standard workspace test
//! port) and can be overridden with the `REDIS_URL` environment variable.

#![cfg(feature = "json")]

use bytes::Bytes;
use redis_tower::commands::{JsonNumIncrBy, JsonSet, SetStatus};
use redis_tower_core::{ConnectionConfig, ProtocolVersion, RedisConnection};
use redis_tower_modules::json::JsonClient;
use serde::{Deserialize, Serialize};

async fn connect() -> RedisConnection {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6399".into());
    RedisConnection::connect_url(&url)
        .await
        .expect("failed to connect to Redis")
}

/// A process-unique key suffix, derived from the current time.
fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos()
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct User {
    name: String,
    age: u32,
}

#[tokio::test]
#[ignore = "requires a live Redis Stack server with RedisJSON"]
async fn json_set_get_roundtrip() {
    let mut conn = connect().await;
    let key = format!("test:json:{}", unique_suffix());

    {
        let mut json = JsonClient::new(&mut conn);

        let user = User {
            name: "Ada".into(),
            age: 36,
        };

        // JSON.SET at the document root.
        json.set(key.clone(), "$", &user).await.unwrap();

        // JSON.GET round-trips the value back through serde.
        let fetched: Option<User> = json.get(key.clone(), "$").await.unwrap();
        assert_eq!(fetched, Some(user));

        // The path exists.
        assert!(json.path_exists(key.clone(), "$.name").await.unwrap());

        // JSON.DEL removes the value.
        let deleted = json.del(key.clone(), "$").await.unwrap();
        assert_eq!(deleted, 1);

        // After deletion the key is gone.
        let missing: Option<User> = json.get(key.clone(), "$").await.unwrap();
        assert_eq!(missing, None);
    }

    // Clean up (no-op if already deleted).
    use redis_tower::commands::Del;
    conn.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a live Redis Stack server with RedisJSON"]
async fn json_merge_and_array_ops() {
    let mut conn = connect().await;
    let key = format!("test:json:{}", unique_suffix());

    {
        let mut json = JsonClient::new(&mut conn);

        // Seed a document with a nested array.
        let doc = serde_json::json!({ "name": "list", "items": [1, 2, 3] });
        json.set(key.clone(), "$", &doc).await.unwrap();

        // JSON.ARRLEN reports the current length.
        assert_eq!(json.arr_len(key.clone(), "$.items").await.unwrap(), Some(3));

        // JSON.ARRAPPEND extends the array and returns the new length.
        let lengths = json
            .arr_append(key.clone(), "$.items", &[4i32, 5])
            .await
            .unwrap();
        assert_eq!(lengths, vec![Some(5)]);

        // JSON.MERGE patches an existing field.
        let patch = serde_json::json!({ "name": "patched" });
        json.merge(key.clone(), "$", &patch).await.unwrap();

        // JSON.OBJKEYS lists the top-level keys.
        let mut keys = json.obj_keys(key.clone(), "$").await.unwrap();
        keys.sort();
        assert_eq!(keys, vec!["items".to_string(), "name".to_string()]);
    }

    use redis_tower::commands::Del;
    conn.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a live Redis Stack server with RedisJSON"]
async fn json_binary_key_roundtrip() {
    let mut conn = connect().await;
    let mut key = format!("test:json:binary:{}:", unique_suffix()).into_bytes();
    key.extend_from_slice(b"\0\xff\r\n");

    {
        let mut json = JsonClient::new(&mut conn);
        let value = serde_json::json!({ "binary": true });
        json.set(&key, "$", &value).await.unwrap();
        assert_eq!(
            json.get::<serde_json::Value>(&key, "$").await.unwrap(),
            Some(value)
        );
    }

    use redis_tower::commands::Del;
    conn.execute(Del::new(key)).await.unwrap();
}

/// `JSON.NUMINCRBY` text and `JSON.SET` outcomes must not depend on the
/// negotiated protocol: RESP2 replies with JSON text and `$-1`, RESP3 with
/// typed values and `_`.
#[tokio::test]
#[ignore = "requires a live Redis Stack server with RedisJSON"]
async fn json_replies_match_across_protocols() {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6399".into());
    let mut results = Vec::new();
    for protocol in [ProtocolVersion::Resp2, ProtocolVersion::Resp3] {
        let mut conn = RedisConnection::connect_url_with_config(
            &url,
            &ConnectionConfig::new().with_protocol(protocol),
        )
        .await
        .expect("failed to connect to Redis");
        let key = format!("test:json:parity:{}", unique_suffix());
        conn.execute(JsonSet::new(
            key.as_str(),
            "$",
            r#"{"a":[1,2],"f":{"small":0.00001,"big":1e20,"whole":3.0}}"#,
        ))
        .await
        .unwrap();

        let mut observed: Vec<Bytes> = Vec::new();
        for (path, by) in [
            (".a[*]", 1.0),
            ("$.a[*]", 1.0),
            ("$.f.small", 0.0),
            ("$.f.big", 0.0),
            ("$.f.whole", 0.0),
            (".f.small", 0.0),
        ] {
            observed.push(
                conn.execute(JsonNumIncrBy::new(key.as_str(), path, by))
                    .await
                    .unwrap(),
            );
        }
        let missing_parent = conn
            .execute(JsonSet::new(key.as_str(), "$.missing.child", "1").with_outcome())
            .await
            .unwrap();
        let applied = conn
            .execute(JsonSet::new(key.as_str(), "$.present", "1").with_outcome())
            .await
            .unwrap();
        conn.execute(redis_tower::commands::Del::new(key.as_str()))
            .await
            .unwrap();
        results.push((observed, missing_parent, applied));
    }

    assert_eq!(results[0], results[1], "RESP2 and RESP3 differ");
    let (observed, missing_parent, applied) = &results[1];
    let texts: Vec<&[u8]> = observed.iter().map(|b| b.as_ref()).collect();
    assert_eq!(
        texts,
        [
            &b"3"[..],
            b"[3,4]",
            b"[0.00001]",
            b"[1e20]",
            b"[3.0]",
            b"0.00001",
        ]
    );
    assert_eq!(*missing_parent, SetStatus::NotApplied);
    assert_eq!(*applied, SetStatus::Applied);
}
