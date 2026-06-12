//! Live-server integration tests for [`JsonClient`].
//!
//! These exercise RedisJSON (`JSON.*`) commands against a real server, so they
//! require a Redis Stack build. CI does not run them -- its Redis is built
//! without modules -- so they are `#[ignore]`d by default and only run when
//! explicitly requested:
//!
//! ```sh
//! cargo test -p redis-tower-modules --test json_integration --features json -- --ignored
//! ```
//!
//! The server defaults to `redis://127.0.0.1:6399` (the standard workspace test
//! port) and can be overridden with the `REDIS_URL` environment variable.

#![cfg(feature = "json")]

use redis_tower_core::RedisConnection;
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
