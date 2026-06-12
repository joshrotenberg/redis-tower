//! Live-server integration tests for [`VectorSetClient`].
//!
//! Vector Sets require Redis 8.0 or later, so these tests are `#[ignore]`d by
//! default (CI also exercises Redis 7.4.3, which lacks the commands). Run them
//! against a Redis 8.0+ server with:
//!
//! ```sh
//! cargo test -p redis-tower-modules --test vector_integration --features vector -- --ignored
//! ```
//!
//! The server defaults to `redis://127.0.0.1:6399` (the standard workspace test
//! port) and can be overridden with the `REDIS_URL` environment variable.

#![cfg(feature = "vector")]

use redis_tower_core::RedisConnection;
use redis_tower_modules::vector::{VectorQuery, VectorSetClient};

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

#[tokio::test]
#[ignore = "requires a live Redis 8.0+ server with Vector Sets"]
async fn vector_set_basic_lifecycle() {
    let mut conn = connect().await;
    let key = format!("test:vset:{}", unique_suffix());

    {
        let mut vset = VectorSetClient::new(&mut conn, key.clone());

        // Add three simple 3-dimensional vectors.
        assert!(vset.add(vec![1.0, 0.0, 0.0], "a").await.unwrap());
        assert!(vset.add(vec![0.0, 1.0, 0.0], "b").await.unwrap());
        assert!(vset.add(vec![0.0, 0.0, 1.0], "c").await.unwrap());

        // Cardinality should be 3.
        assert_eq!(vset.cardinality().await.unwrap(), 3);

        // A similarity search should return results.
        let results = vset
            .search(
                VectorQuery::by_vector(vec![1.0, 0.0, 0.0])
                    .count(3)
                    .withscores(),
            )
            .await
            .unwrap();
        assert!(!results.is_empty());
        // The closest element to (1,0,0) should be "a".
        assert_eq!(results[0].element, "a");

        // Remove one element.
        assert!(vset.remove("a").await.unwrap());

        // Cardinality should now be 2.
        assert_eq!(vset.cardinality().await.unwrap(), 2);
    }

    // Clean up.
    use redis_tower::commands::Del;
    conn.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a live Redis 8.0+ server with Vector Sets"]
async fn vector_set_attr_then_del_attr() {
    let mut conn = connect().await;
    let key = format!("test:vset:attr:{}", unique_suffix());

    {
        let mut vset = VectorSetClient::new(&mut conn, key.clone());
        assert!(vset.add(vec![1.0, 0.0, 0.0], "a").await.unwrap());

        // Set, then clear, the attribute. del_attr must send `VSETATTR "" `
        // (there is no VDELATTR) and round-trip cleanly against a real server.
        assert!(vset.set_attr("a", "{\"color\":\"red\"}").await.unwrap());
        assert_eq!(
            vset.get_attr("a").await.unwrap().as_deref(),
            Some("{\"color\":\"red\"}")
        );

        assert!(vset.del_attr("a").await.unwrap());
        // The attribute is now the empty string (cleared), not the old value.
        let cleared = vset.get_attr("a").await.unwrap();
        assert!(
            cleared.is_none() || cleared.as_deref() == Some(""),
            "attribute should be cleared, got {cleared:?}"
        );
    }

    use redis_tower::commands::Del;
    conn.execute(Del::new(key)).await.unwrap();
}
