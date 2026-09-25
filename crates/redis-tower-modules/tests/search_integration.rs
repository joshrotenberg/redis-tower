//! Live-server integration tests for [`SearchClient`].
//!
//! These exercise RediSearch (`FT.*`) commands against a real server, so they
//! require a module-enabled build. They are `#[ignore]`d by default and run by
//! the path-filtered module gate plus the broader nightly matrix:
//!
//! ```sh
//! cargo test -p redis-tower-modules --test search_integration --features search -- --ignored
//! ```
//!
//! The server defaults to `redis://127.0.0.1:6399` (the standard workspace test
//! port) and can be overridden with the `REDIS_URL` environment variable.

#![cfg(feature = "search")]

use bytes::Bytes;
use redis_tower::commands::FtSugAdd;
use redis_tower_core::RedisConnection;
use redis_tower_modules::search::{IndexBuilder, SearchClient, SearchQuery, SugGetOptions};
use serde::Deserialize;

async fn connect() -> RedisConnection {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6399".into());
    RedisConnection::connect_url(&url)
        .await
        .expect("failed to connect to Redis")
}

/// A process-unique suffix, derived from the current time.
fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos()
}

#[derive(Debug, Deserialize, PartialEq)]
struct Product {
    name: String,
}

#[tokio::test]
#[ignore = "requires a live Redis Stack server with RediSearch"]
async fn search_create_index_and_query() {
    let mut conn = connect().await;
    let suffix = unique_suffix();
    let index = format!("test:idx:{suffix}");
    let prefix = format!("test:product:{suffix}:");
    let key1 = format!("{prefix}1");
    let key2 = format!("{prefix}2");

    // Seed two HASH documents the index will pick up.
    use redis_tower::commands::HSet;
    conn.execute(HSet::new(&key1, "name", "widget"))
        .await
        .unwrap();
    conn.execute(HSet::new(&key2, "name", "gadget"))
        .await
        .unwrap();

    {
        let mut search = SearchClient::new(&mut conn);

        // FT.CREATE: a hash index over the unique prefix with a text field.
        search
            .create_index(
                IndexBuilder::new(&index)
                    .on_hash()
                    .prefix(&prefix)
                    .text_field("name"),
            )
            .await
            .unwrap();

        // The index should show up in FT._LIST.
        let indexes = search.list_indexes().await.unwrap();
        assert!(indexes.iter().any(|i| i == &index));

        // FT.SEARCH for a specific term returns the matching document.
        let results = search
            .search::<Product>(SearchQuery::new(&index, "widget"))
            .await
            .unwrap();
        assert_eq!(results.total, 1);
        assert_eq!(results.docs.len(), 1);
        assert_eq!(results.docs[0].doc.name, "widget");

        // A wildcard query should find both indexed documents.
        let all = search
            .search::<Product>(SearchQuery::new(&index, "*"))
            .await
            .unwrap();
        assert_eq!(all.total, 2);

        // FT.INFO reports the indexed document count.
        let info = search.index_info(&index).await.unwrap();
        assert_eq!(info.num_docs, 2);

        // FT.DROPINDEX with DD deletes the index and its documents.
        search.drop_index(&index, true).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires a live Redis Stack server with RediSearch"]
async fn autocomplete_binary_dictionary_suggestion_and_payload_roundtrip() {
    let mut conn = connect().await;
    let mut dictionary = format!("test:suggest:binary:{}:", unique_suffix()).into_bytes();
    dictionary.extend_from_slice(b"\0\xff");
    // Redis Search treats NUL as a suggestion-string terminator. The low-level
    // serialization test covers NUL; use the remaining binary edge cases for
    // the server roundtrip itself.
    let suggestion = b"pre\xff\r\nfix";
    let payload = b"$5\r\nvalue\xff";

    conn.execute(FtSugAdd::new(&dictionary, suggestion, 1.0).payload(payload))
        .await
        .unwrap();
    {
        let mut search = SearchClient::new(&mut conn);
        let results = search
            .sug_get(
                &dictionary,
                b"pre".as_slice(),
                SugGetOptions {
                    withpayloads: true,
                    ..SugGetOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].string, Bytes::copy_from_slice(suggestion));
        assert_eq!(results[0].payload, Some(Bytes::copy_from_slice(payload)));
    }

    use redis_tower::commands::Del;
    conn.execute(Del::new(dictionary)).await.unwrap();
}
