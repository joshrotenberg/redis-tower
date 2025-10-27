//! Integration tests for RediSearch module
//!
//! Tests Redis full-text search and secondary indexing.
//!
//! Run with: cargo test --test integration_search --features search
//!
//! Note: Requires Redis Stack with RediSearch module installed

#[cfg(feature = "search")]
mod tests {
    use redis_tower::RedisClient;
    use redis_tower::commands::keys::Del;
    use redis_tower::modules::search::*;

    async fn setup_redis() -> RedisClient {
        RedisClient::connect("localhost:6379")
            .await
            .expect("Failed to connect to Redis")
    }

    #[tokio::test]
    async fn test_ft_create_and_list() {
        let client = setup_redis().await;
        let index_name = "search_test_index";

        // Create a simple text index
        client
            .call(
                FtCreate::new(index_name)
                    .schema(vec![SchemaField::text("title"), SchemaField::text("body")]),
            )
            .await
            .unwrap();

        // List all indexes
        let indexes: Vec<String> = client.call(FtList).await.unwrap();
        assert!(indexes.contains(&index_name.to_string()));

        // Cleanup
        client.call(FtDropIndex::new(index_name)).await.unwrap();
    }

    #[tokio::test]
    async fn test_ft_create_and_search() {
        let client = setup_redis().await;
        let index_name = "search_test_docs";

        // Create index
        client
            .call(
                FtCreate::new(index_name)
                    .on_hash()
                    .prefix(vec!["doc:".to_string()])
                    .schema(vec![
                        SchemaField::text("title"),
                        SchemaField::text("content"),
                    ]),
            )
            .await
            .unwrap();

        // Add some documents using HSET
        use redis_tower::commands::hashes::HSet;

        client
            .call(HSet::new("doc:1", "title", b"Rust Programming"))
            .await
            .unwrap();
        client
            .call(HSet::new(
                "doc:1",
                "content",
                b"Learn Rust programming language",
            ))
            .await
            .unwrap();

        client
            .call(HSet::new("doc:2", "title", b"Python Guide"))
            .await
            .unwrap();
        client
            .call(HSet::new(
                "doc:2",
                "content",
                b"Python programming tutorial",
            ))
            .await
            .unwrap();

        // Wait a moment for indexing
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Search for "Rust"
        let results: Vec<SearchDocument> = client
            .call(FtSearch::new(index_name, "Rust"))
            .await
            .unwrap();

        assert!(results.len() >= 1);

        // Cleanup
        client
            .call(Del::new(vec!["doc:1".to_string(), "doc:2".to_string()]))
            .await
            .unwrap();
        client.call(FtDropIndex::new(index_name)).await.unwrap();
    }

    #[tokio::test]
    async fn test_ft_info() {
        let client = setup_redis().await;
        let index_name = "search_test_info";

        // Create index
        client
            .call(
                FtCreate::new(index_name)
                    .schema(vec![SchemaField::text("name"), SchemaField::numeric("age")]),
            )
            .await
            .unwrap();

        // Get index info
        let info: IndexInfo = client.call(FtInfo::new(index_name)).await.unwrap();

        // Verify index name
        assert_eq!(info.index_name, index_name);

        // Verify we have fields
        assert!(info.attributes.len() >= 2);

        // Cleanup
        client.call(FtDropIndex::new(index_name)).await.unwrap();
    }
}
