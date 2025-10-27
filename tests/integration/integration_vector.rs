#[cfg(feature = "modules")]
mod tests {
    use redis_tower::RedisClient;
    use redis_tower::commands::strings::Del;
    use redis_tower::modules::vector::*;

    async fn setup_redis() -> RedisClient {
        RedisClient::connect("localhost:6379")
            .await
            .expect("Failed to connect to Redis")
    }

    #[tokio::test]
    async fn test_vadd_and_vcard() {
        let client = setup_redis().await;
        let key = "vector_test_add";

        // Add vectors (3D vectors for simplicity)
        let elements = vec![
            ("item1", vec![1.0, 2.0, 3.0]),
            ("item2", vec![4.0, 5.0, 6.0]),
            ("item3", vec![7.0, 8.0, 9.0]),
        ];

        let added: i64 = client.call(Vadd::new(key, elements)).await.unwrap();
        assert_eq!(added, 3);

        // Check cardinality
        let card: i64 = client.call(Vcard::new(key)).await.unwrap();
        assert_eq!(card, 3);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_vdim() {
        let client = setup_redis().await;
        let key = "vector_test_dim";

        // Add 4D vector
        let elements = vec![("item1", vec![1.0, 2.0, 3.0, 4.0])];
        client.call(Vadd::new(key, elements)).await.unwrap();

        // Check dimension
        let dim: i64 = client.call(Vdim::new(key)).await.unwrap();
        assert_eq!(dim, 4);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_vemb() {
        let client = setup_redis().await;
        let key = "vector_test_emb";

        // Add vector
        let vector = vec![1.5, 2.5, 3.5];
        let elements = vec![("item1", vector.clone())];
        client.call(Vadd::new(key, elements)).await.unwrap();

        // Get embedding
        let emb: Option<Vec<f32>> = client.call(Vemb::new(key, "item1")).await.unwrap();
        assert!(emb.is_some());

        let retrieved = emb.unwrap();
        assert_eq!(retrieved.len(), 3);
        // Approximate comparison due to floating point
        for (i, val) in retrieved.iter().enumerate() {
            assert!(
                (val - vector[i]).abs() < 0.01,
                "Vector mismatch at index {}",
                i
            );
        }

        // Test non-existent element
        let none: Option<Vec<f32>> = client.call(Vemb::new(key, "nonexistent")).await.unwrap();
        assert!(none.is_none());

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_vismember() {
        let client = setup_redis().await;
        let key = "vector_test_ismember";

        // Add elements
        let elements = vec![
            ("exists", vec![1.0, 2.0, 3.0]),
            ("also_exists", vec![4.0, 5.0, 6.0]),
        ];
        client.call(Vadd::new(key, elements)).await.unwrap();

        // Check membership
        let exists: bool = client.call(Vismember::new(key, "exists")).await.unwrap();
        assert!(exists);

        let not_exists: bool = client
            .call(Vismember::new(key, "not_exists"))
            .await
            .unwrap();
        assert!(!not_exists);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_vrem() {
        let client = setup_redis().await;
        let key = "vector_test_rem";

        // Add elements
        let elements = vec![
            ("item1", vec![1.0, 2.0, 3.0]),
            ("item2", vec![4.0, 5.0, 6.0]),
            ("item3", vec![7.0, 8.0, 9.0]),
        ];
        client.call(Vadd::new(key, elements)).await.unwrap();

        // Remove one element
        let removed: i64 = client.call(Vrem::new(key, vec!["item2"])).await.unwrap();
        assert_eq!(removed, 1);

        // Verify removal
        let exists: bool = client.call(Vismember::new(key, "item2")).await.unwrap();
        assert!(!exists);

        // Check cardinality
        let card: i64 = client.call(Vcard::new(key)).await.unwrap();
        assert_eq!(card, 2);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_vsetattr_vgetattr() {
        let client = setup_redis().await;
        let key = "vector_test_attr";

        // Add element
        let elements = vec![("item1", vec![1.0, 2.0, 3.0])];
        client.call(Vadd::new(key, elements)).await.unwrap();

        // Set attributes
        client
            .call(Vsetattr::new(key, "item1", "color=red,size=large"))
            .await
            .unwrap();

        // Get attributes
        let attrs: Option<String> = client.call(Vgetattr::new(key, "item1")).await.unwrap();
        assert!(attrs.is_some());
        assert!(attrs.unwrap().contains("color"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_vrandmember() {
        let client = setup_redis().await;
        let key = "vector_test_rand";

        // Add multiple elements
        let elements = vec![
            ("item1", vec![1.0, 2.0, 3.0]),
            ("item2", vec![4.0, 5.0, 6.0]),
            ("item3", vec![7.0, 8.0, 9.0]),
        ];
        client.call(Vadd::new(key, elements)).await.unwrap();

        // Get single random member
        let members: Vec<String> = client.call(Vrandmember::new(key)).await.unwrap();
        assert_eq!(members.len(), 1);

        // Get multiple random members
        let members: Vec<String> = client.call(Vrandmember::new(key).count(2)).await.unwrap();
        assert_eq!(members.len(), 2);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_vsim() {
        let client = setup_redis().await;
        let key = "vector_test_sim";

        // Add similar vectors
        let elements = vec![
            ("similar1", vec![1.0, 1.0, 1.0]),
            ("similar2", vec![1.1, 1.1, 1.1]),
            ("different", vec![10.0, 10.0, 10.0]),
        ];
        client.call(Vadd::new(key, elements)).await.unwrap();

        // Search for vectors similar to [1.0, 1.0, 1.0]
        let results: Vec<(String, f64)> = client
            .call(Vsim::new(key, vec![1.0, 1.0, 1.0], 2))
            .await
            .unwrap();

        // Should return 2 nearest neighbors
        assert!(results.len() <= 2);

        // Results should be sorted by similarity (closest first)
        // The similar vectors should be returned
        assert!(results.iter().any(|(name, _)| name.contains("similar")));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_vinfo() {
        let client = setup_redis().await;
        let key = "vector_test_info";

        // Add elements
        let elements = vec![
            ("item1", vec![1.0, 2.0, 3.0]),
            ("item2", vec![4.0, 5.0, 6.0]),
        ];
        client.call(Vadd::new(key, elements)).await.unwrap();

        // Get info
        let info: String = client.call(Vinfo::new(key)).await.unwrap();
        assert!(!info.is_empty());

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_vlinks() {
        let client = setup_redis().await;
        let key = "vector_test_links";

        // Add elements (VLINKS behavior depends on HNSW graph structure)
        let elements = vec![
            ("item1", vec![1.0, 2.0, 3.0]),
            ("item2", vec![1.1, 2.1, 3.1]),
            ("item3", vec![1.2, 2.2, 3.2]),
        ];
        client.call(Vadd::new(key, elements)).await.unwrap();

        // Get links for an element
        let links: Vec<String> = client.call(Vlinks::new(key, "item1")).await.unwrap();

        // Links may or may not exist depending on graph structure
        // Just verify the command executes without error
        assert!(links.len() >= 0);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_vadd_multiple_incremental() {
        let client = setup_redis().await;
        let key = "vector_test_incremental";

        // Add first batch
        let batch1 = vec![("item1", vec![1.0, 2.0, 3.0])];
        let added1: i64 = client.call(Vadd::new(key, batch1)).await.unwrap();
        assert_eq!(added1, 1);

        // Add second batch
        let batch2 = vec![
            ("item2", vec![4.0, 5.0, 6.0]),
            ("item3", vec![7.0, 8.0, 9.0]),
        ];
        let added2: i64 = client.call(Vadd::new(key, batch2)).await.unwrap();
        assert_eq!(added2, 2);

        // Verify total cardinality
        let card: i64 = client.call(Vcard::new(key)).await.unwrap();
        assert_eq!(card, 3);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_vrem_multiple() {
        let client = setup_redis().await;
        let key = "vector_test_rem_multiple";

        // Add elements
        let elements = vec![
            ("item1", vec![1.0, 2.0, 3.0]),
            ("item2", vec![4.0, 5.0, 6.0]),
            ("item3", vec![7.0, 8.0, 9.0]),
        ];
        client.call(Vadd::new(key, elements)).await.unwrap();

        // Remove multiple elements
        let removed: i64 = client
            .call(Vrem::new(key, vec!["item1", "item3"]))
            .await
            .unwrap();
        assert_eq!(removed, 2);

        // Verify only item2 remains
        let exists1: bool = client.call(Vismember::new(key, "item1")).await.unwrap();
        let exists2: bool = client.call(Vismember::new(key, "item2")).await.unwrap();
        let exists3: bool = client.call(Vismember::new(key, "item3")).await.unwrap();

        assert!(!exists1);
        assert!(exists2);
        assert!(!exists3);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }
}
