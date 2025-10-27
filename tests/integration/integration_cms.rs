//! Integration tests for CMS (Count-Min Sketch) module
//!
//! Tests Redis Count-Min Sketch probabilistic data structure.
//!
//! Run with: cargo test --test integration_cms --features modules
//!
//! Note: Requires Redis Stack with RedisBloom module installed

#[cfg(feature = "modules")]
mod tests {
    use redis_tower::RedisClient;
    use redis_tower::commands::Del;
    use redis_tower::modules::cms::*;

    async fn setup_redis() -> RedisClient {
        RedisClient::connect("localhost:6379")
            .await
            .expect("Failed to connect to Redis")
    }

    #[tokio::test]
    async fn test_cms_initbydim() {
        let client = setup_redis().await;
        let key = "cms_test_dim";

        // Initialize CMS with dimensions
        client.call(CmsInitByDim::new(key, 1000, 5)).await.unwrap();

        // Get info
        let info: CmsInfoResult = client.call(CmsInfo::new(key)).await.unwrap();
        assert_eq!(info.width, 1000);
        assert_eq!(info.depth, 5);

        // Clean up
        client.call(Del::new(vec![key])).await.unwrap();
    }

    #[tokio::test]
    async fn test_cms_initbyprob() {
        let client = setup_redis().await;
        let key = "cms_test_prob";

        // Initialize CMS with error rate and probability
        client
            .call(CmsInitByProb::new(key, 0.01, 0.99))
            .await
            .unwrap();

        // Verify it was created
        let info: CmsInfoResult = client.call(CmsInfo::new(key)).await.unwrap();
        assert!(info.width > 0);
        assert!(info.depth > 0);

        // Clean up
        client.call(Del::new(vec![key])).await.unwrap();
    }

    #[tokio::test]
    async fn test_cms_incrby_query() {
        let client = setup_redis().await;
        let key = "cms_test_incrby";

        // Initialize CMS
        client.call(CmsInitByDim::new(key, 1000, 5)).await.unwrap();

        // Increment items
        client
            .call(CmsIncrBy::new(key, vec![("item1".to_string(), 5)]))
            .await
            .unwrap();

        client
            .call(CmsIncrBy::new(key, vec![("item1".to_string(), 3)]))
            .await
            .unwrap();

        client
            .call(CmsIncrBy::new(key, vec![("item2".to_string(), 10)]))
            .await
            .unwrap();

        // Query counts
        let counts: Vec<i64> = client
            .call(CmsQuery::new(key, vec!["item1", "item2", "item3"]))
            .await
            .unwrap();

        assert_eq!(counts[0], 8); // item1: 5 + 3
        assert_eq!(counts[1], 10); // item2: 10
        assert_eq!(counts[2], 0); // item3: not added

        // Clean up
        client.call(Del::new(vec![key])).await.unwrap();
    }

    #[tokio::test]
    async fn test_cms_merge() {
        let client = setup_redis().await;
        let key1 = "cms_merge_1";
        let key2 = "cms_merge_2";
        let dest = "cms_merge_dest";

        // Initialize two sketches
        client.call(CmsInitByDim::new(key1, 1000, 5)).await.unwrap();
        client.call(CmsInitByDim::new(key2, 1000, 5)).await.unwrap();

        // Add items to each
        client
            .call(CmsIncrBy::new(key1, vec![("item1".to_string(), 5)]))
            .await
            .unwrap();

        client
            .call(CmsIncrBy::new(key2, vec![("item1".to_string(), 3)]))
            .await
            .unwrap();

        // Merge sketches
        client
            .call(CmsMerge::new(dest, vec![key1, key2]))
            .await
            .unwrap();

        // Query merged sketch
        let counts: Vec<i64> = client
            .call(CmsQuery::new(dest, vec!["item1"]))
            .await
            .unwrap();

        assert_eq!(counts[0], 8); // 5 + 3

        // Clean up
        client.call(Del::new(vec![key1, key2, dest])).await.unwrap();
    }

    #[tokio::test]
    async fn test_cms_info() {
        let client = setup_redis().await;
        let key = "cms_test_info";

        // Initialize CMS
        client.call(CmsInitByDim::new(key, 2000, 7)).await.unwrap();

        // Add some items
        client
            .call(CmsIncrBy::new(
                key,
                vec![
                    ("item1".to_string(), 10),
                    ("item2".to_string(), 20),
                    ("item3".to_string(), 30),
                ],
            ))
            .await
            .unwrap();

        // Get info
        let info: CmsInfoResult = client.call(CmsInfo::new(key)).await.unwrap();

        assert_eq!(info.width, 2000);
        assert_eq!(info.depth, 7);
        assert_eq!(info.count, 60); // 10 + 20 + 30

        // Clean up
        client.call(Del::new(vec![key])).await.unwrap();
    }
}
