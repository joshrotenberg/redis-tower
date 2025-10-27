//! Integration tests for Cuckoo Filter module
//!
//! Tests Redis Cuckoo Filter probabilistic data structure.
//!
//! Run with: cargo test --test integration_cuckoo --features modules
//!
//! Note: Requires Redis Stack with RedisBloom module installed

#[cfg(feature = "modules")]
mod tests {
    use bytes::Bytes;
    use redis_tower::RedisClient;
    use redis_tower::commands::keys::Del;
    use redis_tower::modules::cuckoo::*;

    async fn setup_redis() -> RedisClient {
        RedisClient::connect("localhost:6379")
            .await
            .expect("Failed to connect to Redis")
    }

    #[tokio::test]
    async fn test_cf_reserve() {
        let client = setup_redis().await;
        let key = "cf_test_reserve";

        // Reserve a cuckoo filter
        client.call(CfReserve::new(key, 1000)).await.unwrap();

        // Verify it was created by getting info
        let info: CfInfoResult = client.call(CfInfo::new(key)).await.unwrap();
        assert!(info.num_buckets > 0);

        // Clean up
        client.call(Del::new(vec![key])).await.unwrap();
    }

    #[tokio::test]
    async fn test_cf_add_exists() {
        let client = setup_redis().await;
        let key = "cf_test_add";

        // Reserve filter
        client.call(CfReserve::new(key, 1000)).await.unwrap();

        // Add items
        let added: bool = client.call(CfAdd::new(key, "item1")).await.unwrap();
        assert!(added);

        let added: bool = client.call(CfAdd::new(key, "item2")).await.unwrap();
        assert!(added);

        // Check existence
        let exists: bool = client.call(CfExists::new(key, "item1")).await.unwrap();
        assert!(exists);

        let exists: bool = client.call(CfExists::new(key, "item2")).await.unwrap();
        assert!(exists);

        let exists: bool = client.call(CfExists::new(key, "item3")).await.unwrap();
        assert!(!exists);

        // Clean up
        client.call(Del::new(vec![key])).await.unwrap();
    }

    #[tokio::test]
    async fn test_cf_addnx() {
        let client = setup_redis().await;
        let key = "cf_test_addnx";

        // Reserve filter
        client.call(CfReserve::new(key, 1000)).await.unwrap();

        // Add item with ADDNX (only if not exists)
        let added: bool = client.call(CfAddNx::new(key, "item1")).await.unwrap();
        assert!(added);

        // Try to add same item again
        let added: bool = client.call(CfAddNx::new(key, "item1")).await.unwrap();
        assert!(!added); // Should not add because it exists

        // Clean up
        client.call(Del::new(vec![key])).await.unwrap();
    }

    #[tokio::test]
    async fn test_cf_insert() {
        let client = setup_redis().await;
        let key = "cf_test_insert";

        // Insert creates filter if it doesn't exist
        let results: Vec<bool> = client
            .call(CfInsert::new(key, vec!["item1", "item2", "item3"]).capacity(1000))
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|&r| r)); // All should be added

        // Verify items exist
        let exists: bool = client.call(CfExists::new(key, "item1")).await.unwrap();
        assert!(exists);

        // Clean up
        client.call(Del::new(vec![key])).await.unwrap();
    }

    #[tokio::test]
    async fn test_cf_insertnx() {
        let client = setup_redis().await;
        let key = "cf_test_insertnx";

        // Insert items with INSERTNX
        let results: Vec<bool> = client
            .call(CfInsertNx::new(key, vec!["item1", "item2"]).capacity(1000))
            .await
            .unwrap();

        assert!(results[0]); // item1 added
        assert!(results[1]); // item2 added

        // Try to insert again (should fail for existing items)
        let results: Vec<bool> = client
            .call(CfInsertNx::new(key, vec!["item1", "item3"]))
            .await
            .unwrap();

        assert!(!results[0]); // item1 already exists
        assert!(results[1]); // item3 is new

        // Clean up
        client.call(Del::new(vec![key])).await.unwrap();
    }

    #[tokio::test]
    async fn test_cf_del() {
        let client = setup_redis().await;
        let key = "cf_test_del";

        // Reserve and add items
        client.call(CfReserve::new(key, 1000)).await.unwrap();
        client.call(CfAdd::new(key, "item1")).await.unwrap();

        // Verify item exists
        let exists: bool = client.call(CfExists::new(key, "item1")).await.unwrap();
        assert!(exists);

        // Delete item
        let deleted: bool = client.call(CfDel::new(key, "item1")).await.unwrap();
        assert!(deleted);

        // Verify item no longer exists
        let exists: bool = client.call(CfExists::new(key, "item1")).await.unwrap();
        assert!(!exists);

        // Clean up
        client.call(Del::new(vec![key])).await.unwrap();
    }

    #[tokio::test]
    async fn test_cf_count() {
        let client = setup_redis().await;
        let key = "cf_test_count";

        // Reserve and add same item multiple times
        client.call(CfReserve::new(key, 1000)).await.unwrap();
        client.call(CfAdd::new(key, "item1")).await.unwrap();
        client.call(CfAdd::new(key, "item1")).await.unwrap();
        client.call(CfAdd::new(key, "item1")).await.unwrap();

        // Count occurrences
        let count: i64 = client.call(CfCount::new(key, "item1")).await.unwrap();
        assert_eq!(count, 3);

        // Count non-existent item
        let count: i64 = client.call(CfCount::new(key, "item2")).await.unwrap();
        assert_eq!(count, 0);

        // Clean up
        client.call(Del::new(vec![key])).await.unwrap();
    }

    #[tokio::test]
    async fn test_cf_info() {
        let client = setup_redis().await;
        let key = "cf_test_info";

        // Reserve filter
        client.call(CfReserve::new(key, 1000)).await.unwrap();

        // Add some items
        client.call(CfAdd::new(key, "item1")).await.unwrap();
        client.call(CfAdd::new(key, "item2")).await.unwrap();

        // Get info
        let info: CfInfoResult = client.call(CfInfo::new(key)).await.unwrap();

        assert!(info.num_buckets > 0);
        assert!(info.num_items >= 2);

        // Clean up
        client.call(Del::new(vec![key])).await.unwrap();
    }
}
