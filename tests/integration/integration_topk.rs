#[cfg(feature = "modules")]
mod tests {
    use bytes::Bytes;
    use redis_tower::RedisClient;
    use redis_tower::commands::strings::Del;
    use redis_tower::modules::topk::*;

    async fn setup_redis() -> RedisClient {
        RedisClient::connect("localhost:6379")
            .await
            .expect("Failed to connect to Redis")
    }

    #[tokio::test]
    async fn test_topk_reserve() {
        let client = setup_redis().await;
        let key = "topk_test_reserve";

        // Create top-10 filter
        client.call(TopKReserve::new(key, 10)).await.unwrap();

        // Verify via INFO
        let info: TopKInfoResult = client.call(TopKInfo::new(key)).await.unwrap();
        assert_eq!(info.k, 10);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_topk_reserve_with_params() {
        let client = setup_redis().await;
        let key = "topk_test_reserve_params";

        // Create with custom parameters
        client
            .call(TopKReserve::new(key, 5).width(1000).depth(5).decay(0.95))
            .await
            .unwrap();

        // Verify via INFO
        let info: TopKInfoResult = client.call(TopKInfo::new(key)).await.unwrap();
        assert_eq!(info.k, 5);
        assert_eq!(info.width, 1000);
        assert_eq!(info.depth, 5);
        assert!((info.decay - 0.95).abs() < 0.001, "Decay mismatch");

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_topk_add_and_list() {
        let client = setup_redis().await;
        let key = "topk_test_add";

        // Create top-3 filter
        client.call(TopKReserve::new(key, 3)).await.unwrap();

        // Add items
        let result: Vec<Option<Bytes>> = client
            .call(TopKAdd::new(
                key,
                vec![
                    Bytes::from("item1"),
                    Bytes::from("item2"),
                    Bytes::from("item3"),
                ],
            ))
            .await
            .unwrap();

        // No evictions initially
        assert_eq!(result.len(), 3);

        // Get list
        let list: TopKListResult = client.call(TopKList::new(key)).await.unwrap();
        assert_eq!(list.items.len(), 3);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_topk_incrby() {
        let client = setup_redis().await;
        let key = "topk_test_incrby";

        // Create top-5 filter
        client.call(TopKReserve::new(key, 5)).await.unwrap();

        // Increment items
        client
            .call(
                TopKIncrBy::new(key)
                    .item("item1", 10)
                    .item("item2", 5)
                    .item("item3", 15),
            )
            .await
            .unwrap();

        // Query counts
        let counts: Vec<i64> = client
            .call(TopKCount::new(
                key,
                vec![
                    Bytes::from("item1"),
                    Bytes::from("item2"),
                    Bytes::from("item3"),
                ],
            ))
            .await
            .unwrap();

        assert_eq!(counts[0], 10);
        assert_eq!(counts[1], 5);
        assert_eq!(counts[2], 15);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_topk_query() {
        let client = setup_redis().await;
        let key = "topk_test_query";

        // Create and add items
        client.call(TopKReserve::new(key, 3)).await.unwrap();
        client
            .call(TopKAdd::new(
                key,
                vec![
                    Bytes::from("item1"),
                    Bytes::from("item2"),
                    Bytes::from("item3"),
                ],
            ))
            .await
            .unwrap();

        // Query which items are in top-K
        let results: Vec<bool> = client
            .call(TopKQuery::new(
                key,
                vec![
                    Bytes::from("item1"),
                    Bytes::from("item2"),
                    Bytes::from("nonexistent"),
                ],
            ))
            .await
            .unwrap();

        assert!(results[0]); // item1 is in top-K
        assert!(results[1]); // item2 is in top-K
        assert!(!results[2]); // nonexistent is not in top-K

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_topk_count() {
        let client = setup_redis().await;
        let key = "topk_test_count";

        // Create and increment
        client.call(TopKReserve::new(key, 5)).await.unwrap();
        client
            .call(TopKIncrBy::new(key).item("popular", 100).item("rare", 1))
            .await
            .unwrap();

        // Get counts
        let counts: Vec<i64> = client
            .call(TopKCount::new(
                key,
                vec![Bytes::from("popular"), Bytes::from("rare")],
            ))
            .await
            .unwrap();

        assert_eq!(counts[0], 100);
        assert_eq!(counts[1], 1);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_topk_list_with_count() {
        let client = setup_redis().await;
        let key = "topk_test_list_count";

        // Create and increment items
        client.call(TopKReserve::new(key, 3)).await.unwrap();
        client
            .call(
                TopKIncrBy::new(key)
                    .item("first", 100)
                    .item("second", 50)
                    .item("third", 25),
            )
            .await
            .unwrap();

        // Get list with counts
        let list: TopKListResult = client.call(TopKList::new(key).with_count()).await.unwrap();

        assert_eq!(list.items.len(), 3);

        // Items should be in top-K with their counts
        let has_first = list
            .items
            .iter()
            .any(|(item, count)| item == &Bytes::from("first") && *count == 100);
        let has_second = list
            .items
            .iter()
            .any(|(item, count)| item == &Bytes::from("second") && *count == 50);
        let has_third = list
            .items
            .iter()
            .any(|(item, count)| item == &Bytes::from("third") && *count == 25);

        assert!(has_first, "first item not found with correct count");
        assert!(has_second, "second item not found with correct count");
        assert!(has_third, "third item not found with correct count");

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_topk_eviction() {
        let client = setup_redis().await;
        let key = "topk_test_eviction";

        // Create small top-2 filter
        client.call(TopKReserve::new(key, 2)).await.unwrap();

        // Add items with different frequencies
        for _ in 0..10 {
            client
                .call(TopKIncrBy::new(key).item("popular1", 1))
                .await
                .unwrap();
        }
        for _ in 0..8 {
            client
                .call(TopKIncrBy::new(key).item("popular2", 1))
                .await
                .unwrap();
        }
        for _ in 0..2 {
            client
                .call(TopKIncrBy::new(key).item("rare", 1))
                .await
                .unwrap();
        }

        // List should contain top 2 items
        let list: TopKListResult = client.call(TopKList::new(key)).await.unwrap();
        assert_eq!(list.items.len(), 2);

        // popular1 and popular2 should be in top-K, rare should not
        let items: Vec<_> = list.items.iter().map(|(item, _)| item).collect();
        assert!(
            items.contains(&&Bytes::from("popular1")),
            "popular1 should be in top-K"
        );
        assert!(
            items.contains(&&Bytes::from("popular2")),
            "popular2 should be in top-K"
        );

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_topk_info() {
        let client = setup_redis().await;
        let key = "topk_test_info";

        // Create with known parameters
        client
            .call(TopKReserve::new(key, 10).width(500).depth(3).decay(0.9))
            .await
            .unwrap();

        // Get info
        let info: TopKInfoResult = client.call(TopKInfo::new(key)).await.unwrap();

        // Verify all fields
        assert_eq!(info.k, 10);
        assert_eq!(info.width, 500);
        assert_eq!(info.depth, 3);
        assert!((info.decay - 0.9).abs() < 0.001);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_topk_single_item_methods() {
        let client = setup_redis().await;
        let key = "topk_test_single";

        // Create
        client.call(TopKReserve::new(key, 5)).await.unwrap();

        // Add single item using single() method
        let result: Vec<Option<Bytes>> = client
            .call(TopKAdd::single(key, Bytes::from("single_item")))
            .await
            .unwrap();
        assert_eq!(result.len(), 1);

        // Query single item using single() method
        let query: Vec<bool> = client
            .call(TopKQuery::single(key, Bytes::from("single_item")))
            .await
            .unwrap();
        assert_eq!(query.len(), 1);
        assert!(query[0]);

        // Count single item using single() method
        let count: Vec<i64> = client
            .call(TopKCount::single(key, Bytes::from("single_item")))
            .await
            .unwrap();
        assert_eq!(count.len(), 1);
        assert_eq!(count[0], 1);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }
}
