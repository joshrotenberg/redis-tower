#[cfg(feature = "modules")]
mod tests {
    use redis_tower::RedisClient;
    use redis_tower::commands::keys::Del;
    use redis_tower::modules::tdigest::*;

    async fn setup_redis() -> RedisClient {
        RedisClient::connect("localhost:6379")
            .await
            .expect("Failed to connect to Redis")
    }

    #[tokio::test]
    async fn test_tdigest_create() {
        let client = setup_redis().await;
        let key = "tdigest_test_create";

        // Create t-digest
        client.call(TDigestCreate::new(key)).await.unwrap();

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_tdigest_create_with_compression() {
        let client = setup_redis().await;
        let key = "tdigest_test_compression";

        // Create with custom compression
        client
            .call(TDigestCreate::new(key).compression(500))
            .await
            .unwrap();

        // Verify compression setting via INFO
        let info: TDigestInfoResult = client.call(TDigestInfo::new(key)).await.unwrap();
        assert_eq!(info.compression, 500);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_tdigest_add_and_query() {
        let client = setup_redis().await;
        let key = "tdigest_test_add";

        // Create and add values
        client.call(TDigestCreate::new(key)).await.unwrap();
        client
            .call(TDigestAdd::new(
                key,
                vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0],
            ))
            .await
            .unwrap();

        // Get min and max
        let min: f64 = client.call(TDigestMin::new(key)).await.unwrap();
        let max: f64 = client.call(TDigestMax::new(key)).await.unwrap();
        assert_eq!(min, 10.0);
        assert_eq!(max, 100.0);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_tdigest_quantile() {
        let client = setup_redis().await;
        let key = "tdigest_test_quantile";

        // Create and add values (1-100)
        client.call(TDigestCreate::new(key)).await.unwrap();
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        client.call(TDigestAdd::new(key, values)).await.unwrap();

        // Get p50, p95, p99
        let quantiles: Vec<f64> = client
            .call(TDigestQuantile::new(key, vec![0.50, 0.95, 0.99]))
            .await
            .unwrap();

        // p50 should be around 50, p95 around 95, p99 around 99
        assert!(
            quantiles[0] > 45.0 && quantiles[0] < 55.0,
            "p50 out of range"
        );
        assert!(
            quantiles[1] > 90.0 && quantiles[1] < 98.0,
            "p95 out of range"
        );
        assert!(
            quantiles[2] > 95.0 && quantiles[2] <= 100.0,
            "p99 out of range"
        );

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_tdigest_cdf() {
        let client = setup_redis().await;
        let key = "tdigest_test_cdf";

        // Create and add values (1-100)
        client.call(TDigestCreate::new(key)).await.unwrap();
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        client.call(TDigestAdd::new(key, values)).await.unwrap();

        // Get CDF for values 25, 50, 75
        let cdfs: Vec<f64> = client
            .call(TDigestCdf::new(key, vec![25.0, 50.0, 75.0]))
            .await
            .unwrap();

        // CDF should be approximately 0.25, 0.50, 0.75
        assert!(cdfs[0] > 0.20 && cdfs[0] < 0.30, "CDF(25) out of range");
        assert!(cdfs[1] > 0.45 && cdfs[1] < 0.55, "CDF(50) out of range");
        assert!(cdfs[2] > 0.70 && cdfs[2] < 0.80, "CDF(75) out of range");

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_tdigest_trimmed_mean() {
        let client = setup_redis().await;
        let key = "tdigest_test_trimmed";

        // Create and add values with some outliers
        client.call(TDigestCreate::new(key)).await.unwrap();
        let mut values: Vec<f64> = (40..=60).map(|i| i as f64).collect();
        values.extend(vec![1.0, 2.0, 98.0, 99.0]); // Add outliers
        client.call(TDigestAdd::new(key, values)).await.unwrap();

        // Get trimmed mean (trim bottom 10% and top 10%)
        let trimmed_mean: f64 = client
            .call(TDigestTrimmedMean::new(key, 0.1, 0.9))
            .await
            .unwrap();

        // Trimmed mean should be close to 50 (middle values)
        assert!(
            trimmed_mean > 45.0 && trimmed_mean < 55.0,
            "Trimmed mean out of expected range: {}",
            trimmed_mean
        );

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_tdigest_rank() {
        let client = setup_redis().await;
        let key = "tdigest_test_rank";

        // Create and add values (1-100)
        client.call(TDigestCreate::new(key)).await.unwrap();
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        client.call(TDigestAdd::new(key, values)).await.unwrap();

        // Get ranks for 25, 50, 75
        let ranks: Vec<i64> = client
            .call(TDigestRank::new(key, vec![25.0, 50.0, 75.0]))
            .await
            .unwrap();

        // Ranks should be approximately 25, 50, 75
        assert!(
            ranks[0] >= 20 && ranks[0] <= 30,
            "Rank(25) out of range: {}",
            ranks[0]
        );
        assert!(
            ranks[1] >= 45 && ranks[1] <= 55,
            "Rank(50) out of range: {}",
            ranks[1]
        );
        assert!(
            ranks[2] >= 70 && ranks[2] <= 80,
            "Rank(75) out of range: {}",
            ranks[2]
        );

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_tdigest_revrank() {
        let client = setup_redis().await;
        let key = "tdigest_test_revrank";

        // Create and add values (1-100)
        client.call(TDigestCreate::new(key)).await.unwrap();
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        client.call(TDigestAdd::new(key, values)).await.unwrap();

        // Get reverse ranks for 25, 50, 75
        let revranks: Vec<i64> = client
            .call(TDigestRevRank::new(key, vec![25.0, 50.0, 75.0]))
            .await
            .unwrap();

        // Reverse ranks should be approximately 75, 50, 25
        assert!(
            revranks[0] >= 70 && revranks[0] <= 80,
            "RevRank(25) out of range: {}",
            revranks[0]
        );
        assert!(
            revranks[1] >= 45 && revranks[1] <= 55,
            "RevRank(50) out of range: {}",
            revranks[1]
        );
        assert!(
            revranks[2] >= 20 && revranks[2] <= 30,
            "RevRank(75) out of range: {}",
            revranks[2]
        );

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_tdigest_byrank() {
        let client = setup_redis().await;
        let key = "tdigest_test_byrank";

        // Create and add values (1-100)
        client.call(TDigestCreate::new(key)).await.unwrap();
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        client.call(TDigestAdd::new(key, values)).await.unwrap();

        // Get values at ranks 25, 50, 75
        let values_at_rank: Vec<f64> = client
            .call(TDigestByRank::new(key, vec![25, 50, 75]))
            .await
            .unwrap();

        // Values should be approximately 25, 50, 75
        assert!(
            values_at_rank[0] > 20.0 && values_at_rank[0] < 30.0,
            "Value at rank 25 out of range: {}",
            values_at_rank[0]
        );
        assert!(
            values_at_rank[1] > 45.0 && values_at_rank[1] < 55.0,
            "Value at rank 50 out of range: {}",
            values_at_rank[1]
        );
        assert!(
            values_at_rank[2] > 70.0 && values_at_rank[2] < 80.0,
            "Value at rank 75 out of range: {}",
            values_at_rank[2]
        );

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_tdigest_byrevrank() {
        let client = setup_redis().await;
        let key = "tdigest_test_byrevrank";

        // Create and add values (1-100)
        client.call(TDigestCreate::new(key)).await.unwrap();
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        client.call(TDigestAdd::new(key, values)).await.unwrap();

        // Get values at reverse ranks 25, 50, 75
        let values_at_revrank: Vec<f64> = client
            .call(TDigestByRevRank::new(key, vec![25, 50, 75]))
            .await
            .unwrap();

        // Values should be approximately 75, 50, 25 (reversed)
        assert!(
            values_at_revrank[0] > 70.0 && values_at_revrank[0] < 80.0,
            "Value at revrank 25 out of range: {}",
            values_at_revrank[0]
        );
        assert!(
            values_at_revrank[1] > 45.0 && values_at_revrank[1] < 55.0,
            "Value at revrank 50 out of range: {}",
            values_at_revrank[1]
        );
        assert!(
            values_at_revrank[2] > 20.0 && values_at_revrank[2] < 30.0,
            "Value at revrank 75 out of range: {}",
            values_at_revrank[2]
        );

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_tdigest_reset() {
        let client = setup_redis().await;
        let key = "tdigest_test_reset";

        // Create and add values
        client.call(TDigestCreate::new(key)).await.unwrap();
        client
            .call(TDigestAdd::new(key, vec![1.0, 2.0, 3.0, 4.0, 5.0]))
            .await
            .unwrap();

        // Verify data exists
        let min_before: f64 = client.call(TDigestMin::new(key)).await.unwrap();
        assert_eq!(min_before, 1.0);

        // Reset
        client.call(TDigestReset::new(key)).await.unwrap();

        // Verify reset by checking info
        let info: TDigestInfoResult = client.call(TDigestInfo::new(key)).await.unwrap();
        assert_eq!(info.observations, 0);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_tdigest_merge() {
        let client = setup_redis().await;
        let key1 = "tdigest_test_merge1";
        let key2 = "tdigest_test_merge2";
        let dest = "tdigest_test_merged";

        // Create two t-digests
        client.call(TDigestCreate::new(key1)).await.unwrap();
        client.call(TDigestCreate::new(key2)).await.unwrap();

        // Add different values to each
        client
            .call(TDigestAdd::new(key1, vec![1.0, 2.0, 3.0, 4.0, 5.0]))
            .await
            .unwrap();
        client
            .call(TDigestAdd::new(key2, vec![6.0, 7.0, 8.0, 9.0, 10.0]))
            .await
            .unwrap();

        // Merge into destination
        client
            .call(TDigestMerge::new(
                dest,
                vec![key1.to_string(), key2.to_string()],
            ))
            .await
            .unwrap();

        // Verify merged result has all values
        let min: f64 = client.call(TDigestMin::new(dest)).await.unwrap();
        let max: f64 = client.call(TDigestMax::new(dest)).await.unwrap();
        assert_eq!(min, 1.0);
        assert_eq!(max, 10.0);

        // Cleanup
        client
            .call(Del::new(vec![
                key1.to_string(),
                key2.to_string(),
                dest.to_string(),
            ]))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_tdigest_info() {
        let client = setup_redis().await;
        let key = "tdigest_test_info";

        // Create with custom compression
        client
            .call(TDigestCreate::new(key).compression(200))
            .await
            .unwrap();

        // Add some values
        client
            .call(TDigestAdd::new(key, vec![1.0, 2.0, 3.0, 4.0, 5.0]))
            .await
            .unwrap();

        // Get info
        let info: TDigestInfoResult = client.call(TDigestInfo::new(key)).await.unwrap();

        // Verify metadata
        assert_eq!(info.compression, 200);
        assert_eq!(info.observations, 5);
        assert!(info.memory_usage > 0);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }
}
