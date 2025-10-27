//! Integration tests for RedisTimeSeries module
//!
//! Tests Redis time-series data storage and queries.
//!
//! Run with: cargo test --test integration_timeseries --features timeseries
//!
//! Note: Requires Redis Stack with RedisTimeSeries module installed

#[cfg(feature = "timeseries")]
mod tests {
    use redis_tower::RedisClient;
    use redis_tower::commands::Del;
    use redis_tower::modules::timeseries::*;

    async fn setup_redis() -> RedisClient {
        RedisClient::connect("localhost:6379")
            .await
            .expect("Failed to connect to Redis")
    }

    #[tokio::test]
    async fn test_ts_create_and_add() {
        let client = setup_redis().await;
        let key = "ts_test_sensor";

        // Create time series
        client.call(TsCreate::new(key)).await.unwrap();

        // Add some data points
        let timestamp1 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let timestamp2 = timestamp1 + 1000; // 1 second later
        let timestamp3 = timestamp2 + 1000; // 2 seconds later

        client
            .call(TsAdd::new(key, timestamp1, 25.5))
            .await
            .unwrap();
        client
            .call(TsAdd::new(key, timestamp2, 26.0))
            .await
            .unwrap();
        client
            .call(TsAdd::new(key, timestamp3, 25.8))
            .await
            .unwrap();

        // Get latest value
        let latest: Option<Sample> = client.call(TsGet::new(key)).await.unwrap();
        assert!(latest.is_some());

        let sample = latest.unwrap();
        assert_eq!(sample.timestamp, timestamp3);
        assert!((sample.value - 25.8).abs() < 0.01);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_ts_range() {
        let client = setup_redis().await;
        let key = "ts_test_range";

        // Create time series
        client.call(TsCreate::new(key)).await.unwrap();

        // Add data points spanning 5 seconds
        let base_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        for i in 0..5 {
            let timestamp = base_timestamp + (i * 1000);
            let value = 20.0 + i as f64;
            client
                .call(TsAdd::new(key, timestamp, value))
                .await
                .unwrap();
        }

        // Query range
        let start = base_timestamp;
        let end = base_timestamp + 5000;

        let samples: Vec<Sample> = client.call(TsRange::new(key, start, end)).await.unwrap();

        // Should get all 5 samples
        assert_eq!(samples.len(), 5);

        // Verify first and last values
        assert!((samples[0].value - 20.0).abs() < 0.01);
        assert!((samples[4].value - 24.0).abs() < 0.01);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_ts_info() {
        let client = setup_redis().await;
        let key = "ts_test_info";

        // Create time series with retention
        client
            .call(TsCreate::new(key).retention(86400000)) // 1 day in ms
            .await
            .unwrap();

        // Add a data point
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        client.call(TsAdd::new(key, timestamp, 42.0)).await.unwrap();

        // Get info
        let info: TimeSeriesInfo = client.call(TsInfo::new(key)).await.unwrap();

        // Verify retention policy
        assert_eq!(info.retention_time, 86400000);

        // Verify we have at least one sample
        assert!(info.total_samples >= 1);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }
}
