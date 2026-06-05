//! Live-server integration tests for [`TimeSeriesClient`].
//!
//! These exercise RedisTimeSeries (`TS.*`) commands against a real server, so
//! they require a Redis Stack build (CI runs Redis 8.0.6 with Stack). They are
//! `#[ignore]`d by default and only run when explicitly requested:
//!
//! ```sh
//! cargo test -p redis-tower-modules --test timeseries_integration --features timeseries -- --ignored
//! ```
//!
//! The server defaults to `redis://127.0.0.1:6399` (the standard workspace test
//! port) and can be overridden with the `REDIS_URL` environment variable.

#![cfg(feature = "timeseries")]

use redis_tower_core::RedisConnection;
use redis_tower_modules::timeseries::{
    TimeSeriesClient, TsKeyConfig, TsRangeQuery, TsSample, TsTimestamp,
};

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
#[ignore = "requires a live Redis Stack server with RedisTimeSeries"]
async fn timeseries_add_and_range() {
    let mut conn = connect().await;
    let key = format!("test:ts:{}", unique_suffix());

    {
        let mut ts = TimeSeriesClient::new(&mut conn);

        // TS.CREATE with a retention window and a label.
        ts.create(
            &key,
            TsKeyConfig::new()
                .retention(3_600_000)
                .label("sensor", "temperature"),
        )
        .await
        .unwrap();

        // TS.ADD two samples at explicit timestamps.
        let t0 = ts.add(&key, TsTimestamp::Value(1_000), 21.5).await.unwrap();
        assert_eq!(t0, 1_000);
        let t1 = ts.add(&key, TsTimestamp::Value(2_000), 22.5).await.unwrap();
        assert_eq!(t1, 2_000);

        // TS.RANGE over the full range returns both samples in order.
        let samples = ts.range(&key, TsRangeQuery::all()).await.unwrap();
        assert_eq!(
            samples,
            vec![
                TsSample {
                    timestamp: 1_000,
                    value: 21.5,
                },
                TsSample {
                    timestamp: 2_000,
                    value: 22.5,
                },
            ]
        );

        // TS.GET returns the most recent sample.
        let last = ts.get(&key).await.unwrap();
        assert_eq!(
            last,
            Some(TsSample {
                timestamp: 2_000,
                value: 22.5,
            })
        );

        // TS.INFO reflects the sample count and the configured label.
        let info = ts.info(&key).await.unwrap();
        assert_eq!(info.total_samples, 2);
        assert_eq!(info.labels.len(), 1);
        assert_eq!(info.labels[0].key, "sensor");
        assert_eq!(info.labels[0].value, "temperature");
    }

    use redis_tower::commands::Del;
    conn.execute(Del::new(key)).await.unwrap();
}
