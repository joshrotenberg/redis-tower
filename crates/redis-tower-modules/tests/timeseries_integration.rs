//! Live-server integration tests for [`TimeSeriesClient`].
//!
//! These exercise RedisTimeSeries (`TS.*`) commands against a real server, so
//! they require a Redis Stack build. CI does not run them -- its Redis is
//! built without modules -- so they are `#[ignore]`d by default and only run
//! when explicitly requested:
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
    TimeSeriesClient, TsKeyConfig, TsMRangeQuery, TsRangeQuery, TsSample, TsTimestamp,
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
    let suffix = unique_suffix();
    let key = format!("test:ts:{suffix}");
    let test_id = suffix.to_string();
    let filter = format!("test_id={test_id}");

    {
        let mut ts = TimeSeriesClient::new(&mut conn);

        // TS.CREATE with a retention window and a label.
        ts.create(
            &key,
            TsKeyConfig::new()
                .retention(3_600_000)
                .label("sensor", "temperature")
                .label("test_id", &test_id),
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
        assert!(
            info.labels
                .iter()
                .any(|label| label.key == "sensor" && label.value == "temperature")
        );

        // RESP3 uses a set for TS.QUERYINDEX and maps for the multi-key reads.
        let matching_keys = ts.query_index(&filter).await.unwrap();
        assert_eq!(matching_keys, vec![key.clone()]);

        let mget = ts.mget(&filter, true).await.unwrap();
        assert_eq!(mget.len(), 1);
        assert_eq!(mget[0].key, key);
        assert_eq!(mget[0].samples.last(), last.as_ref());

        let mrange = ts
            .mrange(TsMRangeQuery::new(TsRangeQuery::all(), &filter).withlabels())
            .await
            .unwrap();
        assert_eq!(mrange.len(), 1);
        assert_eq!(mrange[0].key, key);
        assert_eq!(mrange[0].samples, samples);
    }

    use redis_tower::commands::Del;
    conn.execute(Del::new(key)).await.unwrap();
}
