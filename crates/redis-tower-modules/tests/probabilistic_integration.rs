//! Live-server integration tests for the probabilistic data-structure clients
//! ([`BloomFilter`], [`CuckooFilter`], [`CountMinSketch`], [`TopK`], and
//! [`TDigest`]).
//!
//! These exercise the Bloom/Cuckoo/CMS/TopK/T-Digest commands against a real
//! server, so they require a Redis Stack build (CI runs Redis 8.0.6 with
//! Stack). They are `#[ignore]`d by default and only run when explicitly
//! requested:
//!
//! ```sh
//! cargo test -p redis-tower-modules --test probabilistic_integration --features probabilistic -- --ignored
//! ```
//!
//! The server defaults to `redis://127.0.0.1:6399` (the standard workspace test
//! port) and can be overridden with the `REDIS_URL` environment variable.

#![cfg(feature = "probabilistic")]

use redis_tower_core::RedisConnection;
use redis_tower_modules::probabilistic::{
    BloomFilter, CountMinSketch, CuckooFilter, TDigest, TopK,
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
#[ignore = "requires a live Redis Stack server with the Bloom module"]
async fn bloom_filter_add_and_exists() {
    let mut conn = connect().await;
    let key = format!("test:bf:{}", unique_suffix());

    {
        let mut bf = BloomFilter::new(&mut conn, key.clone());

        // BF.ADD a fresh item returns true (newly added).
        assert!(bf.add("alice").await.unwrap());

        // BF.EXISTS reports the item as present, and a different item as absent.
        assert!(bf.exists("alice").await.unwrap());
        assert!(!bf.exists("bob").await.unwrap());

        // BF.INFO reports a non-zero capacity after the implicit reserve.
        let info = bf.info().await.unwrap();
        assert!(info.capacity > 0);
        assert_eq!(info.num_items_inserted, 1);
    }

    use redis_tower::commands::Del;
    conn.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a live Redis Stack server with the Bloom module"]
async fn cuckoo_filter_add_exists_del() {
    let mut conn = connect().await;
    let key = format!("test:cf:{}", unique_suffix());

    {
        let mut cf = CuckooFilter::new(&mut conn, key.clone());

        // CF.ADD then CF.EXISTS.
        assert!(cf.add("item").await.unwrap());
        assert!(cf.exists("item").await.unwrap());
        assert!(!cf.exists("missing").await.unwrap());

        // CF.DEL removes the single occurrence.
        assert!(cf.del("item").await.unwrap());
        assert!(!cf.exists("item").await.unwrap());
    }

    use redis_tower::commands::Del;
    conn.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a live Redis Stack server with the CMS module"]
async fn count_min_sketch_incrby_and_query() {
    let mut conn = connect().await;
    let key = format!("test:cms:{}", unique_suffix());

    {
        let mut cms = CountMinSketch::new(&mut conn, key.clone());

        // CMS.INITBYDIM sets up the sketch dimensions.
        cms.init_by_dim(100, 5).await.unwrap();

        // CMS.INCRBY bumps the counts for two items.
        let counts = cms.incrby(&[("foo", 5), ("bar", 3)]).await.unwrap();
        assert_eq!(counts, vec![5, 3]);

        // CMS.QUERY returns at-least estimates for those items.
        let queried = cms.query(&["foo", "bar", "baz"]).await.unwrap();
        assert!(queried[0] >= 5);
        assert!(queried[1] >= 3);
        assert_eq!(queried[2], 0);
    }

    use redis_tower::commands::Del;
    conn.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a live Redis Stack server with the TopK module"]
async fn topk_reserve_add_and_query() {
    let mut conn = connect().await;
    let key = format!("test:topk:{}", unique_suffix());

    {
        let mut topk = TopK::new(&mut conn, key.clone());

        // TOPK.RESERVE creates the structure.
        topk.reserve(3).await.unwrap();

        // TOPK.ADD inserts items.
        topk.add(&["a", "b", "c", "a"]).await.unwrap();

        // TOPK.QUERY reports membership in the top-k set.
        let present = topk.query(&["a"]).await.unwrap();
        assert_eq!(present, vec![true]);

        // TOPK.LIST returns the tracked items (at most k of them).
        let list = topk.list().await.unwrap();
        assert!(list.contains(&"a".to_string()));
        assert!(list.len() <= 3);
    }

    use redis_tower::commands::Del;
    conn.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a live Redis Stack server with the T-Digest module"]
async fn tdigest_create_add_and_quantile() {
    let mut conn = connect().await;
    let key = format!("test:td:{}", unique_suffix());

    {
        let mut td = TDigest::new(&mut conn, key.clone());

        // TDIGEST.CREATE then TDIGEST.ADD a handful of observations.
        td.create().await.unwrap();
        td.add(&[1.0, 2.0, 3.0, 4.0, 5.0]).await.unwrap();

        // TDIGEST.MIN / MAX bracket the observed values.
        assert_eq!(td.min().await.unwrap(), 1.0);
        assert_eq!(td.max().await.unwrap(), 5.0);

        // TDIGEST.QUANTILE returns one estimate per requested quantile, in range.
        let quantiles = td.quantile(&[0.0, 0.5, 1.0]).await.unwrap();
        assert_eq!(quantiles.len(), 3);
        assert_eq!(quantiles[0], 1.0);
        assert_eq!(quantiles[2], 5.0);
    }

    use redis_tower::commands::Del;
    conn.execute(Del::new(key)).await.unwrap();
}
