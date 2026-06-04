//! # Probabilistic Data Structures
//!
//! High-level clients for Bloom filter, Cuckoo filter, Count-Min Sketch,
//! TopK, and T-Digest. Each client binds to a single Redis key and holds a
//! mutable borrow of an executor, so it can be created cheaply from any
//! `&mut impl RedisExecutor` without transferring ownership.

use std::collections::HashMap;

use redis_tower::RedisExecutor;
use redis_tower_commands::{
    BfAdd, BfExists, BfInfo, BfInsert, BfMAdd, BfMExists, BfReserve, CfAdd, CfAddNx, CfCount,
    CfDel, CfExists, CfInfo, CfInsert, CfInsertNx, CfMExists, CfReserve, CmsIncrBy,
    CmsInfo as CmsInfoCmd, CmsInitByDim, CmsInitByProb, CmsMerge, CmsQuery, TdigestAdd,
    TdigestByRank, TdigestByRevRank, TdigestCdf, TdigestCreate, TdigestInfo, TdigestMax,
    TdigestMerge, TdigestMin, TdigestQuantile, TdigestRank, TdigestReset, TdigestRevRank,
    TdigestTrimmedMean, TopkAdd, TopkCount, TopkIncrBy, TopkInfo, TopkList, TopkQuery, TopkReserve,
};
use redis_tower_core::{Frame, RedisError};

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fn parse_flat_kv_frame(frame: Frame) -> Result<HashMap<String, Frame>, RedisError> {
    match frame {
        Frame::Array(Some(items)) => {
            let mut map = HashMap::new();
            let mut iter = items.into_iter();
            while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                let key = match k {
                    Frame::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
                    Frame::SimpleString(b) => String::from_utf8_lossy(&b).into_owned(),
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "bulk or simple string key",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                map.insert(key, v);
            }
            Ok(map)
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "flat key-value array",
            actual: format!("{other:?}"),
        }),
    }
}

fn get_i64(map: &HashMap<String, Frame>, field: &str) -> Result<i64, RedisError> {
    match map.get(field) {
        Some(Frame::Integer(n)) => Ok(*n),
        Some(other) => Err(RedisError::UnexpectedResponse {
            expected: "integer",
            actual: format!("{other:?}"),
        }),
        None => Err(RedisError::UnexpectedResponse {
            expected: "field present in response",
            actual: format!("field `{field}` not found"),
        }),
    }
}

fn get_f64(map: &HashMap<String, Frame>, field: &str) -> Result<f64, RedisError> {
    match map.get(field) {
        Some(Frame::Double(v)) => Ok(*v),
        Some(Frame::BulkString(Some(b))) => {
            let s = std::str::from_utf8(b).map_err(|_| RedisError::UnexpectedResponse {
                expected: "valid UTF-8",
                actual: format!("{b:?}"),
            })?;
            s.parse::<f64>()
                .map_err(|_| RedisError::UnexpectedResponse {
                    expected: "float",
                    actual: s.to_string(),
                })
        }
        Some(other) => Err(RedisError::UnexpectedResponse {
            expected: "double or bulk string",
            actual: format!("{other:?}"),
        }),
        None => Err(RedisError::UnexpectedResponse {
            expected: "field present in response",
            actual: format!("field `{field}` not found"),
        }),
    }
}

// ===========================================================================
// BloomFilter
// ===========================================================================

/// Info returned by [`BloomFilter::info`].
#[derive(Debug, Clone)]
pub struct BloomFilterInfo {
    /// Configured capacity of the filter.
    pub capacity: i64,
    /// Memory size of the filter in bytes.
    pub size: i64,
    /// Number of sub-filters (scaling filters only).
    pub num_filters: i64,
    /// Number of items that have been inserted.
    pub num_items_inserted: i64,
    /// Expansion factor applied when the filter scales.
    pub expansion_rate: i64,
}

/// Config for [`BloomFilter::insert_with_config`].
#[derive(Debug, Clone, Default)]
pub struct BfInsertConfig {
    /// Desired initial capacity.
    pub capacity: Option<i64>,
    /// Desired false-positive error rate.
    pub error_rate: Option<f64>,
    /// Expansion factor for sub-filters.
    pub expansion: Option<i64>,
    /// If `true`, return an error when the key does not exist.
    pub nocreate: bool,
    /// If `true`, the filter will not scale beyond its initial capacity.
    pub nonscaling: bool,
}

/// High-level client for Bloom filter operations bound to a single key.
pub struct BloomFilter<'a, C> {
    key: String,
    conn: &'a mut C,
}

impl<'a, C: RedisExecutor + Send> BloomFilter<'a, C> {
    /// Create a new [`BloomFilter`] bound to `key`, borrowing `conn`.
    pub fn new(conn: &'a mut C, key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            conn,
        }
    }

    /// Create a Bloom filter (BF.RESERVE).
    pub async fn reserve(&mut self, error_rate: f64, capacity: i64) -> Result<(), RedisError> {
        self.conn
            .execute(BfReserve::new(&self.key, error_rate, capacity))
            .await
    }

    /// Create a non-scaling Bloom filter (BF.RESERVE … NONSCALING).
    pub async fn reserve_nonscaling(
        &mut self,
        error_rate: f64,
        capacity: i64,
    ) -> Result<(), RedisError> {
        self.conn
            .execute(BfReserve::new(&self.key, error_rate, capacity).nonscaling())
            .await
    }

    /// Create a Bloom filter with a custom expansion factor (BF.RESERVE … EXPANSION).
    pub async fn reserve_with_expansion(
        &mut self,
        error_rate: f64,
        capacity: i64,
        expansion: i64,
    ) -> Result<(), RedisError> {
        self.conn
            .execute(BfReserve::new(&self.key, error_rate, capacity).expansion(expansion))
            .await
    }

    /// Add one item (BF.ADD). Returns `true` if newly added.
    pub async fn add(&mut self, item: &str) -> Result<bool, RedisError> {
        self.conn.execute(BfAdd::new(&self.key, item)).await
    }

    /// Add multiple items (BF.MADD).
    pub async fn madd(&mut self, items: &[&str]) -> Result<Vec<bool>, RedisError> {
        self.conn
            .execute(BfMAdd::new(&self.key, items.iter().copied()))
            .await
    }

    /// Insert items, auto-creating the filter if absent (BF.INSERT plain).
    pub async fn insert(&mut self, items: &[&str]) -> Result<Vec<bool>, RedisError> {
        self.conn
            .execute(BfInsert::new(&self.key, items.iter().copied()))
            .await
    }

    /// Insert items with full configuration options (BF.INSERT with options).
    pub async fn insert_with_config(
        &mut self,
        items: &[&str],
        config: BfInsertConfig,
    ) -> Result<Vec<bool>, RedisError> {
        let mut cmd = BfInsert::new(&self.key, items.iter().copied());
        if let Some(cap) = config.capacity {
            cmd = cmd.capacity(cap);
        }
        if let Some(err) = config.error_rate {
            cmd = cmd.error(err);
        }
        if let Some(exp) = config.expansion {
            cmd = cmd.expansion(exp);
        }
        if config.nocreate {
            cmd = cmd.nocreate();
        }
        if config.nonscaling {
            cmd = cmd.nonscaling();
        }
        self.conn.execute(cmd).await
    }

    /// Test membership of one item (BF.EXISTS).
    pub async fn exists(&mut self, item: &str) -> Result<bool, RedisError> {
        self.conn.execute(BfExists::new(&self.key, item)).await
    }

    /// Test membership of multiple items (BF.MEXISTS).
    pub async fn mexists(&mut self, items: &[&str]) -> Result<Vec<bool>, RedisError> {
        self.conn
            .execute(BfMExists::new(&self.key, items.iter().copied()))
            .await
    }

    /// Return filter metadata (BF.INFO).
    pub async fn info(&mut self) -> Result<BloomFilterInfo, RedisError> {
        let frame = self.conn.execute(BfInfo::new(&self.key)).await?;
        let map = parse_flat_kv_frame(frame)?;
        Ok(BloomFilterInfo {
            capacity: get_i64(&map, "Capacity")?,
            size: get_i64(&map, "Size")?,
            num_filters: get_i64(&map, "Number of filters")?,
            num_items_inserted: get_i64(&map, "Number of items inserted")?,
            expansion_rate: get_i64(&map, "Expansion rate")?,
        })
    }
}

// ===========================================================================
// CuckooFilter
// ===========================================================================

/// Info returned by [`CuckooFilter::info`].
#[derive(Debug, Clone)]
pub struct CuckooFilterInfo {
    /// Size of the filter in bytes.
    pub size: i64,
    /// Number of buckets.
    pub num_buckets: i64,
    /// Number of sub-filters.
    pub num_filters: i64,
    /// Number of items inserted.
    pub num_items_inserted: i64,
    /// Number of items deleted.
    pub num_items_deleted: i64,
    /// Number of items per bucket.
    pub bucket_size: i64,
    /// Expansion factor.
    pub expansion_rate: i64,
    /// Maximum number of cuckoo kicks before declaring failure.
    pub max_iterations: i64,
}

/// Config for [`CuckooFilter::reserve_with_config`].
#[derive(Debug, Clone, Default)]
pub struct CfReserveConfig {
    /// Items per bucket.
    pub bucketsize: Option<i64>,
    /// Maximum cuckoo kicks.
    pub maxiterations: Option<i64>,
    /// Expansion factor.
    pub expansion: Option<i64>,
}

/// High-level client for Cuckoo filter operations bound to a single key.
pub struct CuckooFilter<'a, C> {
    key: String,
    conn: &'a mut C,
}

impl<'a, C: RedisExecutor + Send> CuckooFilter<'a, C> {
    /// Create a new [`CuckooFilter`] bound to `key`, borrowing `conn`.
    pub fn new(conn: &'a mut C, key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            conn,
        }
    }

    /// Reserve a Cuckoo filter (CF.RESERVE plain).
    pub async fn reserve(&mut self, capacity: i64) -> Result<(), RedisError> {
        self.conn.execute(CfReserve::new(&self.key, capacity)).await
    }

    /// Reserve a Cuckoo filter with configuration options.
    pub async fn reserve_with_config(
        &mut self,
        capacity: i64,
        config: CfReserveConfig,
    ) -> Result<(), RedisError> {
        let mut cmd = CfReserve::new(&self.key, capacity);
        if let Some(bs) = config.bucketsize {
            cmd = cmd.bucketsize(bs);
        }
        if let Some(mi) = config.maxiterations {
            cmd = cmd.maxiterations(mi);
        }
        if let Some(exp) = config.expansion {
            cmd = cmd.expansion(exp);
        }
        self.conn.execute(cmd).await
    }

    /// Add one item (CF.ADD). Returns `true` if added successfully.
    pub async fn add(&mut self, item: &str) -> Result<bool, RedisError> {
        self.conn.execute(CfAdd::new(&self.key, item)).await
    }

    /// Add one item only if it does not already exist (CF.ADDNX).
    pub async fn add_nx(&mut self, item: &str) -> Result<bool, RedisError> {
        self.conn.execute(CfAddNx::new(&self.key, item)).await
    }

    /// Insert multiple items (CF.INSERT).
    pub async fn insert(&mut self, items: &[&str]) -> Result<Vec<bool>, RedisError> {
        self.conn
            .execute(CfInsert::new(&self.key, items.iter().copied()))
            .await
    }

    /// Insert multiple items only if they are absent (CF.INSERTNX).
    pub async fn insert_nx(&mut self, items: &[&str]) -> Result<Vec<bool>, RedisError> {
        self.conn
            .execute(CfInsertNx::new(&self.key, items.iter().copied()))
            .await
    }

    /// Test membership of one item (CF.EXISTS).
    pub async fn exists(&mut self, item: &str) -> Result<bool, RedisError> {
        self.conn.execute(CfExists::new(&self.key, item)).await
    }

    /// Test membership of multiple items (CF.MEXISTS).
    pub async fn mexists(&mut self, items: &[&str]) -> Result<Vec<bool>, RedisError> {
        self.conn
            .execute(CfMExists::new(&self.key, items.iter().copied()))
            .await
    }

    /// Count approximate occurrences of an item (CF.COUNT).
    pub async fn count(&mut self, item: &str) -> Result<i64, RedisError> {
        self.conn.execute(CfCount::new(&self.key, item)).await
    }

    /// Delete one occurrence of an item (CF.DEL). Returns `true` if found.
    pub async fn del(&mut self, item: &str) -> Result<bool, RedisError> {
        self.conn.execute(CfDel::new(&self.key, item)).await
    }

    /// Return filter metadata (CF.INFO).
    pub async fn info(&mut self) -> Result<CuckooFilterInfo, RedisError> {
        let frame = self.conn.execute(CfInfo::new(&self.key)).await?;
        let map = parse_flat_kv_frame(frame)?;
        Ok(CuckooFilterInfo {
            size: get_i64(&map, "Size")?,
            num_buckets: get_i64(&map, "Number of buckets")?,
            num_filters: get_i64(&map, "Number of filters")?,
            num_items_inserted: get_i64(&map, "Number of items inserted")?,
            num_items_deleted: get_i64(&map, "Number of items deleted")?,
            bucket_size: get_i64(&map, "Bucket size")?,
            expansion_rate: get_i64(&map, "Expansion rate")?,
            max_iterations: get_i64(&map, "Max iterations")?,
        })
    }
}

// ===========================================================================
// CountMinSketch
// ===========================================================================

/// Info returned by [`CountMinSketch::info`].
#[derive(Debug, Clone)]
pub struct CmsInfo {
    /// Width of the sketch (number of hash functions).
    pub width: i64,
    /// Depth of the sketch (number of counters per hash function).
    pub depth: i64,
    /// Total count of all items observed.
    pub count: i64,
}

/// High-level client for Count-Min Sketch operations bound to a single key.
pub struct CountMinSketch<'a, C> {
    key: String,
    conn: &'a mut C,
}

impl<'a, C: RedisExecutor + Send> CountMinSketch<'a, C> {
    /// Create a new [`CountMinSketch`] bound to `key`, borrowing `conn`.
    pub fn new(conn: &'a mut C, key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            conn,
        }
    }

    /// Initialise by explicit dimensions (CMS.INITBYDIM).
    pub async fn init_by_dim(&mut self, width: i64, depth: i64) -> Result<(), RedisError> {
        self.conn
            .execute(CmsInitByDim::new(&self.key, width, depth))
            .await
    }

    /// Initialise by error rate and probability (CMS.INITBYPROB).
    pub async fn init_by_prob(&mut self, error: f64, probability: f64) -> Result<(), RedisError> {
        self.conn
            .execute(CmsInitByProb::new(&self.key, error, probability))
            .await
    }

    /// Increment counts for one or more items (CMS.INCRBY).
    pub async fn incrby(&mut self, items: &[(&str, i64)]) -> Result<Vec<i64>, RedisError> {
        self.conn
            .execute(CmsIncrBy::new(
                &self.key,
                items.iter().map(|&(s, n)| (s, n)),
            ))
            .await
    }

    /// Query estimated counts for one or more items (CMS.QUERY).
    pub async fn query(&mut self, items: &[&str]) -> Result<Vec<i64>, RedisError> {
        self.conn
            .execute(CmsQuery::new(&self.key, items.iter().copied()))
            .await
    }

    /// Merge multiple sketches into `destination` (CMS.MERGE, no weights).
    ///
    /// Note: `self.key` is not used as the destination; pass it explicitly.
    pub async fn merge(&mut self, destination: &str, sources: &[&str]) -> Result<(), RedisError> {
        self.conn
            .execute(CmsMerge::new(destination, sources.iter().copied()))
            .await
    }

    /// Merge multiple sketches into `destination` with per-source weights
    /// (CMS.MERGE … WEIGHTS).
    pub async fn merge_weighted(
        &mut self,
        destination: &str,
        sources: &[(&str, i64)],
    ) -> Result<(), RedisError> {
        let keys: Vec<&str> = sources.iter().map(|(k, _)| *k).collect();
        let weights: Vec<i64> = sources.iter().map(|(_, w)| *w).collect();
        self.conn
            .execute(CmsMerge::new(destination, keys.iter().copied()).weights(weights))
            .await
    }

    /// Return sketch metadata (CMS.INFO).
    pub async fn info(&mut self) -> Result<CmsInfo, RedisError> {
        let frame = self.conn.execute(CmsInfoCmd::new(&self.key)).await?;
        let map = parse_flat_kv_frame(frame)?;
        Ok(CmsInfo {
            width: get_i64(&map, "width")?,
            depth: get_i64(&map, "depth")?,
            count: get_i64(&map, "count")?,
        })
    }
}

// ===========================================================================
// TopK
// ===========================================================================

/// Info returned by [`TopK::info`].
#[derive(Debug, Clone)]
pub struct TopKInfo {
    /// k — number of top-k items tracked.
    pub k: i64,
    /// Width of the heavy-keeper sketch.
    pub width: i64,
    /// Depth of the heavy-keeper sketch.
    pub depth: i64,
    /// Decay factor.
    pub decay: f64,
}

/// High-level client for TopK operations bound to a single key.
pub struct TopK<'a, C> {
    key: String,
    conn: &'a mut C,
}

impl<'a, C: RedisExecutor + Send> TopK<'a, C> {
    /// Create a new [`TopK`] bound to `key`, borrowing `conn`.
    pub fn new(conn: &'a mut C, key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            conn,
        }
    }

    /// Reserve a Top-K structure (TOPK.RESERVE plain).
    pub async fn reserve(&mut self, k: i64) -> Result<(), RedisError> {
        self.conn.execute(TopkReserve::new(&self.key, k)).await
    }

    /// Reserve a Top-K structure with explicit width, depth, and decay.
    pub async fn reserve_with_params(
        &mut self,
        k: i64,
        width: i64,
        depth: i64,
        decay: f64,
    ) -> Result<(), RedisError> {
        self.conn
            .execute(TopkReserve::new(&self.key, k).params(width, depth, decay))
            .await
    }

    /// Add items (TOPK.ADD). Returns evicted items for each slot, if any.
    pub async fn add(&mut self, items: &[&str]) -> Result<Vec<Option<String>>, RedisError> {
        let raw = self
            .conn
            .execute(TopkAdd::new(&self.key, items.iter().copied()))
            .await?;
        Ok(raw
            .into_iter()
            .map(|opt| opt.map(|b| String::from_utf8_lossy(&b).into_owned()))
            .collect())
    }

    /// Increment item scores (TOPK.INCRBY). Returns evicted items, if any.
    pub async fn incrby(
        &mut self,
        items: &[(&str, i64)],
    ) -> Result<Vec<Option<String>>, RedisError> {
        let raw = self
            .conn
            .execute(TopkIncrBy::new(
                &self.key,
                items.iter().map(|&(s, n)| (s, n)),
            ))
            .await?;
        Ok(raw
            .into_iter()
            .map(|opt| opt.map(|b| String::from_utf8_lossy(&b).into_owned()))
            .collect())
    }

    /// Check whether items are in the Top-K (TOPK.QUERY).
    pub async fn query(&mut self, items: &[&str]) -> Result<Vec<bool>, RedisError> {
        self.conn
            .execute(TopkQuery::new(&self.key, items.iter().copied()))
            .await
    }

    /// Return approximate counts for items (TOPK.COUNT).
    pub async fn count(&mut self, items: &[&str]) -> Result<Vec<i64>, RedisError> {
        self.conn
            .execute(TopkCount::new(&self.key, items.iter().copied()))
            .await
    }

    /// List all top-k items (TOPK.LIST, no WITHCOUNT).
    pub async fn list(&mut self) -> Result<Vec<String>, RedisError> {
        let frame = self.conn.execute(TopkList::new(&self.key)).await?;
        match frame {
            Frame::Array(Some(items)) => items
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(b)) => Ok(String::from_utf8_lossy(&b).into_owned()),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string",
                        actual: format!("{other:?}"),
                    }),
                })
                .collect(),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    /// List top-k items with approximate counts (TOPK.LIST WITHCOUNT).
    ///
    /// Returns a `Vec` of `(item, count)` pairs.
    pub async fn list_with_counts(&mut self) -> Result<Vec<(String, i64)>, RedisError> {
        let frame = self
            .conn
            .execute(TopkList::new(&self.key).withcount())
            .await?;
        match frame {
            Frame::Array(Some(items)) => {
                let mut result = Vec::new();
                let mut iter = items.into_iter();
                while let (Some(item_f), Some(count_f)) = (iter.next(), iter.next()) {
                    let item = match item_f {
                        Frame::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string item",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    let count = match count_f {
                        Frame::Integer(n) => n,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "integer count",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    result.push((item, count));
                }
                Ok(result)
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    /// Return Top-K metadata (TOPK.INFO).
    pub async fn info(&mut self) -> Result<TopKInfo, RedisError> {
        let frame = self.conn.execute(TopkInfo::new(&self.key)).await?;
        let map = parse_flat_kv_frame(frame)?;
        Ok(TopKInfo {
            k: get_i64(&map, "k")?,
            width: get_i64(&map, "width")?,
            depth: get_i64(&map, "depth")?,
            decay: get_f64(&map, "decay")?,
        })
    }
}

// ===========================================================================
// TDigest
// ===========================================================================

/// Info returned by [`TDigest::info`].
#[derive(Debug, Clone)]
pub struct TDigestInfo {
    /// Compression parameter.
    pub compression: i64,
    /// Maximum number of centroids (centroid capacity).
    pub capacity: i64,
    /// Number of merged centroids.
    pub merged_nodes: i64,
    /// Number of unmerged data points.
    pub unmerged_nodes: i64,
    /// Total weight of merged centroids.
    pub merged_weight: f64,
    /// Total weight of unmerged data points.
    pub unmerged_weight: f64,
    /// Total number of compressions that have been performed.
    pub total_compressions: i64,
    /// Memory usage in bytes.
    pub memory_usage: i64,
}

/// High-level client for T-Digest operations bound to a single key.
pub struct TDigest<'a, C> {
    key: String,
    conn: &'a mut C,
}

impl<'a, C: RedisExecutor + Send> TDigest<'a, C> {
    /// Create a new [`TDigest`] bound to `key`, borrowing `conn`.
    pub fn new(conn: &'a mut C, key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            conn,
        }
    }

    /// Create an empty T-Digest sketch (TDIGEST.CREATE).
    pub async fn create(&mut self) -> Result<(), RedisError> {
        self.conn.execute(TdigestCreate::new(&self.key)).await
    }

    /// Create an empty T-Digest sketch with a compression parameter.
    pub async fn create_with_compression(&mut self, compression: i64) -> Result<(), RedisError> {
        self.conn
            .execute(TdigestCreate::new(&self.key).compression(compression))
            .await
    }

    /// Add values to the sketch (TDIGEST.ADD).
    pub async fn add(&mut self, values: &[f64]) -> Result<(), RedisError> {
        self.conn
            .execute(TdigestAdd::new(&self.key, values.iter().copied()))
            .await
    }

    /// Merge source sketches into `destination` (TDIGEST.MERGE).
    ///
    /// Note: `self.key` is not used as the destination; pass it explicitly.
    pub async fn merge(&mut self, destination: &str, sources: &[&str]) -> Result<(), RedisError> {
        self.conn
            .execute(TdigestMerge::new(destination, sources.iter().copied()))
            .await
    }

    /// Reset the sketch, discarding all observations (TDIGEST.RESET).
    pub async fn reset(&mut self) -> Result<(), RedisError> {
        self.conn.execute(TdigestReset::new(&self.key)).await
    }

    /// Cumulative distribution function at each value (TDIGEST.CDF).
    pub async fn cdf(&mut self, values: &[f64]) -> Result<Vec<f64>, RedisError> {
        self.conn
            .execute(TdigestCdf::new(&self.key, values.iter().copied()))
            .await
    }

    /// Estimated value at each quantile (TDIGEST.QUANTILE).
    pub async fn quantile(&mut self, quantiles: &[f64]) -> Result<Vec<f64>, RedisError> {
        self.conn
            .execute(TdigestQuantile::new(&self.key, quantiles.iter().copied()))
            .await
    }

    /// Minimum observed value (TDIGEST.MIN).
    pub async fn min(&mut self) -> Result<f64, RedisError> {
        self.conn.execute(TdigestMin::new(&self.key)).await
    }

    /// Maximum observed value (TDIGEST.MAX).
    pub async fn max(&mut self) -> Result<f64, RedisError> {
        self.conn.execute(TdigestMax::new(&self.key)).await
    }

    /// Trimmed mean between two quantile bounds (TDIGEST.TRIMMED_MEAN).
    pub async fn trimmed_mean(&mut self, low: f64, high: f64) -> Result<f64, RedisError> {
        self.conn
            .execute(TdigestTrimmedMean::new(&self.key, low, high))
            .await
    }

    /// Estimated rank of each value (TDIGEST.RANK).
    pub async fn rank(&mut self, values: &[f64]) -> Result<Vec<i64>, RedisError> {
        self.conn
            .execute(TdigestRank::new(&self.key, values.iter().copied()))
            .await
    }

    /// Estimated reverse rank of each value (TDIGEST.REVRANK).
    pub async fn revrank(&mut self, values: &[f64]) -> Result<Vec<i64>, RedisError> {
        self.conn
            .execute(TdigestRevRank::new(&self.key, values.iter().copied()))
            .await
    }

    /// Estimated value at each rank (TDIGEST.BYRANK).
    pub async fn byrank(&mut self, ranks: &[i64]) -> Result<Vec<f64>, RedisError> {
        self.conn
            .execute(TdigestByRank::new(&self.key, ranks.iter().copied()))
            .await
    }

    /// Estimated value at each reverse rank (TDIGEST.BYREVRANK).
    pub async fn byrevrank(&mut self, ranks: &[i64]) -> Result<Vec<f64>, RedisError> {
        self.conn
            .execute(TdigestByRevRank::new(&self.key, ranks.iter().copied()))
            .await
    }

    /// Return sketch metadata (TDIGEST.INFO).
    pub async fn info(&mut self) -> Result<TDigestInfo, RedisError> {
        let frame = self.conn.execute(TdigestInfo::new(&self.key)).await?;
        let map = parse_flat_kv_frame(frame)?;
        Ok(TDigestInfo {
            compression: get_i64(&map, "Compression")?,
            capacity: get_i64(&map, "Capacity")?,
            merged_nodes: get_i64(&map, "Merged nodes")?,
            unmerged_nodes: get_i64(&map, "Unmerged nodes")?,
            merged_weight: get_f64(&map, "Merged weight")?,
            unmerged_weight: get_f64(&map, "Unmerged weight")?,
            total_compressions: get_i64(&map, "Total compressions")?,
            memory_usage: get_i64(&map, "Memory usage")?,
        })
    }
}

// ===========================================================================
// Unit tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::future::Future;

    use bytes::Bytes;
    use redis_tower_core::Command;

    struct MockExecutor {
        responses: VecDeque<Frame>,
    }

    impl MockExecutor {
        fn new(frames: Vec<Frame>) -> Self {
            Self {
                responses: VecDeque::from(frames),
            }
        }
    }

    impl RedisExecutor for MockExecutor {
        fn execute<Cmd: Command>(
            &mut self,
            cmd: Cmd,
        ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
            let frame = self.responses.pop_front().unwrap_or(Frame::Null);
            async move { cmd.parse_response(frame) }
        }
    }

    fn ok_frame() -> Frame {
        Frame::SimpleString(Bytes::from_static(b"OK"))
    }

    // -----------------------------------------------------------------------
    // BloomFilter tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn bloom_add_returns_true() {
        let mut mock = MockExecutor::new(vec![Frame::Integer(1)]);
        let mut bf = BloomFilter::new(&mut mock, "test:bf");
        let result = bf.add("item").await.unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn bloom_madd_returns_vec() {
        let mut mock = MockExecutor::new(vec![Frame::Array(Some(vec![
            Frame::Integer(1),
            Frame::Integer(0),
        ]))]);
        let mut bf = BloomFilter::new(&mut mock, "test:bf");
        let result = bf.madd(&["a", "b"]).await.unwrap();
        assert_eq!(result, vec![true, false]);
    }

    #[tokio::test]
    async fn bloom_exists_returns_false() {
        let mut mock = MockExecutor::new(vec![Frame::Integer(0)]);
        let mut bf = BloomFilter::new(&mut mock, "test:bf");
        let result = bf.exists("missing").await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn bloom_info_parses() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("Capacity"))),
            Frame::Integer(1000),
            Frame::BulkString(Some(Bytes::from("Size"))),
            Frame::Integer(2048),
            Frame::BulkString(Some(Bytes::from("Number of filters"))),
            Frame::Integer(1),
            Frame::BulkString(Some(Bytes::from("Number of items inserted"))),
            Frame::Integer(0),
            Frame::BulkString(Some(Bytes::from("Expansion rate"))),
            Frame::Integer(2),
        ]));
        let mut mock = MockExecutor::new(vec![frame]);
        let mut bf = BloomFilter::new(&mut mock, "test:bf");
        let info = bf.info().await.unwrap();
        assert_eq!(info.capacity, 1000);
        assert_eq!(info.size, 2048);
        assert_eq!(info.num_filters, 1);
        assert_eq!(info.num_items_inserted, 0);
        assert_eq!(info.expansion_rate, 2);
    }

    // -----------------------------------------------------------------------
    // CuckooFilter tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cuckoo_add_returns_true() {
        let mut mock = MockExecutor::new(vec![Frame::Integer(1)]);
        let mut cf = CuckooFilter::new(&mut mock, "test:cf");
        let result = cf.add("item").await.unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn cuckoo_del_returns_bool() {
        let mut mock = MockExecutor::new(vec![Frame::Integer(1)]);
        let mut cf = CuckooFilter::new(&mut mock, "test:cf");
        let result = cf.del("item").await.unwrap();
        assert!(result);
    }

    // -----------------------------------------------------------------------
    // CountMinSketch tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cms_init_by_dim_ok() {
        let mut mock = MockExecutor::new(vec![ok_frame()]);
        let mut cms = CountMinSketch::new(&mut mock, "test:cms");
        cms.init_by_dim(100, 5).await.unwrap();
    }

    #[tokio::test]
    async fn cms_incrby_returns_counts() {
        let mut mock = MockExecutor::new(vec![Frame::Array(Some(vec![
            Frame::Integer(5),
            Frame::Integer(3),
        ]))]);
        let mut cms = CountMinSketch::new(&mut mock, "test:cms");
        let counts = cms.incrby(&[("foo", 5), ("bar", 3)]).await.unwrap();
        assert_eq!(counts, vec![5, 3]);
    }

    #[tokio::test]
    async fn cms_query_returns_counts() {
        let mut mock = MockExecutor::new(vec![Frame::Array(Some(vec![
            Frame::Integer(10),
            Frame::Integer(0),
        ]))]);
        let mut cms = CountMinSketch::new(&mut mock, "test:cms");
        let counts = cms.query(&["foo", "bar"]).await.unwrap();
        assert_eq!(counts, vec![10, 0]);
    }

    // -----------------------------------------------------------------------
    // TopK tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn topk_add_returns_evicted() {
        let mut mock = MockExecutor::new(vec![Frame::Array(Some(vec![
            Frame::Null,
            Frame::BulkString(Some(Bytes::from("evicted-item"))),
        ]))]);
        let mut topk = TopK::new(&mut mock, "test:topk");
        let result = topk.add(&["new1", "new2"]).await.unwrap();
        assert_eq!(result, vec![None, Some("evicted-item".to_string())]);
    }

    #[tokio::test]
    async fn topk_list_returns_items() {
        let mut mock = MockExecutor::new(vec![Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("alpha"))),
            Frame::BulkString(Some(Bytes::from("beta"))),
        ]))]);
        let mut topk = TopK::new(&mut mock, "test:topk");
        let items = topk.list().await.unwrap();
        assert_eq!(items, vec!["alpha", "beta"]);
    }

    // -----------------------------------------------------------------------
    // TDigest tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn tdigest_add_ok() {
        let mut mock = MockExecutor::new(vec![ok_frame()]);
        let mut td = TDigest::new(&mut mock, "test:td");
        td.add(&[1.0, 2.0, 3.0]).await.unwrap();
    }

    #[tokio::test]
    async fn tdigest_quantile_returns_values() {
        let mut mock = MockExecutor::new(vec![Frame::Array(Some(vec![
            Frame::Double(1.0),
            Frame::Double(5.0),
            Frame::Double(9.0),
        ]))]);
        let mut td = TDigest::new(&mut mock, "test:td");
        let values = td.quantile(&[0.1, 0.5, 0.9]).await.unwrap();
        assert_eq!(values, vec![1.0, 5.0, 9.0]);
    }

    #[tokio::test]
    async fn tdigest_info_parses() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("Compression"))),
            Frame::Integer(100),
            Frame::BulkString(Some(Bytes::from("Capacity"))),
            Frame::Integer(610),
            Frame::BulkString(Some(Bytes::from("Merged nodes"))),
            Frame::Integer(3),
            Frame::BulkString(Some(Bytes::from("Unmerged nodes"))),
            Frame::Integer(0),
            Frame::BulkString(Some(Bytes::from("Merged weight"))),
            Frame::Double(10.0),
            Frame::BulkString(Some(Bytes::from("Unmerged weight"))),
            Frame::Double(0.0),
            Frame::BulkString(Some(Bytes::from("Total compressions"))),
            Frame::Integer(1),
            Frame::BulkString(Some(Bytes::from("Memory usage"))),
            Frame::Integer(9136),
        ]));
        let mut mock = MockExecutor::new(vec![frame]);
        let mut td = TDigest::new(&mut mock, "test:td");
        let info = td.info().await.unwrap();
        assert_eq!(info.compression, 100);
        assert_eq!(info.capacity, 610);
        assert_eq!(info.merged_nodes, 3);
        assert_eq!(info.unmerged_nodes, 0);
        assert_eq!(info.merged_weight, 10.0);
        assert_eq!(info.unmerged_weight, 0.0);
        assert_eq!(info.total_compressions, 1);
        assert_eq!(info.memory_usage, 9136);
    }
}
