//! # TimeSeries Client
//!
//! Ergonomic, typed client over [RedisTimeSeries](https://redis.io/docs/data-types/timeseries/).
//! Values are exchanged as [`TsSample`] pairs rather than raw [`Frame`] values.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::RedisClient;
//! use redis_tower_modules::timeseries::{TimeSeriesClient, TsKeyConfig, TsRangeQuery};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("redis://127.0.0.1:6379").await?;
//! let mut ts = TimeSeriesClient::new(client);
//!
//! // Create a key with a 1-hour retention window and a label.
//! ts.create(
//!     "sensors:temp",
//!     TsKeyConfig::new().retention(3_600_000).label("sensor", "temperature"),
//! )
//! .await?;
//!
//! // Append a sample with a server-assigned timestamp.
//! use redis_tower_modules::timeseries::TsTimestamp;
//! let ts_returned = ts.add("sensors:temp", TsTimestamp::Auto, 21.5).await?;
//!
//! // Query the last hour.
//! let samples = ts.range("sensors:temp", TsRangeQuery::all()).await?;
//! for s in samples {
//!     println!("{}: {}", s.timestamp, s.value);
//! }
//! # Ok(())
//! # }
//! ```

use redis_tower::RedisExecutor;
use redis_tower_core::{Frame, RedisError};

// Low-level command builders
use redis_tower::commands::{
    TsAdd, TsAlter, TsCreate, TsDecrBy, TsDel, TsGet, TsIncrBy, TsInfo, TsMAdd, TsMGet, TsMRange,
    TsMRevRange, TsQueryIndex, TsRange, TsRevRange,
};

// Re-export the enums from commands so callers don't need a second import.
pub use redis_tower::commands::{TsAggregation, TsDuplicatePolicy, TsEncoding, TsTimestamp};

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// A single time series sample: a timestamp (milliseconds since epoch) and
/// a floating-point value.
#[derive(Debug, Clone, PartialEq)]
pub struct TsSample {
    pub timestamp: i64,
    pub value: f64,
}

/// A label key-value pair attached to a TimeSeries key.
#[derive(Debug, Clone, PartialEq)]
pub struct TsLabel {
    pub key: String,
    pub value: String,
}

/// Configuration for creating or altering a TimeSeries key.
///
/// All fields are optional; unset fields are omitted from the command.
#[derive(Debug, Clone, Default)]
pub struct TsKeyConfig {
    pub retention_ms: Option<u64>,
    pub encoding: Option<TsEncoding>,
    pub chunk_size: Option<u64>,
    pub duplicate_policy: Option<TsDuplicatePolicy>,
    pub labels: Vec<TsLabel>,
}

impl TsKeyConfig {
    /// Create an empty configuration (all fields unset).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the retention period in milliseconds.
    pub fn retention(mut self, ms: u64) -> Self {
        self.retention_ms = Some(ms);
        self
    }

    /// Set the storage encoding.
    pub fn encoding(mut self, enc: TsEncoding) -> Self {
        self.encoding = Some(enc);
        self
    }

    /// Set the chunk size in bytes.
    pub fn chunk_size(mut self, bytes: u64) -> Self {
        self.chunk_size = Some(bytes);
        self
    }

    /// Set the duplicate-sample policy.
    pub fn duplicate_policy(mut self, policy: TsDuplicatePolicy) -> Self {
        self.duplicate_policy = Some(policy);
        self
    }

    /// Add a label key-value pair.
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push(TsLabel {
            key: key.into(),
            value: value.into(),
        });
        self
    }
}

/// Options for [`TimeSeriesClient::incrby`] and [`TimeSeriesClient::decrby`].
#[derive(Debug, Clone, Default)]
pub struct TsIncrOptions {
    pub timestamp: Option<TsTimestamp>,
    pub retention_ms: Option<u64>,
    pub labels: Vec<TsLabel>,
}

impl TsIncrOptions {
    /// Create empty options (all fields unset).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set an explicit timestamp.
    pub fn timestamp(mut self, ts: impl Into<TsTimestamp>) -> Self {
        self.timestamp = Some(ts.into());
        self
    }

    /// Set the retention period in milliseconds.
    pub fn retention(mut self, ms: u64) -> Self {
        self.retention_ms = Some(ms);
        self
    }

    /// Add a label key-value pair.
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push(TsLabel {
            key: key.into(),
            value: value.into(),
        });
        self
    }
}

/// A single result entry returned by `TS.MRANGE`, `TS.MREVRANGE`, or
/// `TS.MGET` — one per matched key.
#[derive(Debug, Clone)]
pub struct TsKeyResult {
    pub key: String,
    pub labels: Vec<TsLabel>,
    pub samples: Vec<TsSample>,
}

/// Typed statistics from `TS.INFO`.
#[derive(Debug, Clone, Default)]
pub struct TsInfoResult {
    pub total_samples: i64,
    pub memory_usage: i64,
    pub first_timestamp: i64,
    pub last_timestamp: i64,
    pub retention_time: i64,
    pub chunk_count: i64,
    pub chunk_size: i64,
    pub duplicate_policy: Option<String>,
    pub labels: Vec<TsLabel>,
}

// ---------------------------------------------------------------------------
// Query builders
// ---------------------------------------------------------------------------

/// Range query parameters for [`TimeSeriesClient::range`] and
/// [`TimeSeriesClient::revrange`].
///
/// `from` and `to` are either millisecond timestamps or the special strings
/// `"-"` (smallest possible timestamp) and `"+"` (largest possible timestamp).
#[derive(Debug, Clone)]
pub struct TsRangeQuery {
    pub(crate) from: String,
    pub(crate) to: String,
    pub(crate) latest: bool,
    pub(crate) filter_by_ts: Vec<i64>,
    pub(crate) filter_by_value: Option<(f64, f64)>,
    pub(crate) count: Option<i64>,
    pub(crate) aggregation: Option<(TsAggregation, i64)>,
}

impl TsRangeQuery {
    /// Build a query for the given time range.
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
            latest: false,
            filter_by_ts: Vec::new(),
            filter_by_value: None,
            count: None,
            aggregation: None,
        }
    }

    /// Query the entire time range (`"-"` to `"+"`).
    pub fn all() -> Self {
        Self::new("-", "+")
    }

    /// Include the latest (possibly un-compacted) bucket.
    pub fn latest(mut self) -> Self {
        self.latest = true;
        self
    }

    /// Filter results to only these explicit timestamps.
    pub fn filter_by_ts(mut self, timestamps: impl IntoIterator<Item = i64>) -> Self {
        self.filter_by_ts = timestamps.into_iter().collect();
        self
    }

    /// Filter results to samples within the given value range.
    pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
        self.filter_by_value = Some((min, max));
        self
    }

    /// Limit the number of returned samples.
    pub fn count(mut self, n: i64) -> Self {
        self.count = Some(n);
        self
    }

    /// Apply an aggregation with the given bucket size in milliseconds.
    pub fn aggregate(mut self, agg: TsAggregation, bucket_ms: i64) -> Self {
        self.aggregation = Some((agg, bucket_ms));
        self
    }
}

/// Multi-key range query parameters for [`TimeSeriesClient::mrange`] and
/// [`TimeSeriesClient::mrevrange`].
#[derive(Debug, Clone)]
pub struct TsMRangeQuery {
    pub(crate) range: TsRangeQuery,
    pub(crate) filters: Vec<String>,
    pub(crate) withlabels: bool,
}

impl TsMRangeQuery {
    /// Build a multi-key query. At least one `filter` expression is required.
    pub fn new(range: TsRangeQuery, filter: impl Into<String>) -> Self {
        Self {
            range,
            filters: vec![filter.into()],
            withlabels: false,
        }
    }

    /// Add another filter expression (all are ANDed by the server).
    pub fn filter(mut self, expr: impl Into<String>) -> Self {
        self.filters.push(expr.into());
        self
    }

    /// Include labels in the response.
    pub fn withlabels(mut self) -> Self {
        self.withlabels = true;
        self
    }
}

// ---------------------------------------------------------------------------
// Response-parsing helpers
// ---------------------------------------------------------------------------

fn frame_to_i64(frame: Frame) -> Result<i64, RedisError> {
    match frame {
        Frame::Integer(n) => Ok(n),
        Frame::BulkString(Some(b)) => {
            let s = std::str::from_utf8(&b).map_err(|_| RedisError::TypeMismatch {
                expected: "integer as UTF-8",
            })?;
            s.parse::<i64>().map_err(|_| RedisError::TypeMismatch {
                expected: "integer",
            })
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "integer",
            actual: format!("{other:?}"),
        }),
    }
}

fn frame_to_f64(frame: Frame) -> Result<f64, RedisError> {
    match frame {
        Frame::BulkString(Some(b)) => {
            let s = std::str::from_utf8(&b).map_err(|_| RedisError::TypeMismatch {
                expected: "f64 as UTF-8",
            })?;
            s.parse::<f64>()
                .map_err(|_| RedisError::TypeMismatch { expected: "f64" })
        }
        Frame::Integer(n) => Ok(n as f64),
        other => Err(RedisError::UnexpectedResponse {
            expected: "bulk string (f64)",
            actual: format!("{other:?}"),
        }),
    }
}

fn frame_to_string(frame: Frame) -> Result<String, RedisError> {
    match frame {
        Frame::BulkString(Some(b)) => {
            String::from_utf8(b.into()).map_err(|_| RedisError::TypeMismatch {
                expected: "UTF-8 string",
            })
        }
        Frame::SimpleString(b) => {
            String::from_utf8(b.into()).map_err(|_| RedisError::TypeMismatch {
                expected: "UTF-8 string",
            })
        }
        Frame::BulkString(None) => Ok(String::new()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "bulk/simple string",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse a 2-element `[timestamp, value]` array into a `TsSample`.
fn parse_sample(frame: Frame) -> Result<TsSample, RedisError> {
    match frame {
        Frame::Array(Some(mut elems)) if elems.len() == 2 => {
            let value = frame_to_f64(elems.pop().unwrap())?;
            let timestamp = frame_to_i64(elems.pop().unwrap())?;
            Ok(TsSample { timestamp, value })
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "[timestamp, value] array",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse an array-of-arrays into a `Vec<TsSample>`.
fn parse_samples(frame: Frame) -> Result<Vec<TsSample>, RedisError> {
    match frame {
        Frame::Array(Some(frames)) => frames.into_iter().map(parse_sample).collect(),
        Frame::Array(None) => Ok(Vec::new()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "array of [timestamp, value] arrays",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse `TS.GET` response: `[ts, value]` (sample) or `[]` (empty).
fn parse_get(frame: Frame) -> Result<Option<TsSample>, RedisError> {
    match frame {
        Frame::Array(Some(elems)) if elems.is_empty() => Ok(None),
        Frame::Array(None) => Ok(None),
        Frame::Array(Some(mut elems)) if elems.len() == 2 => {
            let value = frame_to_f64(elems.pop().unwrap())?;
            let timestamp = frame_to_i64(elems.pop().unwrap())?;
            Ok(Some(TsSample { timestamp, value }))
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "[] or [timestamp, value]",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse a `[[label_k, label_v], ...]` frame into `Vec<TsLabel>`.
fn parse_labels(frame: Frame) -> Result<Vec<TsLabel>, RedisError> {
    match frame {
        Frame::Array(Some(frames)) => frames
            .into_iter()
            .map(|f| match f {
                Frame::Array(Some(mut kv)) if kv.len() == 2 => {
                    let value = frame_to_string(kv.pop().unwrap())?;
                    let key = frame_to_string(kv.pop().unwrap())?;
                    Ok(TsLabel { key, value })
                }
                other => Err(RedisError::UnexpectedResponse {
                    expected: "[label_key, label_value] pair",
                    actual: format!("{other:?}"),
                }),
            })
            .collect(),
        Frame::Array(None) => Ok(Vec::new()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "labels array",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse one entry of TS.MRANGE / TS.MREVRANGE: `[key, labels_arr, samples_arr]`.
fn parse_mrange_entry(frame: Frame) -> Result<TsKeyResult, RedisError> {
    match frame {
        Frame::Array(Some(mut elems)) if elems.len() == 3 => {
            let samples_frame = elems.pop().unwrap();
            let labels_frame = elems.pop().unwrap();
            let key_frame = elems.pop().unwrap();

            let key = frame_to_string(key_frame)?;
            let labels = parse_labels(labels_frame)?;
            let samples = parse_samples(samples_frame)?;
            Ok(TsKeyResult {
                key,
                labels,
                samples,
            })
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "[key, labels, samples] entry",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse TS.MRANGE / TS.MREVRANGE response.
fn parse_mrange(frame: Frame) -> Result<Vec<TsKeyResult>, RedisError> {
    match frame {
        Frame::Array(Some(entries)) => entries.into_iter().map(parse_mrange_entry).collect(),
        Frame::Array(None) => Ok(Vec::new()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "array of mrange entries",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse one entry of TS.MGET: `[key, labels_arr, [ts, value]]`.
/// The last element is a *single* sample (or empty array), not an array of samples.
fn parse_mget_entry(frame: Frame) -> Result<TsKeyResult, RedisError> {
    match frame {
        Frame::Array(Some(mut elems)) if elems.len() == 3 => {
            let sample_frame = elems.pop().unwrap();
            let labels_frame = elems.pop().unwrap();
            let key_frame = elems.pop().unwrap();

            let key = frame_to_string(key_frame)?;
            let labels = parse_labels(labels_frame)?;
            let samples = match parse_get(sample_frame)? {
                Some(s) => vec![s],
                None => Vec::new(),
            };
            Ok(TsKeyResult {
                key,
                labels,
                samples,
            })
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "[key, labels, sample] entry",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse TS.MGET response.
fn parse_mget(frame: Frame) -> Result<Vec<TsKeyResult>, RedisError> {
    match frame {
        Frame::Array(Some(entries)) => entries.into_iter().map(parse_mget_entry).collect(),
        Frame::Array(None) => Ok(Vec::new()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "array of mget entries",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse a flat `[key_bulk, value, key_bulk, value, ...]` TS.INFO response.
fn parse_info(frame: Frame) -> Result<TsInfoResult, RedisError> {
    let pairs = match frame {
        Frame::Array(Some(frames)) => frames,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "flat key-value array from TS.INFO",
                actual: format!("{other:?}"),
            });
        }
    };

    if pairs.len() % 2 != 0 {
        return Err(RedisError::UnexpectedResponse {
            expected: "even-length array from TS.INFO",
            actual: format!("odd length: {}", pairs.len()),
        });
    }

    let mut result = TsInfoResult::default();
    let mut iter = pairs.into_iter();

    while let (Some(key_frame), Some(val_frame)) = (iter.next(), iter.next()) {
        let key = frame_to_string(key_frame)?;
        match key.as_str() {
            "totalSamples" => result.total_samples = frame_to_i64(val_frame)?,
            "memoryUsage" => result.memory_usage = frame_to_i64(val_frame)?,
            "firstTimestamp" => result.first_timestamp = frame_to_i64(val_frame)?,
            "lastTimestamp" => result.last_timestamp = frame_to_i64(val_frame)?,
            "retentionTime" => result.retention_time = frame_to_i64(val_frame)?,
            "chunkCount" => result.chunk_count = frame_to_i64(val_frame)?,
            "chunkSize" => result.chunk_size = frame_to_i64(val_frame)?,
            "duplicatePolicy" => match val_frame {
                Frame::BulkString(None) | Frame::Null => result.duplicate_policy = None,
                f => result.duplicate_policy = Some(frame_to_string(f)?),
            },
            "labels" => {
                result.labels = parse_labels(val_frame)?;
            }
            // Unknown fields are silently ignored to allow forward compatibility.
            _ => {}
        }
    }

    Ok(result)
}

/// Parse TS.MADD response: an array of integers or errors.
fn parse_madd(frame: Frame) -> Result<Vec<Result<i64, RedisError>>, RedisError> {
    match frame {
        Frame::Array(Some(frames)) => Ok(frames
            .into_iter()
            .map(|f| match f {
                Frame::Integer(n) => Ok(n),
                Frame::Error(msg) => {
                    let s = String::from_utf8_lossy(&msg).into_owned();
                    Err(RedisError::Redis(s))
                }
                other => Err(RedisError::UnexpectedResponse {
                    expected: "integer or error",
                    actual: format!("{other:?}"),
                }),
            })
            .collect()),
        Frame::Array(None) => Ok(Vec::new()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "array of integers/errors",
            actual: format!("{other:?}"),
        }),
    }
}

// ---------------------------------------------------------------------------
// TimeSeriesClient
// ---------------------------------------------------------------------------

/// High-level client for RedisTimeSeries operations.
///
/// Wraps any [`RedisExecutor`] and exposes typed sample-append, range-query,
/// and key-management operations. All response parsing is handled internally;
/// callers work with [`TsSample`], [`TsKeyResult`], and [`TsInfoResult`] rather
/// than raw [`Frame`] values.
///
/// Accepts both owned executors (e.g. `MultiplexedClient`, which is `Clone`) and
/// mutable references (e.g. `&mut RedisClient`), thanks to the blanket
/// `impl RedisExecutor for &mut C`.
pub struct TimeSeriesClient<C> {
    conn: C,
}

impl<C: RedisExecutor> TimeSeriesClient<C> {
    /// Create a new [`TimeSeriesClient`] wrapping the given executor.
    pub fn new(conn: C) -> Self {
        Self { conn }
    }

    // -----------------------------------------------------------------------
    // Key management
    // -----------------------------------------------------------------------

    /// Create a new TimeSeries key with the given configuration.
    pub async fn create(&mut self, key: &str, config: TsKeyConfig) -> Result<(), RedisError> {
        let mut cmd = TsCreate::new(key);
        if let Some(ms) = config.retention_ms {
            cmd = cmd.retention(ms);
        }
        if let Some(enc) = config.encoding {
            cmd = cmd.encoding(enc);
        }
        if let Some(cs) = config.chunk_size {
            cmd = cmd.chunk_size(cs);
        }
        if let Some(dp) = config.duplicate_policy {
            cmd = cmd.duplicate_policy(dp);
        }
        for lbl in &config.labels {
            cmd = cmd.label(lbl.key.clone(), lbl.value.clone());
        }
        self.conn.execute(cmd).await
    }

    /// Alter an existing TimeSeries key's configuration.
    pub async fn alter(&mut self, key: &str, config: TsKeyConfig) -> Result<(), RedisError> {
        let mut cmd = TsAlter::new(key);
        if let Some(ms) = config.retention_ms {
            cmd = cmd.retention(ms);
        }
        if let Some(cs) = config.chunk_size {
            cmd = cmd.chunk_size(cs);
        }
        if let Some(dp) = config.duplicate_policy {
            cmd = cmd.duplicate_policy(dp);
        }
        for lbl in &config.labels {
            cmd = cmd.label(lbl.key.clone(), lbl.value.clone());
        }
        self.conn.execute(cmd).await
    }

    /// Delete all samples in the time range `[from, to]` (inclusive).
    /// Returns the number of samples deleted.
    pub async fn del_range(&mut self, key: &str, from: i64, to: i64) -> Result<i64, RedisError> {
        self.conn.execute(TsDel::new(key, from, to)).await
    }

    // -----------------------------------------------------------------------
    // Writing samples
    // -----------------------------------------------------------------------

    /// Append a sample to a TimeSeries key. Returns the stored timestamp.
    ///
    /// Pass [`TsTimestamp::Auto`] to let the server assign the current time.
    pub async fn add(
        &mut self,
        key: &str,
        timestamp: impl Into<TsTimestamp>,
        value: f64,
    ) -> Result<i64, RedisError> {
        self.conn
            .execute(TsAdd::new(key, timestamp.into(), value))
            .await
    }

    /// Append a sample, also supplying creation-time configuration.
    ///
    /// If the key does not yet exist, it will be created with the given
    /// `config`. Useful for the "upsert" pattern.
    pub async fn add_with_config(
        &mut self,
        key: &str,
        timestamp: impl Into<TsTimestamp>,
        value: f64,
        config: TsKeyConfig,
    ) -> Result<i64, RedisError> {
        let mut cmd = TsAdd::new(key, timestamp.into(), value);
        if let Some(ms) = config.retention_ms {
            cmd = cmd.retention(ms);
        }
        if let Some(enc) = config.encoding {
            cmd = cmd.encoding(enc);
        }
        if let Some(cs) = config.chunk_size {
            cmd = cmd.chunk_size(cs);
        }
        if let Some(dp) = config.duplicate_policy {
            cmd = cmd.on_duplicate(dp);
        }
        for lbl in &config.labels {
            cmd = cmd.label(lbl.key.clone(), lbl.value.clone());
        }
        self.conn.execute(cmd).await
    }

    /// Append samples to multiple keys in a single round-trip.
    ///
    /// Returns one `Result<i64, RedisError>` per input sample. Per-sample
    /// errors (e.g. duplicate-policy violations) are returned as `Err` items
    /// rather than propagating the whole call.
    pub async fn madd(
        &mut self,
        samples: &[(&str, TsTimestamp, f64)],
    ) -> Result<Vec<Result<i64, RedisError>>, RedisError> {
        let mut cmd = TsMAdd::new();
        for &(key, ts, value) in samples {
            cmd = cmd.sample(key, ts, value);
        }
        let raw = self.conn.execute(cmd).await?;
        parse_madd(Frame::Array(Some(raw)))
    }

    /// Increment the value of the latest sample (or create the key).
    /// Returns the timestamp of the updated sample.
    pub async fn incrby(
        &mut self,
        key: &str,
        value: f64,
        options: TsIncrOptions,
    ) -> Result<i64, RedisError> {
        let mut cmd = TsIncrBy::new(key, value);
        if let Some(ts) = options.timestamp {
            cmd = cmd.timestamp(ts);
        }
        if let Some(ms) = options.retention_ms {
            cmd = cmd.retention(ms);
        }
        for lbl in &options.labels {
            cmd = cmd.label(lbl.key.clone(), lbl.value.clone());
        }
        self.conn.execute(cmd).await
    }

    /// Decrement the value of the latest sample (or create the key).
    /// Returns the timestamp of the updated sample.
    pub async fn decrby(
        &mut self,
        key: &str,
        value: f64,
        options: TsIncrOptions,
    ) -> Result<i64, RedisError> {
        let mut cmd = TsDecrBy::new(key, value);
        if let Some(ts) = options.timestamp {
            cmd = cmd.timestamp(ts);
        }
        if let Some(ms) = options.retention_ms {
            cmd = cmd.retention(ms);
        }
        for lbl in &options.labels {
            cmd = cmd.label(lbl.key.clone(), lbl.value.clone());
        }
        self.conn.execute(cmd).await
    }

    // -----------------------------------------------------------------------
    // Reading samples
    // -----------------------------------------------------------------------

    /// Return the last sample of a key, or `None` if the key has no samples.
    pub async fn get(&mut self, key: &str) -> Result<Option<TsSample>, RedisError> {
        let raw = self.conn.execute(TsGet::new(key)).await?;
        parse_get(raw)
    }

    /// Return the last sample, including the latest un-compacted bucket.
    pub async fn get_latest(&mut self, key: &str) -> Result<Option<TsSample>, RedisError> {
        let raw = self.conn.execute(TsGet::new(key).latest()).await?;
        parse_get(raw)
    }

    /// Query samples in chronological order.
    pub async fn range(
        &mut self,
        key: &str,
        query: TsRangeQuery,
    ) -> Result<Vec<TsSample>, RedisError> {
        let mut cmd = TsRange::new(key, query.from, query.to);
        if query.latest {
            cmd = cmd.latest();
        }
        if !query.filter_by_ts.is_empty() {
            cmd = cmd.filter_by_ts(query.filter_by_ts);
        }
        if let Some((min, max)) = query.filter_by_value {
            cmd = cmd.filter_by_value(min, max);
        }
        if let Some(n) = query.count {
            cmd = cmd.count(n);
        }
        if let Some((agg, bucket)) = query.aggregation {
            cmd = cmd.aggregation(agg, bucket);
        }
        let raw = self.conn.execute(cmd).await?;
        parse_samples(raw)
    }

    /// Query samples in reverse chronological order.
    pub async fn revrange(
        &mut self,
        key: &str,
        query: TsRangeQuery,
    ) -> Result<Vec<TsSample>, RedisError> {
        let mut cmd = TsRevRange::new(key, query.from, query.to);
        if query.latest {
            cmd = cmd.latest();
        }
        if !query.filter_by_ts.is_empty() {
            cmd = cmd.filter_by_ts(query.filter_by_ts);
        }
        if let Some((min, max)) = query.filter_by_value {
            cmd = cmd.filter_by_value(min, max);
        }
        if let Some(n) = query.count {
            cmd = cmd.count(n);
        }
        if let Some((agg, bucket)) = query.aggregation {
            cmd = cmd.aggregation(agg, bucket);
        }
        let raw = self.conn.execute(cmd).await?;
        parse_samples(raw)
    }

    /// Query samples across multiple keys in chronological order.
    pub async fn mrange(&mut self, query: TsMRangeQuery) -> Result<Vec<TsKeyResult>, RedisError> {
        let first_filter = query.filters.first().cloned().unwrap_or_default();
        let mut cmd = TsMRange::new(query.range.from, query.range.to, first_filter);
        for f in query.filters.iter().skip(1) {
            cmd = cmd.filter(f.clone());
        }
        if query.range.latest {
            cmd = cmd.latest();
        }
        if query.withlabels {
            cmd = cmd.withlabels();
        }
        if !query.range.filter_by_ts.is_empty() {
            cmd = cmd.filter_by_ts(query.range.filter_by_ts);
        }
        if let Some((min, max)) = query.range.filter_by_value {
            cmd = cmd.filter_by_value(min, max);
        }
        if let Some(n) = query.range.count {
            cmd = cmd.count(n);
        }
        if let Some((agg, bucket)) = query.range.aggregation {
            cmd = cmd.aggregation(agg, bucket);
        }
        let raw = self.conn.execute(cmd).await?;
        parse_mrange(raw)
    }

    /// Query samples across multiple keys in reverse chronological order.
    pub async fn mrevrange(
        &mut self,
        query: TsMRangeQuery,
    ) -> Result<Vec<TsKeyResult>, RedisError> {
        let first_filter = query.filters.first().cloned().unwrap_or_default();
        let mut cmd = TsMRevRange::new(query.range.from, query.range.to, first_filter);
        for f in query.filters.iter().skip(1) {
            cmd = cmd.filter(f.clone());
        }
        if query.range.latest {
            cmd = cmd.latest();
        }
        if query.withlabels {
            cmd = cmd.withlabels();
        }
        if !query.range.filter_by_ts.is_empty() {
            cmd = cmd.filter_by_ts(query.range.filter_by_ts);
        }
        if let Some((min, max)) = query.range.filter_by_value {
            cmd = cmd.filter_by_value(min, max);
        }
        if let Some(n) = query.range.count {
            cmd = cmd.count(n);
        }
        if let Some((agg, bucket)) = query.range.aggregation {
            cmd = cmd.aggregation(agg, bucket);
        }
        let raw = self.conn.execute(cmd).await?;
        parse_mrange(raw)
    }

    /// Return the last sample of every key matching `filter`.
    pub async fn mget(
        &mut self,
        filter: &str,
        withlabels: bool,
    ) -> Result<Vec<TsKeyResult>, RedisError> {
        let mut cmd = TsMGet::new(filter);
        if withlabels {
            cmd = cmd.withlabels();
        }
        let raw = self.conn.execute(cmd).await?;
        parse_mget(raw)
    }

    // -----------------------------------------------------------------------
    // Query and info
    // -----------------------------------------------------------------------

    /// Return all key names matching the given filter expression.
    pub async fn query_index(&mut self, filter: &str) -> Result<Vec<String>, RedisError> {
        let bytes = self.conn.execute(TsQueryIndex::new(filter)).await?;
        bytes
            .into_iter()
            .map(|b| {
                String::from_utf8(b.into()).map_err(|_| RedisError::TypeMismatch {
                    expected: "UTF-8 key name",
                })
            })
            .collect()
    }

    /// Return typed statistics for the given key.
    pub async fn info(&mut self, key: &str) -> Result<TsInfoResult, RedisError> {
        let raw = self.conn.execute(TsInfo::new(key)).await?;
        parse_info(raw)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use redis_tower_core::{Command, Frame};
    use std::collections::VecDeque;
    use std::future::Future;

    struct MockRedis {
        responses: VecDeque<Frame>,
    }

    impl MockRedis {
        fn new(responses: Vec<Frame>) -> Self {
            Self {
                responses: VecDeque::from(responses),
            }
        }
    }

    impl RedisExecutor for MockRedis {
        fn execute<Cmd: Command>(
            &mut self,
            cmd: Cmd,
        ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
            let frame = self.responses.pop_front().unwrap_or(Frame::Null);
            async move { cmd.parse_response(frame) }
        }
    }

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Some(Bytes::from(s.to_owned())))
    }

    fn int(n: i64) -> Frame {
        Frame::Integer(n)
    }

    // --- get ----------------------------------------------------------------

    #[tokio::test]
    async fn get_returns_none_for_empty_series() {
        // TS.GET on a key with no samples returns an empty array.
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![]))]);
        let mut ts = TimeSeriesClient::new(&mut mock);
        let result = ts.get("sensors:temp").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn get_returns_sample() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            int(1_700_000_000_000),
            bulk("21.5"),
        ]))]);
        let mut ts = TimeSeriesClient::new(&mut mock);
        let result = ts.get("sensors:temp").await.unwrap();
        assert_eq!(
            result,
            Some(TsSample {
                timestamp: 1_700_000_000_000,
                value: 21.5,
            })
        );
    }

    // --- range --------------------------------------------------------------

    #[tokio::test]
    async fn range_returns_samples() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            Frame::Array(Some(vec![int(1_000), bulk("10.0")])),
            Frame::Array(Some(vec![int(2_000), bulk("20.0")])),
        ]))]);
        let mut ts = TimeSeriesClient::new(&mut mock);
        let samples = ts.range("key", TsRangeQuery::all()).await.unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(
            samples[0],
            TsSample {
                timestamp: 1_000,
                value: 10.0
            }
        );
        assert_eq!(
            samples[1],
            TsSample {
                timestamp: 2_000,
                value: 20.0
            }
        );
    }

    #[tokio::test]
    async fn range_returns_empty_for_null_array() {
        let mut mock = MockRedis::new(vec![Frame::Array(None)]);
        let mut ts = TimeSeriesClient::new(&mut mock);
        let samples = ts.range("key", TsRangeQuery::all()).await.unwrap();
        assert!(samples.is_empty());
    }

    // --- mrange -------------------------------------------------------------

    #[tokio::test]
    async fn mrange_parses_multiple_keys() {
        // TS.MRANGE returns [[key, [[lk, lv], ...], [[ts, val], ...]], ...]
        let entry = Frame::Array(Some(vec![
            bulk("sensors:temp"),
            Frame::Array(Some(vec![Frame::Array(Some(vec![
                bulk("sensor"),
                bulk("temperature"),
            ]))])),
            Frame::Array(Some(vec![Frame::Array(Some(vec![
                int(1_700_000_000_000),
                bulk("22.0"),
            ]))])),
        ]));
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![entry]))]);
        let mut ts = TimeSeriesClient::new(&mut mock);
        let results = ts
            .mrange(TsMRangeQuery::new(TsRangeQuery::all(), "sensor=temperature").withlabels())
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "sensors:temp");
        assert_eq!(results[0].labels.len(), 1);
        assert_eq!(results[0].labels[0].key, "sensor");
        assert_eq!(results[0].labels[0].value, "temperature");
        assert_eq!(results[0].samples.len(), 1);
        assert_eq!(results[0].samples[0].value, 22.0);
    }

    // --- madd ---------------------------------------------------------------

    #[tokio::test]
    async fn madd_returns_timestamps() {
        // TS.MADD returns an array of integers.
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![int(1_000), int(2_000)]))]);
        let mut ts = TimeSeriesClient::new(&mut mock);
        let samples = vec![
            ("key1", TsTimestamp::Value(1_000), 1.0),
            ("key2", TsTimestamp::Value(2_000), 2.0),
        ];
        let results = ts.madd(&samples).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap(), &1_000i64);
        assert_eq!(results[1].as_ref().unwrap(), &2_000i64);
    }

    // --- info ---------------------------------------------------------------

    #[tokio::test]
    async fn info_parses_flat_kv_array() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            bulk("totalSamples"),
            int(42),
            bulk("memoryUsage"),
            int(4096),
            bulk("firstTimestamp"),
            int(1_000),
            bulk("lastTimestamp"),
            int(9_000),
            bulk("retentionTime"),
            int(3_600_000),
            bulk("chunkCount"),
            int(1),
            bulk("chunkSize"),
            int(4096),
            bulk("duplicatePolicy"),
            Frame::BulkString(Some(Bytes::from("LAST"))),
            bulk("labels"),
            Frame::Array(Some(vec![Frame::Array(Some(vec![
                bulk("sensor"),
                bulk("temperature"),
            ]))])),
        ]))]);
        let mut ts = TimeSeriesClient::new(&mut mock);
        let info = ts.info("sensors:temp").await.unwrap();
        assert_eq!(info.total_samples, 42);
        assert_eq!(info.memory_usage, 4096);
        assert_eq!(info.first_timestamp, 1_000);
        assert_eq!(info.last_timestamp, 9_000);
        assert_eq!(info.retention_time, 3_600_000);
        assert_eq!(info.chunk_count, 1);
        assert_eq!(info.chunk_size, 4096);
        assert_eq!(info.duplicate_policy, Some("LAST".to_string()));
        assert_eq!(info.labels.len(), 1);
        assert_eq!(info.labels[0].key, "sensor");
        assert_eq!(info.labels[0].value, "temperature");
    }

    #[tokio::test]
    async fn info_handles_null_duplicate_policy() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            bulk("totalSamples"),
            int(0),
            bulk("memoryUsage"),
            int(0),
            bulk("firstTimestamp"),
            int(0),
            bulk("lastTimestamp"),
            int(0),
            bulk("retentionTime"),
            int(0),
            bulk("chunkCount"),
            int(0),
            bulk("chunkSize"),
            int(0),
            bulk("duplicatePolicy"),
            Frame::Null,
            bulk("labels"),
            Frame::Array(None),
        ]))]);
        let mut ts = TimeSeriesClient::new(&mut mock);
        let info = ts.info("key").await.unwrap();
        assert_eq!(info.duplicate_policy, None);
        assert!(info.labels.is_empty());
    }

    // --- del_range ----------------------------------------------------------

    #[tokio::test]
    async fn del_range_returns_count() {
        let mut mock = MockRedis::new(vec![int(5)]);
        let mut ts = TimeSeriesClient::new(&mut mock);
        let n = ts.del_range("key", 0, 9_999).await.unwrap();
        assert_eq!(n, 5);
    }

    // --- create -------------------------------------------------------------

    #[tokio::test]
    async fn create_sends_ok() {
        let mut mock = MockRedis::new(vec![Frame::SimpleString(Bytes::from("OK"))]);
        let mut ts = TimeSeriesClient::new(&mut mock);
        ts.create(
            "sensors:temp",
            TsKeyConfig::new()
                .retention(3_600_000)
                .label("sensor", "temperature"),
        )
        .await
        .unwrap();
    }

    // --- query_index --------------------------------------------------------

    #[tokio::test]
    async fn query_index_returns_key_names() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("sensors:temp"))),
            Frame::BulkString(Some(Bytes::from("sensors:humidity"))),
        ]))]);
        let mut ts = TimeSeriesClient::new(&mut mock);
        let keys = ts.query_index("sensor!=").await.unwrap();
        assert_eq!(keys, vec!["sensors:temp", "sensors:humidity"]);
    }
}
