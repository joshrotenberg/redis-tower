//! RedisTimeSeries module - Time-series data storage with automatic downsampling
//!
//! This module provides complete RedisTimeSeries functionality with ergonomic, type-safe APIs.
//!
//! # Key Design: Enum-Based Aggregations and Policies
//!
//! RedisTimeSeries has complex aggregation options and duplicate policies. Instead of forcing
//! users to work with strings, we provide typed enums that give compile-time safety.
//!
//! # Examples
//!
//! ## Create and add samples
//! ```no_run
//! use redis_tower::modules::timeseries::{TsCreate, TsAdd, DuplicatePolicy, Encoding};
//! use redis_tower::RedisClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("localhost:6379").await?;
//!
//! // Create time series with options
//! client.call(TsCreate::new("temperature:living_room")
//!     .retention(86400000) // 24 hours in ms
//!     .duplicate_policy(DuplicatePolicy::Last)
//!     .label("room", "living")
//!     .label("sensor", "temp"))
//!     .await?;
//!
//! // Add sample with current timestamp
//! let timestamp = client.call(TsAdd::new("temperature:living_room", "*", 23.5)).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Query with aggregation
//! ```no_run
//! use redis_tower::modules::timeseries::{TsRange, Aggregator};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let client = redis_tower::RedisClient::connect("localhost:6379").await?;
//! // Get hourly averages
//! let data = client.call(TsRange::new("temperature:living_room", "-", "+")
//!     .aggregation(Aggregator::Avg, 3600000)) // 1 hour buckets
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::codec::Frame;
use crate::commands::Command;
use crate::read_preference::ReadOnly;
use crate::types::RedisError;
use bytes::Bytes;
use std::collections::HashMap;

// ============================================================================
// ENUMS - Type-safe options
// ============================================================================

/// Encoding format for time series samples
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// Compressed encoding (default, ~90% memory savings)
    Compressed,
    /// Uncompressed encoding (raw samples)
    Uncompressed,
}

/// Policy for handling duplicate timestamps
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuplicatePolicy {
    /// Block duplicate timestamps with error
    Block,
    /// Keep first value, ignore new
    First,
    /// Keep last value, override old
    Last,
    /// Keep minimum value
    Min,
    /// Keep maximum value
    Max,
    /// Sum old and new values
    Sum,
}

/// Aggregation function for downsampling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregator {
    /// Arithmetic mean
    Avg,
    /// Sum of values
    Sum,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Range (max - min)
    Range,
    /// Count of values
    Count,
    /// First value (by timestamp)
    First,
    /// Last value (by timestamp)
    Last,
    /// Population standard deviation
    StdP,
    /// Sample standard deviation
    StdS,
    /// Population variance
    VarP,
    /// Sample variance
    VarS,
    /// Time-weighted average
    Twa,
}

/// Bucket timestamp reporting mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketTimestamp {
    /// Bucket start time (default)
    Start,
    /// Bucket end time
    End,
    /// Bucket mid time
    Mid,
}

// ============================================================================
// RESPONSE TYPES
// ============================================================================

/// Sample (timestamp, value) pair
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    /// Unix timestamp in milliseconds
    pub timestamp: i64,
    /// Sample value
    pub value: f64,
}

/// Time series information from TS.INFO
#[derive(Debug, Clone, PartialEq)]
pub struct TimeSeriesInfo {
    /// Total number of samples in the series
    pub total_samples: i64,
    /// Memory usage in bytes
    pub memory_usage: i64,
    /// Timestamp of the first sample
    pub first_timestamp: i64,
    /// Timestamp of the last sample
    pub last_timestamp: i64,
    /// Retention period in milliseconds
    pub retention_time: i64,
    /// Number of memory chunks used
    pub chunk_count: i64,
    /// Chunk size in bytes
    pub chunk_size: i64,
    /// Duplicate sample policy
    pub duplicate_policy: Option<String>,
    /// Series labels (metadata)
    pub labels: HashMap<String, String>,
    /// Source key if this is a compaction series
    pub source_key: Option<String>,
    /// Compaction rules for downsampling
    pub rules: Vec<CompactionRule>,
}

/// Downsampling/compaction rule
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionRule {
    /// Destination key for compacted data
    pub dest_key: String,
    /// Bucket duration in milliseconds
    pub bucket_duration: i64,
    /// Aggregation function name
    pub aggregator: String,
}

/// Result from TS.MGET - multiple series latest values
#[derive(Debug, Clone, PartialEq)]
pub struct MGetResult {
    /// Time series key
    pub key: String,
    /// Series labels
    pub labels: HashMap<String, String>,
    /// Latest sample (None if series is empty)
    pub sample: Option<Sample>,
}

/// Result from TS.MRANGE/MREVRANGE - multiple series ranges
#[derive(Debug, Clone, PartialEq)]
pub struct MRangeResult {
    /// Time series key
    pub key: String,
    /// Series labels
    pub labels: HashMap<String, String>,
    /// Samples in the requested range
    pub samples: Vec<Sample>,
}

// ============================================================================
// TS.CREATE - Create time series
// ============================================================================

/// TS.CREATE - Create a new time series
///
/// Available since: RedisTimeSeries 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::timeseries::{TsCreate, DuplicatePolicy, Encoding};
///
/// // Create with all options
/// let cmd = TsCreate::new("temperature:bedroom")
///     .retention(86400000) // 24 hours
///     .encoding(Encoding::Compressed)
///     .chunk_size(4096)
///     .duplicate_policy(DuplicatePolicy::Last)
///     .label("room", "bedroom")
///     .label("sensor", "temp");
/// ```
#[derive(Debug, Clone)]
pub struct TsCreate {
    key: String,
    retention: Option<i64>,
    encoding: Option<Encoding>,
    chunk_size: Option<i64>,
    duplicate_policy: Option<DuplicatePolicy>,
    ignore: Option<(i64, f64)>, // (max_time_diff, max_val_diff)
    labels: Vec<(String, String)>,
}

impl TsCreate {
    /// Create a new TS.CREATE command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            retention: None,
            encoding: None,
            chunk_size: None,
            duplicate_policy: None,
            ignore: None,
            labels: Vec::new(),
        }
    }

    /// Set retention period in milliseconds (0 = never expire)
    pub fn retention(mut self, ms: i64) -> Self {
        self.retention = Some(ms);
        self
    }

    /// Set encoding format
    pub fn encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = Some(encoding);
        self
    }

    /// Set chunk size in bytes (must be multiple of 8, range 48-1048576)
    pub fn chunk_size(mut self, size: i64) -> Self {
        self.chunk_size = Some(size);
        self
    }

    /// Set duplicate timestamp policy
    pub fn duplicate_policy(mut self, policy: DuplicatePolicy) -> Self {
        self.duplicate_policy = Some(policy);
        self
    }

    /// Set ignore policy for near-duplicate samples
    pub fn ignore(mut self, max_time_diff: i64, max_val_diff: f64) -> Self {
        self.ignore = Some((max_time_diff, max_val_diff));
        self
    }

    /// Add a label (metadata key-value pair)
    pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((name.into(), value.into()));
        self
    }

    /// Add multiple labels at once
    pub fn labels(mut self, labels: Vec<(String, String)>) -> Self {
        self.labels.extend(labels);
        self
    }
}

impl Command for TsCreate {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TS.CREATE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(retention) = self.retention {
            frames.push(Frame::BulkString(Some(Bytes::from("RETENTION"))));
            frames.push(Frame::BulkString(Some(Bytes::from(retention.to_string()))));
        }

        if let Some(encoding) = self.encoding {
            frames.push(Frame::BulkString(Some(Bytes::from("ENCODING"))));
            frames.push(Frame::BulkString(Some(Bytes::from(match encoding {
                Encoding::Compressed => "COMPRESSED",
                Encoding::Uncompressed => "UNCOMPRESSED",
            }))));
        }

        if let Some(size) = self.chunk_size {
            frames.push(Frame::BulkString(Some(Bytes::from("CHUNK_SIZE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(size.to_string()))));
        }

        if let Some(policy) = self.duplicate_policy {
            frames.push(Frame::BulkString(Some(Bytes::from("DUPLICATE_POLICY"))));
            frames.push(Frame::BulkString(Some(Bytes::from(match policy {
                DuplicatePolicy::Block => "BLOCK",
                DuplicatePolicy::First => "FIRST",
                DuplicatePolicy::Last => "LAST",
                DuplicatePolicy::Min => "MIN",
                DuplicatePolicy::Max => "MAX",
                DuplicatePolicy::Sum => "SUM",
            }))));
        }

        if let Some((time_diff, val_diff)) = self.ignore {
            frames.push(Frame::BulkString(Some(Bytes::from("IGNORE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(time_diff.to_string()))));
            frames.push(Frame::BulkString(Some(Bytes::from(val_diff.to_string()))));
        }

        if !self.labels.is_empty() {
            frames.push(Frame::BulkString(Some(Bytes::from("LABELS"))));
            for (name, value) in &self.labels {
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    name.as_bytes(),
                ))));
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    value.as_bytes(),
                ))));
            }
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// TS.ADD - Add sample
// ============================================================================

/// TS.ADD - Append a sample to a time series
///
/// Timestamp can be "*" for current server time or explicit millisecond Unix timestamp.
///
/// Available since: RedisTimeSeries 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::timeseries::{TsAdd, DuplicatePolicy};
///
/// // Add with current timestamp
/// let cmd = TsAdd::new("temp:room1", "*", 23.5);
///
/// // Add with explicit timestamp and options
/// let cmd = TsAdd::new("temp:room1", "1548149180000", 24.2)
///     .on_duplicate(DuplicatePolicy::Last)
///     .retention(86400000)
///     .label("room", "1");
/// ```
#[derive(Debug, Clone)]
pub struct TsAdd {
    key: String,
    timestamp: String,
    value: f64,
    retention: Option<i64>,
    encoding: Option<Encoding>,
    chunk_size: Option<i64>,
    duplicate_policy: Option<DuplicatePolicy>,
    on_duplicate: Option<DuplicatePolicy>,
    ignore: Option<(i64, f64)>,
    labels: Vec<(String, String)>,
}

impl TsAdd {
    /// Create a new TS.ADD command
    ///
    /// # Arguments
    /// * `key` - Time series key
    /// * `timestamp` - "*" for server time or millisecond Unix timestamp as string
    /// * `value` - Sample value
    pub fn new(key: impl Into<String>, timestamp: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            timestamp: timestamp.into(),
            value,
            retention: None,
            encoding: None,
            chunk_size: None,
            duplicate_policy: None,
            on_duplicate: None,
            ignore: None,
            labels: Vec::new(),
        }
    }

    /// Set retention (only used if creating new series)
    pub fn retention(mut self, ms: i64) -> Self {
        self.retention = Some(ms);
        self
    }

    /// Set encoding (only used if creating new series)
    pub fn encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = Some(encoding);
        self
    }

    /// Set chunk size (only used if creating new series)
    pub fn chunk_size(mut self, size: i64) -> Self {
        self.chunk_size = Some(size);
        self
    }

    /// Set duplicate policy (only used if creating new series)
    pub fn duplicate_policy(mut self, policy: DuplicatePolicy) -> Self {
        self.duplicate_policy = Some(policy);
        self
    }

    /// Override duplicate policy for this single add operation
    pub fn on_duplicate(mut self, policy: DuplicatePolicy) -> Self {
        self.on_duplicate = Some(policy);
        self
    }

    /// Set ignore policy (only used if creating new series)
    pub fn ignore(mut self, max_time_diff: i64, max_val_diff: f64) -> Self {
        self.ignore = Some((max_time_diff, max_val_diff));
        self
    }

    /// Add label (only used if creating new series)
    pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((name.into(), value.into()));
        self
    }
}

impl Command for TsAdd {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TS.ADD"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.timestamp.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.value.to_string()))),
        ];

        if let Some(retention) = self.retention {
            frames.push(Frame::BulkString(Some(Bytes::from("RETENTION"))));
            frames.push(Frame::BulkString(Some(Bytes::from(retention.to_string()))));
        }

        if let Some(encoding) = self.encoding {
            frames.push(Frame::BulkString(Some(Bytes::from("ENCODING"))));
            frames.push(Frame::BulkString(Some(Bytes::from(match encoding {
                Encoding::Compressed => "COMPRESSED",
                Encoding::Uncompressed => "UNCOMPRESSED",
            }))));
        }

        if let Some(size) = self.chunk_size {
            frames.push(Frame::BulkString(Some(Bytes::from("CHUNK_SIZE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(size.to_string()))));
        }

        if let Some(policy) = self.duplicate_policy {
            frames.push(Frame::BulkString(Some(Bytes::from("DUPLICATE_POLICY"))));
            frames.push(Frame::BulkString(Some(Bytes::from(match policy {
                DuplicatePolicy::Block => "BLOCK",
                DuplicatePolicy::First => "FIRST",
                DuplicatePolicy::Last => "LAST",
                DuplicatePolicy::Min => "MIN",
                DuplicatePolicy::Max => "MAX",
                DuplicatePolicy::Sum => "SUM",
            }))));
        }

        if let Some(policy) = self.on_duplicate {
            frames.push(Frame::BulkString(Some(Bytes::from("ON_DUPLICATE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(match policy {
                DuplicatePolicy::Block => "BLOCK",
                DuplicatePolicy::First => "FIRST",
                DuplicatePolicy::Last => "LAST",
                DuplicatePolicy::Min => "MIN",
                DuplicatePolicy::Max => "MAX",
                DuplicatePolicy::Sum => "SUM",
            }))));
        }

        if let Some((time_diff, val_diff)) = self.ignore {
            frames.push(Frame::BulkString(Some(Bytes::from("IGNORE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(time_diff.to_string()))));
            frames.push(Frame::BulkString(Some(Bytes::from(val_diff.to_string()))));
        }

        if !self.labels.is_empty() {
            frames.push(Frame::BulkString(Some(Bytes::from("LABELS"))));
            for (name, value) in &self.labels {
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    name.as_bytes(),
                ))));
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    value.as_bytes(),
                ))));
            }
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// TS.MADD - Add multiple samples
// ============================================================================

/// TS.MADD - Append samples to multiple time series
///
/// Available since: RedisTimeSeries 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::timeseries::TsMAdd;
///
/// let cmd = TsMAdd::new()
///     .add("temp:room1", "1548149180000", 23.5)
///     .add("temp:room2", "1548149180000", 24.2)
///     .add("temp:room3", "*", 22.8);
/// ```
#[derive(Debug, Clone)]
pub struct TsMAdd {
    samples: Vec<(String, String, f64)>, // (key, timestamp, value)
}

impl TsMAdd {
    /// Create a new TS.MADD command
    pub fn new() -> Self {
        Self {
            samples: Vec::new(),
        }
    }

    /// Add a sample (key, timestamp, value)
    pub fn add(mut self, key: impl Into<String>, timestamp: impl Into<String>, value: f64) -> Self {
        self.samples.push((key.into(), timestamp.into(), value));
        self
    }
}

impl Default for TsMAdd {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for TsMAdd {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("TS.MADD")))];

        for (key, timestamp, value) in &self.samples {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                timestamp.as_bytes(),
            ))));
            frames.push(Frame::BulkString(Some(Bytes::from(value.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => items
                .iter()
                .map(|f| match f {
                    Frame::Integer(n) => Ok(*n),
                    _ => Err(RedisError::UnexpectedResponse),
                })
                .collect(),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// TS.INCRBY / TS.DECRBY - Increment/Decrement and add sample
// ============================================================================

/// TS.INCRBY - Create a new sample by incrementing the latest sample value
///
/// Available since: RedisTimeSeries 1.0.0
#[derive(Debug, Clone)]
pub struct TsIncrBy {
    key: String,
    value: f64,
    timestamp: Option<String>,
    retention: Option<i64>,
    chunk_size: Option<i64>,
    duplicate_policy: Option<DuplicatePolicy>,
    labels: Vec<(String, String)>,
}

impl TsIncrBy {
    /// Create a new TS.INCRBY command
    pub fn new(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value,
            timestamp: None,
            retention: None,
            chunk_size: None,
            duplicate_policy: None,
            labels: Vec::new(),
        }
    }

    /// Set explicit timestamp (otherwise uses current time)
    pub fn timestamp(mut self, ts: impl Into<String>) -> Self {
        self.timestamp = Some(ts.into());
        self
    }

    /// Set retention period (only if series doesn't exist)
    pub fn retention(mut self, ms: i64) -> Self {
        self.retention = Some(ms);
        self
    }

    /// Set chunk size (only if series doesn't exist)
    pub fn chunk_size(mut self, size: i64) -> Self {
        self.chunk_size = Some(size);
        self
    }

    /// Set duplicate policy (only if series doesn't exist)
    pub fn duplicate_policy(mut self, policy: DuplicatePolicy) -> Self {
        self.duplicate_policy = Some(policy);
        self
    }

    /// Add label (only if series doesn't exist)
    pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((name.into(), value.into()));
        self
    }
}

impl Command for TsIncrBy {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TS.INCRBY"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.value.to_string()))),
        ];

        if let Some(ref ts) = self.timestamp {
            frames.push(Frame::BulkString(Some(Bytes::from("TIMESTAMP"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                ts.as_bytes(),
            ))));
        }

        if let Some(retention) = self.retention {
            frames.push(Frame::BulkString(Some(Bytes::from("RETENTION"))));
            frames.push(Frame::BulkString(Some(Bytes::from(retention.to_string()))));
        }

        if let Some(size) = self.chunk_size {
            frames.push(Frame::BulkString(Some(Bytes::from("CHUNK_SIZE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(size.to_string()))));
        }

        if let Some(policy) = self.duplicate_policy {
            frames.push(Frame::BulkString(Some(Bytes::from("DUPLICATE_POLICY"))));
            frames.push(Frame::BulkString(Some(Bytes::from(match policy {
                DuplicatePolicy::Block => "BLOCK",
                DuplicatePolicy::First => "FIRST",
                DuplicatePolicy::Last => "LAST",
                DuplicatePolicy::Min => "MIN",
                DuplicatePolicy::Max => "MAX",
                DuplicatePolicy::Sum => "SUM",
            }))));
        }

        if !self.labels.is_empty() {
            frames.push(Frame::BulkString(Some(Bytes::from("LABELS"))));
            for (name, value) in &self.labels {
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    name.as_bytes(),
                ))));
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    value.as_bytes(),
                ))));
            }
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TS.DECRBY - Create a new sample by decrementing the latest sample value
///
/// Available since: RedisTimeSeries 1.0.0
#[derive(Debug, Clone)]
pub struct TsDecrBy {
    key: String,
    value: f64,
    timestamp: Option<String>,
    retention: Option<i64>,
    chunk_size: Option<i64>,
    duplicate_policy: Option<DuplicatePolicy>,
    labels: Vec<(String, String)>,
}

impl TsDecrBy {
    /// Create a new TS.DECRBY command
    pub fn new(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value,
            timestamp: None,
            retention: None,
            chunk_size: None,
            duplicate_policy: None,
            labels: Vec::new(),
        }
    }

    /// Set explicit timestamp (otherwise uses current time)
    pub fn timestamp(mut self, ts: impl Into<String>) -> Self {
        self.timestamp = Some(ts.into());
        self
    }

    /// Set retention period (only if series doesn't exist)
    pub fn retention(mut self, ms: i64) -> Self {
        self.retention = Some(ms);
        self
    }

    /// Set chunk size (only if series doesn't exist)
    pub fn chunk_size(mut self, size: i64) -> Self {
        self.chunk_size = Some(size);
        self
    }

    /// Set duplicate policy (only if series doesn't exist)
    pub fn duplicate_policy(mut self, policy: DuplicatePolicy) -> Self {
        self.duplicate_policy = Some(policy);
        self
    }

    /// Add label (only if series doesn't exist)
    pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((name.into(), value.into()));
        self
    }
}

impl Command for TsDecrBy {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TS.DECRBY"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.value.to_string()))),
        ];

        if let Some(ref ts) = self.timestamp {
            frames.push(Frame::BulkString(Some(Bytes::from("TIMESTAMP"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                ts.as_bytes(),
            ))));
        }

        if let Some(retention) = self.retention {
            frames.push(Frame::BulkString(Some(Bytes::from("RETENTION"))));
            frames.push(Frame::BulkString(Some(Bytes::from(retention.to_string()))));
        }

        if let Some(size) = self.chunk_size {
            frames.push(Frame::BulkString(Some(Bytes::from("CHUNK_SIZE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(size.to_string()))));
        }

        if let Some(policy) = self.duplicate_policy {
            frames.push(Frame::BulkString(Some(Bytes::from("DUPLICATE_POLICY"))));
            frames.push(Frame::BulkString(Some(Bytes::from(match policy {
                DuplicatePolicy::Block => "BLOCK",
                DuplicatePolicy::First => "FIRST",
                DuplicatePolicy::Last => "LAST",
                DuplicatePolicy::Min => "MIN",
                DuplicatePolicy::Max => "MAX",
                DuplicatePolicy::Sum => "SUM",
            }))));
        }

        if !self.labels.is_empty() {
            frames.push(Frame::BulkString(Some(Bytes::from("LABELS"))));
            for (name, value) in &self.labels {
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    name.as_bytes(),
                ))));
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    value.as_bytes(),
                ))));
            }
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// TS.RANGE / TS.REVRANGE - Query time range
// ============================================================================

/// TS.RANGE - Query a range in forward direction
///
/// Available since: RedisTimeSeries 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::timeseries::{TsRange, Aggregator, BucketTimestamp};
///
/// // Basic range query
/// let cmd = TsRange::new("temperature", "-", "+");
///
/// // With aggregation (hourly averages)
/// let cmd = TsRange::new("temperature", "1548149180000", "1548149480000")
///     .aggregation(Aggregator::Avg, 3600000);
///
/// // Advanced query with all options
/// let cmd = TsRange::new("temperature", "-", "+")
///     .latest()
///     .filter_by_value(20.0, 30.0)
///     .count(100)
///     .aggregation(Aggregator::Avg, 60000)
///     .bucket_timestamp(BucketTimestamp::Mid)
///     .empty();
/// ```
#[derive(Debug, Clone)]
pub struct TsRange {
    key: String,
    from_timestamp: String,
    to_timestamp: String,
    latest: bool,
    filter_by_ts: Vec<i64>,
    filter_by_value: Option<(f64, f64)>,
    count: Option<i64>,
    align: Option<String>,
    aggregation: Option<(Aggregator, i64)>, // (aggregator, bucket_duration)
    bucket_timestamp: Option<BucketTimestamp>,
    empty: bool,
}

impl TsRange {
    /// Create a new TS.RANGE query
    ///
    /// # Arguments
    /// * `key` - Time series key
    /// * `from_timestamp` - "-" for earliest or millisecond timestamp
    /// * `to_timestamp` - "+" for latest or millisecond timestamp
    pub fn new(
        key: impl Into<String>,
        from_timestamp: impl Into<String>,
        to_timestamp: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            from_timestamp: from_timestamp.into(),
            to_timestamp: to_timestamp.into(),
            latest: false,
            filter_by_ts: Vec::new(),
            filter_by_value: None,
            count: None,
            align: None,
            aggregation: None,
            bucket_timestamp: None,
            empty: false,
        }
    }

    /// Include latest possibly partial bucket (for compacted series)
    pub fn latest(mut self) -> Self {
        self.latest = true;
        self
    }

    /// Filter by specific timestamps
    pub fn filter_by_ts(mut self, timestamps: Vec<i64>) -> Self {
        self.filter_by_ts = timestamps;
        self
    }

    /// Filter by value range
    pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
        self.filter_by_value = Some((min, max));
        self
    }

    /// Limit number of samples/buckets returned
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Set bucket alignment
    ///
    /// align: "-" or "start" for from_timestamp, "+" or "end" for to_timestamp, or explicit timestamp
    pub fn align(mut self, align: impl Into<String>) -> Self {
        self.align = Some(align.into());
        self
    }

    /// Add aggregation with bucket duration in milliseconds
    pub fn aggregation(mut self, aggregator: Aggregator, bucket_duration: i64) -> Self {
        self.aggregation = Some((aggregator, bucket_duration));
        self
    }

    /// Set bucket timestamp reporting mode
    pub fn bucket_timestamp(mut self, bt: BucketTimestamp) -> Self {
        self.bucket_timestamp = Some(bt);
        self
    }

    /// Report empty buckets
    pub fn empty(mut self) -> Self {
        self.empty = true;
        self
    }
}

impl Command for TsRange {
    type Response = Vec<Sample>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TS.RANGE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.from_timestamp.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.to_timestamp.as_bytes()))),
        ];

        if self.latest {
            frames.push(Frame::BulkString(Some(Bytes::from("LATEST"))));
        }

        if !self.filter_by_ts.is_empty() {
            frames.push(Frame::BulkString(Some(Bytes::from("FILTER_BY_TS"))));
            for ts in &self.filter_by_ts {
                frames.push(Frame::BulkString(Some(Bytes::from(ts.to_string()))));
            }
        }

        if let Some((min, max)) = self.filter_by_value {
            frames.push(Frame::BulkString(Some(Bytes::from("FILTER_BY_VALUE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(min.to_string()))));
            frames.push(Frame::BulkString(Some(Bytes::from(max.to_string()))));
        }

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        if let Some(ref align) = self.align {
            frames.push(Frame::BulkString(Some(Bytes::from("ALIGN"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                align.as_bytes(),
            ))));
        }

        if let Some((aggregator, bucket_duration)) = self.aggregation {
            frames.push(Frame::BulkString(Some(Bytes::from("AGGREGATION"))));
            frames.push(Frame::BulkString(Some(Bytes::from(match aggregator {
                Aggregator::Avg => "AVG",
                Aggregator::Sum => "SUM",
                Aggregator::Min => "MIN",
                Aggregator::Max => "MAX",
                Aggregator::Range => "RANGE",
                Aggregator::Count => "COUNT",
                Aggregator::First => "FIRST",
                Aggregator::Last => "LAST",
                Aggregator::StdP => "STD.P",
                Aggregator::StdS => "STD.S",
                Aggregator::VarP => "VAR.P",
                Aggregator::VarS => "VAR.S",
                Aggregator::Twa => "TWA",
            }))));
            frames.push(Frame::BulkString(Some(Bytes::from(
                bucket_duration.to_string(),
            ))));

            if let Some(bt) = self.bucket_timestamp {
                frames.push(Frame::BulkString(Some(Bytes::from("BUCKETTIMESTAMP"))));
                frames.push(Frame::BulkString(Some(Bytes::from(match bt {
                    BucketTimestamp::Start => "-",
                    BucketTimestamp::End => "+",
                    BucketTimestamp::Mid => "~",
                }))));
            }

            if self.empty {
                frames.push(Frame::BulkString(Some(Bytes::from("EMPTY"))));
            }
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut samples = Vec::new();
                for item in items {
                    if let Frame::Array(pair) = item {
                        if pair.len() == 2 {
                            let timestamp = match &pair[0] {
                                Frame::Integer(n) => *n,
                                _ => continue,
                            };
                            let value = match &pair[1] {
                                Frame::BulkString(Some(data)) => {
                                    String::from_utf8_lossy(data).parse().unwrap_or(0.0)
                                }
                                _ => continue,
                            };
                            samples.push(Sample { timestamp, value });
                        }
                    }
                }
                Ok(samples)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for TsRange {
    fn is_read_only(&self) -> bool {
        true
    }
}

/// TS.REVRANGE - Query a range in reverse direction (same API as TS.RANGE)
///
/// Available since: RedisTimeSeries 1.0.0
#[derive(Debug, Clone)]
pub struct TsRevRange {
    inner: TsRange,
}

impl TsRevRange {
    /// Create a new TS.REVRANGE query
    pub fn new(
        key: impl Into<String>,
        from_timestamp: impl Into<String>,
        to_timestamp: impl Into<String>,
    ) -> Self {
        Self {
            inner: TsRange::new(key, from_timestamp, to_timestamp),
        }
    }

    // Delegate all builder methods to inner
    /// Include latest possibly partial bucket (for compacted series)
    pub fn latest(mut self) -> Self {
        self.inner = self.inner.latest();
        self
    }

    /// Filter by specific timestamps
    pub fn filter_by_ts(mut self, timestamps: Vec<i64>) -> Self {
        self.inner = self.inner.filter_by_ts(timestamps);
        self
    }

    /// Filter by value range
    pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
        self.inner = self.inner.filter_by_value(min, max);
        self
    }

    /// Limit number of samples/buckets returned
    pub fn count(mut self, count: i64) -> Self {
        self.inner = self.inner.count(count);
        self
    }

    /// Set bucket alignment
    pub fn align(mut self, align: impl Into<String>) -> Self {
        self.inner = self.inner.align(align);
        self
    }

    /// Add aggregation with bucket duration
    pub fn aggregation(mut self, aggregator: Aggregator, bucket_duration: i64) -> Self {
        self.inner = self.inner.aggregation(aggregator, bucket_duration);
        self
    }

    /// Set bucket timestamp reporting mode
    pub fn bucket_timestamp(mut self, bt: BucketTimestamp) -> Self {
        self.inner = self.inner.bucket_timestamp(bt);
        self
    }

    /// Report empty buckets
    pub fn empty(mut self) -> Self {
        self.inner = self.inner.empty();
        self
    }
}

impl Command for TsRevRange {
    type Response = Vec<Sample>;

    fn to_frame(&self) -> Frame {
        let mut frame = self.inner.to_frame();
        // Change TS.RANGE to TS.REVRANGE
        if let Frame::Array(ref mut frames) = frame {
            frames[0] = Frame::BulkString(Some(Bytes::from("TS.REVRANGE")));
        }
        frame
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        TsRange::parse_response(frame)
    }
}

impl ReadOnly for TsRevRange {
    fn is_read_only(&self) -> bool {
        true
    }
}

// ============================================================================
// TS.GET - Get latest sample
// ============================================================================

/// TS.GET - Get the latest sample from a time series
///
/// Available since: RedisTimeSeries 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::timeseries::TsGet;
///
/// let cmd = TsGet::new("temperature:room1");
/// let cmd = TsGet::new("temperature:room1").latest(); // Include compacted data
/// ```
#[derive(Debug, Clone)]
pub struct TsGet {
    key: String,
    latest: bool,
}

impl TsGet {
    /// Create a new TS.GET command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            latest: false,
        }
    }

    /// Include latest possibly partial bucket (for compacted series)
    pub fn latest(mut self) -> Self {
        self.latest = true;
        self
    }
}

impl Command for TsGet {
    type Response = Option<Sample>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TS.GET"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if self.latest {
            frames.push(Frame::BulkString(Some(Bytes::from("LATEST"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) if items.len() == 2 => {
                let timestamp = match &items[0] {
                    Frame::Integer(n) => *n,
                    _ => return Err(RedisError::UnexpectedResponse),
                };
                let value = match &items[1] {
                    Frame::BulkString(Some(data)) => String::from_utf8_lossy(data)
                        .parse()
                        .map_err(|_| RedisError::UnexpectedResponse)?,
                    _ => return Err(RedisError::UnexpectedResponse),
                };
                Ok(Some(Sample { timestamp, value }))
            }
            Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for TsGet {
    fn is_read_only(&self) -> bool {
        true
    }
}

// ============================================================================
// TS.MGET - Get latest samples from multiple series
// ============================================================================

/// TS.MGET - Get the latest samples matching a label filter
///
/// Available since: RedisTimeSeries 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::timeseries::TsMGet;
///
/// // Get all temperature sensors
/// let cmd = TsMGet::new()
///     .latest()
///     .with_labels()
///     .filter("sensor=temp");
///
/// // Multiple filters
/// let cmd = TsMGet::new()
///     .filter("room=bedroom")
///     .filter("sensor=temp");
/// ```
#[derive(Debug, Clone)]
pub struct TsMGet {
    latest: bool,
    with_labels: bool,
    selected_labels: Vec<String>,
    filters: Vec<String>,
}

impl TsMGet {
    /// Create a new TS.MGET command
    pub fn new() -> Self {
        Self {
            latest: false,
            with_labels: false,
            selected_labels: Vec::new(),
            filters: Vec::new(),
        }
    }

    /// Include latest possibly partial bucket (for compacted series)
    pub fn latest(mut self) -> Self {
        self.latest = true;
        self
    }

    /// Include all labels in response
    pub fn with_labels(mut self) -> Self {
        self.with_labels = true;
        self
    }

    /// Include specific labels in response
    pub fn selected_labels(mut self, labels: Vec<String>) -> Self {
        self.selected_labels = labels;
        self
    }

    /// Add label filter (format: "label=value" or "label!=value")
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filters.push(filter.into());
        self
    }
}

impl Default for TsMGet {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for TsMGet {
    type Response = Vec<MGetResult>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("TS.MGET")))];

        if self.latest {
            frames.push(Frame::BulkString(Some(Bytes::from("LATEST"))));
        }

        if self.with_labels {
            frames.push(Frame::BulkString(Some(Bytes::from("WITHLABELS"))));
        } else if !self.selected_labels.is_empty() {
            frames.push(Frame::BulkString(Some(Bytes::from("SELECTED_LABELS"))));
            for label in &self.selected_labels {
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    label.as_bytes(),
                ))));
            }
        }

        frames.push(Frame::BulkString(Some(Bytes::from("FILTER"))));
        for filter in &self.filters {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                filter.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::new();
                for item in items {
                    if let Frame::Array(parts) = item {
                        let key = match parts.first() {
                            Some(Frame::BulkString(Some(data))) => {
                                String::from_utf8_lossy(data).to_string()
                            }
                            _ => continue,
                        };

                        let mut labels = HashMap::new();
                        let mut sample = None;

                        // Parse labels and sample based on structure
                        if parts.len() == 3 {
                            // With labels: [key, labels_array, sample_array]
                            if let Frame::Array(label_pairs) = &parts[1] {
                                for pair in label_pairs.chunks(2) {
                                    if pair.len() == 2 {
                                        if let (
                                            Frame::BulkString(Some(k)),
                                            Frame::BulkString(Some(v)),
                                        ) = (&pair[0], &pair[1])
                                        {
                                            labels.insert(
                                                String::from_utf8_lossy(k).to_string(),
                                                String::from_utf8_lossy(v).to_string(),
                                            );
                                        }
                                    }
                                }
                            }
                            if let Frame::Array(sample_parts) = &parts[2] {
                                if sample_parts.len() == 2 {
                                    if let (Frame::Integer(ts), Frame::BulkString(Some(val))) =
                                        (&sample_parts[0], &sample_parts[1])
                                    {
                                        if let Ok(value) =
                                            String::from_utf8_lossy(val).parse::<f64>()
                                        {
                                            sample = Some(Sample {
                                                timestamp: *ts,
                                                value,
                                            });
                                        }
                                    }
                                }
                            }
                        } else if parts.len() == 2 {
                            // Without labels: [key, sample_array]
                            if let Frame::Array(sample_parts) = &parts[1] {
                                if sample_parts.len() == 2 {
                                    if let (Frame::Integer(ts), Frame::BulkString(Some(val))) =
                                        (&sample_parts[0], &sample_parts[1])
                                    {
                                        if let Ok(value) =
                                            String::from_utf8_lossy(val).parse::<f64>()
                                        {
                                            sample = Some(Sample {
                                                timestamp: *ts,
                                                value,
                                            });
                                        }
                                    }
                                }
                            }
                        }

                        results.push(MGetResult {
                            key,
                            labels,
                            sample,
                        });
                    }
                }
                Ok(results)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for TsMGet {
    fn is_read_only(&self) -> bool {
        true
    }
}

// ============================================================================
// TS.MRANGE / TS.MREVRANGE - Multi-series range queries
// ============================================================================

/// TS.MRANGE - Query ranges from multiple series matching label filters
///
/// Available since: RedisTimeSeries 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::timeseries::{TsMRange, Aggregator};
///
/// // Get hourly averages from all temperature sensors
/// let cmd = TsMRange::new("-", "+")
///     .latest()
///     .with_labels()
///     .aggregation(Aggregator::Avg, 3600000)
///     .filter("sensor=temp");
///
/// // Multiple filters with value filtering
/// let cmd = TsMRange::new("1548149180000", "1548149480000")
///     .filter_by_value(20.0, 30.0)
///     .filter("room=bedroom")
///     .filter("sensor=temp");
/// ```
#[derive(Debug, Clone)]
pub struct TsMRange {
    from_timestamp: String,
    to_timestamp: String,
    latest: bool,
    filter_by_ts: Vec<i64>,
    filter_by_value: Option<(f64, f64)>,
    count: Option<i64>,
    align: Option<String>,
    aggregation: Option<(Aggregator, i64)>,
    bucket_timestamp: Option<BucketTimestamp>,
    empty: bool,
    with_labels: bool,
    selected_labels: Vec<String>,
    filters: Vec<String>,
    group_by: Option<(String, Aggregator)>, // (label, reduce_aggregator)
}

impl TsMRange {
    /// Create a new TS.MRANGE command
    pub fn new(from_timestamp: impl Into<String>, to_timestamp: impl Into<String>) -> Self {
        Self {
            from_timestamp: from_timestamp.into(),
            to_timestamp: to_timestamp.into(),
            latest: false,
            filter_by_ts: Vec::new(),
            filter_by_value: None,
            count: None,
            align: None,
            aggregation: None,
            bucket_timestamp: None,
            empty: false,
            with_labels: false,
            selected_labels: Vec::new(),
            filters: Vec::new(),
            group_by: None,
        }
    }

    /// Include latest possibly partial bucket (for compacted series)
    pub fn latest(mut self) -> Self {
        self.latest = true;
        self
    }

    /// Filter by specific timestamps
    pub fn filter_by_ts(mut self, timestamps: Vec<i64>) -> Self {
        self.filter_by_ts = timestamps;
        self
    }

    /// Filter by value range
    pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
        self.filter_by_value = Some((min, max));
        self
    }

    /// Limit number of samples/buckets returned per series
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Set bucket alignment
    pub fn align(mut self, align: impl Into<String>) -> Self {
        self.align = Some(align.into());
        self
    }

    /// Add aggregation with bucket duration
    pub fn aggregation(mut self, aggregator: Aggregator, bucket_duration: i64) -> Self {
        self.aggregation = Some((aggregator, bucket_duration));
        self
    }

    /// Set bucket timestamp reporting mode
    pub fn bucket_timestamp(mut self, bt: BucketTimestamp) -> Self {
        self.bucket_timestamp = Some(bt);
        self
    }

    /// Report empty buckets
    pub fn empty(mut self) -> Self {
        self.empty = true;
        self
    }

    /// Include all labels in response
    pub fn with_labels(mut self) -> Self {
        self.with_labels = true;
        self
    }

    /// Include specific labels in response
    pub fn selected_labels(mut self, labels: Vec<String>) -> Self {
        self.selected_labels = labels;
        self
    }

    /// Add label filter (format: "label=value" or "label!=value")
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filters.push(filter.into());
        self
    }

    /// Group results by label and reduce with aggregator
    pub fn group_by(mut self, label: impl Into<String>, reduce: Aggregator) -> Self {
        self.group_by = Some((label.into(), reduce));
        self
    }
}

impl Command for TsMRange {
    type Response = Vec<MRangeResult>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TS.MRANGE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.from_timestamp.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.to_timestamp.as_bytes()))),
        ];

        if self.latest {
            frames.push(Frame::BulkString(Some(Bytes::from("LATEST"))));
        }

        if !self.filter_by_ts.is_empty() {
            frames.push(Frame::BulkString(Some(Bytes::from("FILTER_BY_TS"))));
            for ts in &self.filter_by_ts {
                frames.push(Frame::BulkString(Some(Bytes::from(ts.to_string()))));
            }
        }

        if let Some((min, max)) = self.filter_by_value {
            frames.push(Frame::BulkString(Some(Bytes::from("FILTER_BY_VALUE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(min.to_string()))));
            frames.push(Frame::BulkString(Some(Bytes::from(max.to_string()))));
        }

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        if let Some(ref align) = self.align {
            frames.push(Frame::BulkString(Some(Bytes::from("ALIGN"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                align.as_bytes(),
            ))));
        }

        if let Some((aggregator, bucket_duration)) = self.aggregation {
            frames.push(Frame::BulkString(Some(Bytes::from("AGGREGATION"))));
            frames.push(Frame::BulkString(Some(Bytes::from(match aggregator {
                Aggregator::Avg => "AVG",
                Aggregator::Sum => "SUM",
                Aggregator::Min => "MIN",
                Aggregator::Max => "MAX",
                Aggregator::Range => "RANGE",
                Aggregator::Count => "COUNT",
                Aggregator::First => "FIRST",
                Aggregator::Last => "LAST",
                Aggregator::StdP => "STD.P",
                Aggregator::StdS => "STD.S",
                Aggregator::VarP => "VAR.P",
                Aggregator::VarS => "VAR.S",
                Aggregator::Twa => "TWA",
            }))));
            frames.push(Frame::BulkString(Some(Bytes::from(
                bucket_duration.to_string(),
            ))));

            if let Some(bt) = self.bucket_timestamp {
                frames.push(Frame::BulkString(Some(Bytes::from("BUCKETTIMESTAMP"))));
                frames.push(Frame::BulkString(Some(Bytes::from(match bt {
                    BucketTimestamp::Start => "-",
                    BucketTimestamp::End => "+",
                    BucketTimestamp::Mid => "~",
                }))));
            }

            if self.empty {
                frames.push(Frame::BulkString(Some(Bytes::from("EMPTY"))));
            }
        }

        if self.with_labels {
            frames.push(Frame::BulkString(Some(Bytes::from("WITHLABELS"))));
        } else if !self.selected_labels.is_empty() {
            frames.push(Frame::BulkString(Some(Bytes::from("SELECTED_LABELS"))));
            for label in &self.selected_labels {
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    label.as_bytes(),
                ))));
            }
        }

        frames.push(Frame::BulkString(Some(Bytes::from("FILTER"))));
        for filter in &self.filters {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                filter.as_bytes(),
            ))));
        }

        if let Some((ref label, reduce)) = self.group_by {
            frames.push(Frame::BulkString(Some(Bytes::from("GROUPBY"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                label.as_bytes(),
            ))));
            frames.push(Frame::BulkString(Some(Bytes::from("REDUCE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(match reduce {
                Aggregator::Avg => "AVG",
                Aggregator::Sum => "SUM",
                Aggregator::Min => "MIN",
                Aggregator::Max => "MAX",
                Aggregator::Range => "RANGE",
                Aggregator::Count => "COUNT",
                Aggregator::First => "FIRST",
                Aggregator::Last => "LAST",
                Aggregator::StdP => "STD.P",
                Aggregator::StdS => "STD.S",
                Aggregator::VarP => "VAR.P",
                Aggregator::VarS => "VAR.S",
                Aggregator::Twa => "TWA",
            }))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::new();
                for item in items {
                    if let Frame::Array(parts) = item {
                        let key = match parts.first() {
                            Some(Frame::BulkString(Some(data))) => {
                                String::from_utf8_lossy(data).to_string()
                            }
                            _ => continue,
                        };

                        let mut labels = HashMap::new();
                        let mut samples = Vec::new();

                        // Parse structure: [key, labels_array, samples_array] or [key, samples_array]
                        if parts.len() == 3 {
                            // With labels
                            if let Frame::Array(label_pairs) = &parts[1] {
                                for pair in label_pairs.chunks(2) {
                                    if pair.len() == 2 {
                                        if let (
                                            Frame::BulkString(Some(k)),
                                            Frame::BulkString(Some(v)),
                                        ) = (&pair[0], &pair[1])
                                        {
                                            labels.insert(
                                                String::from_utf8_lossy(k).to_string(),
                                                String::from_utf8_lossy(v).to_string(),
                                            );
                                        }
                                    }
                                }
                            }
                            if let Frame::Array(sample_arr) = &parts[2] {
                                for sample_pair in sample_arr {
                                    if let Frame::Array(sp) = sample_pair {
                                        if sp.len() == 2 {
                                            if let (
                                                Frame::Integer(ts),
                                                Frame::BulkString(Some(val)),
                                            ) = (&sp[0], &sp[1])
                                            {
                                                if let Ok(value) =
                                                    String::from_utf8_lossy(val).parse::<f64>()
                                                {
                                                    samples.push(Sample {
                                                        timestamp: *ts,
                                                        value,
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } else if parts.len() == 2 {
                            // Without labels
                            if let Frame::Array(sample_arr) = &parts[1] {
                                for sample_pair in sample_arr {
                                    if let Frame::Array(sp) = sample_pair {
                                        if sp.len() == 2 {
                                            if let (
                                                Frame::Integer(ts),
                                                Frame::BulkString(Some(val)),
                                            ) = (&sp[0], &sp[1])
                                            {
                                                if let Ok(value) =
                                                    String::from_utf8_lossy(val).parse::<f64>()
                                                {
                                                    samples.push(Sample {
                                                        timestamp: *ts,
                                                        value,
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        results.push(MRangeResult {
                            key,
                            labels,
                            samples,
                        });
                    }
                }
                Ok(results)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for TsMRange {
    fn is_read_only(&self) -> bool {
        true
    }
}

/// TS.MREVRANGE - Query ranges in reverse from multiple series
///
/// Available since: RedisTimeSeries 1.0.0
#[derive(Debug, Clone)]
pub struct TsMRevRange {
    inner: TsMRange,
}

impl TsMRevRange {
    /// Create a new TS.MREVRANGE command
    pub fn new(from_timestamp: impl Into<String>, to_timestamp: impl Into<String>) -> Self {
        Self {
            inner: TsMRange::new(from_timestamp, to_timestamp),
        }
    }

    // Delegate all builder methods
    /// Include latest possibly partial bucket (for compacted series)
    pub fn latest(mut self) -> Self {
        self.inner = self.inner.latest();
        self
    }

    /// Filter by specific timestamps
    pub fn filter_by_ts(mut self, timestamps: Vec<i64>) -> Self {
        self.inner = self.inner.filter_by_ts(timestamps);
        self
    }

    /// Filter by value range
    pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
        self.inner = self.inner.filter_by_value(min, max);
        self
    }

    /// Limit number of samples/buckets returned per series
    pub fn count(mut self, count: i64) -> Self {
        self.inner = self.inner.count(count);
        self
    }

    /// Set bucket alignment
    pub fn align(mut self, align: impl Into<String>) -> Self {
        self.inner = self.inner.align(align);
        self
    }

    /// Add aggregation with bucket duration
    pub fn aggregation(mut self, aggregator: Aggregator, bucket_duration: i64) -> Self {
        self.inner = self.inner.aggregation(aggregator, bucket_duration);
        self
    }

    /// Set bucket timestamp reporting mode
    pub fn bucket_timestamp(mut self, bt: BucketTimestamp) -> Self {
        self.inner = self.inner.bucket_timestamp(bt);
        self
    }

    /// Report empty buckets
    pub fn empty(mut self) -> Self {
        self.inner = self.inner.empty();
        self
    }

    /// Include all labels in response
    pub fn with_labels(mut self) -> Self {
        self.inner = self.inner.with_labels();
        self
    }

    /// Include specific labels in response
    pub fn selected_labels(mut self, labels: Vec<String>) -> Self {
        self.inner = self.inner.selected_labels(labels);
        self
    }

    /// Add label filter (format: "label=value" or "label!=value")
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.inner = self.inner.filter(filter);
        self
    }

    /// Group results by label and reduce with aggregator
    pub fn group_by(mut self, label: impl Into<String>, reduce: Aggregator) -> Self {
        self.inner = self.inner.group_by(label, reduce);
        self
    }
}

impl Command for TsMRevRange {
    type Response = Vec<MRangeResult>;

    fn to_frame(&self) -> Frame {
        let mut frame = self.inner.to_frame();
        if let Frame::Array(ref mut frames) = frame {
            frames[0] = Frame::BulkString(Some(Bytes::from("TS.MREVRANGE")));
        }
        frame
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        TsMRange::parse_response(frame)
    }
}

impl ReadOnly for TsMRevRange {
    fn is_read_only(&self) -> bool {
        true
    }
}

// ============================================================================
// TS.INFO - Get time series information
// ============================================================================

/// TS.INFO - Get information about a time series
///
/// Available since: RedisTimeSeries 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::timeseries::TsInfo;
///
/// let cmd = TsInfo::new("temperature:room1");
/// let cmd = TsInfo::new("temperature:room1").debug(); // Include debug info
/// ```
#[derive(Debug, Clone)]
pub struct TsInfo {
    key: String,
    debug: bool,
}

impl TsInfo {
    /// Create a new TS.INFO command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            debug: false,
        }
    }

    /// Include debug information
    pub fn debug(mut self) -> Self {
        self.debug = true;
        self
    }
}

impl Command for TsInfo {
    type Response = TimeSeriesInfo;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TS.INFO"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if self.debug {
            frames.push(Frame::BulkString(Some(Bytes::from("DEBUG"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        // TS.INFO returns array of key-value pairs
        match frame {
            Frame::Array(items) => {
                let mut info = TimeSeriesInfo {
                    total_samples: 0,
                    memory_usage: 0,
                    first_timestamp: 0,
                    last_timestamp: 0,
                    retention_time: 0,
                    chunk_count: 0,
                    chunk_size: 0,
                    duplicate_policy: None,
                    labels: HashMap::new(),
                    source_key: None,
                    rules: Vec::new(),
                };

                let mut i = 0;
                while i < items.len() {
                    if let Frame::BulkString(Some(key)) = &items[i] {
                        let key_str = String::from_utf8_lossy(key);
                        i += 1;
                        if i >= items.len() {
                            break;
                        }

                        match key_str.as_ref() {
                            "totalSamples" => {
                                if let Frame::Integer(n) = items[i] {
                                    info.total_samples = n;
                                }
                            }
                            "memoryUsage" => {
                                if let Frame::Integer(n) = items[i] {
                                    info.memory_usage = n;
                                }
                            }
                            "firstTimestamp" => {
                                if let Frame::Integer(n) = items[i] {
                                    info.first_timestamp = n;
                                }
                            }
                            "lastTimestamp" => {
                                if let Frame::Integer(n) = items[i] {
                                    info.last_timestamp = n;
                                }
                            }
                            "retentionTime" => {
                                if let Frame::Integer(n) = items[i] {
                                    info.retention_time = n;
                                }
                            }
                            "chunkCount" => {
                                if let Frame::Integer(n) = items[i] {
                                    info.chunk_count = n;
                                }
                            }
                            "chunkSize" => {
                                if let Frame::Integer(n) = items[i] {
                                    info.chunk_size = n;
                                }
                            }
                            "duplicatePolicy" => {
                                if let Frame::BulkString(Some(s)) = &items[i] {
                                    info.duplicate_policy =
                                        Some(String::from_utf8_lossy(s).to_string());
                                }
                            }
                            "labels" => {
                                if let Frame::Array(label_pairs) = &items[i] {
                                    for pair in label_pairs.chunks(2) {
                                        if pair.len() == 2 {
                                            if let (
                                                Frame::BulkString(Some(k)),
                                                Frame::BulkString(Some(v)),
                                            ) = (&pair[0], &pair[1])
                                            {
                                                info.labels.insert(
                                                    String::from_utf8_lossy(k).to_string(),
                                                    String::from_utf8_lossy(v).to_string(),
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            "sourceKey" => {
                                if let Frame::BulkString(Some(s)) = &items[i] {
                                    info.source_key = Some(String::from_utf8_lossy(s).to_string());
                                }
                            }
                            "rules" => {
                                if let Frame::Array(rules_arr) = &items[i] {
                                    for rule in rules_arr {
                                        if let Frame::Array(rule_parts) = rule {
                                            if rule_parts.len() >= 3 {
                                                let dest_key = if let Frame::BulkString(Some(k)) =
                                                    &rule_parts[0]
                                                {
                                                    String::from_utf8_lossy(k).to_string()
                                                } else {
                                                    continue;
                                                };
                                                let bucket_duration =
                                                    if let Frame::Integer(n) = rule_parts[1] {
                                                        n
                                                    } else {
                                                        continue;
                                                    };
                                                let aggregator = if let Frame::BulkString(Some(a)) =
                                                    &rule_parts[2]
                                                {
                                                    String::from_utf8_lossy(a).to_string()
                                                } else {
                                                    continue;
                                                };

                                                info.rules.push(CompactionRule {
                                                    dest_key,
                                                    bucket_duration,
                                                    aggregator,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    i += 1;
                }

                Ok(info)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for TsInfo {
    fn is_read_only(&self) -> bool {
        true
    }
}

// ============================================================================
// TS.QUERYINDEX - Find series by labels
// ============================================================================

/// TS.QUERYINDEX - Get keys matching label filters
///
/// Available since: RedisTimeSeries 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::timeseries::TsQueryIndex;
///
/// // Find all temperature sensors
/// let cmd = TsQueryIndex::new().filter("sensor=temp");
///
/// // Multiple filters
/// let cmd = TsQueryIndex::new()
///     .filter("room=bedroom")
///     .filter("sensor=temp");
/// ```
#[derive(Debug, Clone)]
pub struct TsQueryIndex {
    filters: Vec<String>,
}

impl TsQueryIndex {
    /// Create a new TS.QUERYINDEX command
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
        }
    }

    /// Add label filter (format: "label=value" or "label!=value")
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filters.push(filter.into());
        self
    }
}

impl Default for TsQueryIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for TsQueryIndex {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("TS.QUERYINDEX")))];

        for filter in &self.filters {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                filter.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let keys = items
                    .iter()
                    .filter_map(|f| {
                        if let Frame::BulkString(Some(data)) = f {
                            Some(String::from_utf8_lossy(data).to_string())
                        } else {
                            None
                        }
                    })
                    .collect();
                Ok(keys)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for TsQueryIndex {
    fn is_read_only(&self) -> bool {
        true
    }
}

// ============================================================================
// TS.ALTER - Modify series configuration
// ============================================================================

/// TS.ALTER - Update retention, chunk size, duplicate policy, or labels
///
/// Available since: RedisTimeSeries 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::timeseries::{TsAlter, DuplicatePolicy};
///
/// // Update retention
/// let cmd = TsAlter::new("temperature:room1").retention(172800000); // 48 hours
///
/// // Update duplicate policy
/// let cmd = TsAlter::new("temperature:room1")
///     .duplicate_policy(DuplicatePolicy::Last);
///
/// // Update labels
/// let cmd = TsAlter::new("temperature:room1")
///     .label("room", "bedroom")
///     .label("floor", "2");
/// ```
#[derive(Debug, Clone)]
pub struct TsAlter {
    key: String,
    retention: Option<i64>,
    chunk_size: Option<i64>,
    duplicate_policy: Option<DuplicatePolicy>,
    ignore: Option<(i64, f64)>,
    labels: Vec<(String, String)>,
}

impl TsAlter {
    /// Create a new TS.ALTER command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            retention: None,
            chunk_size: None,
            duplicate_policy: None,
            ignore: None,
            labels: Vec::new(),
        }
    }

    /// Update retention period in milliseconds
    pub fn retention(mut self, ms: i64) -> Self {
        self.retention = Some(ms);
        self
    }

    /// Update chunk size in bytes
    pub fn chunk_size(mut self, size: i64) -> Self {
        self.chunk_size = Some(size);
        self
    }

    /// Update duplicate timestamp policy
    pub fn duplicate_policy(mut self, policy: DuplicatePolicy) -> Self {
        self.duplicate_policy = Some(policy);
        self
    }

    /// Update ignore policy for near-duplicate samples
    pub fn ignore(mut self, max_time_diff: i64, max_val_diff: f64) -> Self {
        self.ignore = Some((max_time_diff, max_val_diff));
        self
    }

    /// Add or update a label
    pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((name.into(), value.into()));
        self
    }

    /// Add or update multiple labels
    pub fn labels(mut self, labels: Vec<(String, String)>) -> Self {
        self.labels.extend(labels);
        self
    }
}

impl Command for TsAlter {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TS.ALTER"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(retention) = self.retention {
            frames.push(Frame::BulkString(Some(Bytes::from("RETENTION"))));
            frames.push(Frame::BulkString(Some(Bytes::from(retention.to_string()))));
        }

        if let Some(size) = self.chunk_size {
            frames.push(Frame::BulkString(Some(Bytes::from("CHUNK_SIZE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(size.to_string()))));
        }

        if let Some(policy) = self.duplicate_policy {
            frames.push(Frame::BulkString(Some(Bytes::from("DUPLICATE_POLICY"))));
            frames.push(Frame::BulkString(Some(Bytes::from(match policy {
                DuplicatePolicy::Block => "BLOCK",
                DuplicatePolicy::First => "FIRST",
                DuplicatePolicy::Last => "LAST",
                DuplicatePolicy::Min => "MIN",
                DuplicatePolicy::Max => "MAX",
                DuplicatePolicy::Sum => "SUM",
            }))));
        }

        if let Some((time_diff, val_diff)) = self.ignore {
            frames.push(Frame::BulkString(Some(Bytes::from("IGNORE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(time_diff.to_string()))));
            frames.push(Frame::BulkString(Some(Bytes::from(val_diff.to_string()))));
        }

        if !self.labels.is_empty() {
            frames.push(Frame::BulkString(Some(Bytes::from("LABELS"))));
            for (name, value) in &self.labels {
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    name.as_bytes(),
                ))));
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    value.as_bytes(),
                ))));
            }
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// TS.DEL - Delete samples in time range
// ============================================================================

/// TS.DEL - Delete samples in a time range
///
/// Available since: RedisTimeSeries 1.6.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::timeseries::TsDel;
///
/// // Delete all samples
/// let cmd = TsDel::new("temperature:room1", "-", "+");
///
/// // Delete specific range
/// let cmd = TsDel::new("temperature:room1", "1548149180000", "1548149480000");
/// ```
#[derive(Debug, Clone)]
pub struct TsDel {
    key: String,
    from_timestamp: String,
    to_timestamp: String,
}

impl TsDel {
    /// Create a new TS.DEL command
    pub fn new(
        key: impl Into<String>,
        from_timestamp: impl Into<String>,
        to_timestamp: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            from_timestamp: from_timestamp.into(),
            to_timestamp: to_timestamp.into(),
        }
    }
}

impl Command for TsDel {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("TS.DEL"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.from_timestamp.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.to_timestamp.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// TS.CREATERULE - Create downsampling rule
// ============================================================================

/// TS.CREATERULE - Create a compaction/downsampling rule
///
/// Available since: RedisTimeSeries 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::timeseries::{TsCreateRule, Aggregator};
///
/// // Create hourly averages
/// let cmd = TsCreateRule::new("temperature:raw", "temperature:hourly", Aggregator::Avg, 3600000);
///
/// // Create daily maximums with alignment
/// let cmd = TsCreateRule::new("temperature:raw", "temperature:daily", Aggregator::Max, 86400000)
///     .align_timestamp(0); // Align to midnight
/// ```
#[derive(Debug, Clone)]
pub struct TsCreateRule {
    source_key: String,
    dest_key: String,
    aggregation: Aggregator,
    bucket_duration: i64,
    align_timestamp: Option<i64>,
}

impl TsCreateRule {
    /// Create a new TS.CREATERULE command
    pub fn new(
        source_key: impl Into<String>,
        dest_key: impl Into<String>,
        aggregation: Aggregator,
        bucket_duration: i64,
    ) -> Self {
        Self {
            source_key: source_key.into(),
            dest_key: dest_key.into(),
            aggregation,
            bucket_duration,
            align_timestamp: None,
        }
    }

    /// Align buckets to specific timestamp (e.g., 0 for midnight alignment)
    pub fn align_timestamp(mut self, timestamp: i64) -> Self {
        self.align_timestamp = Some(timestamp);
        self
    }
}

impl Command for TsCreateRule {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TS.CREATERULE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.source_key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.dest_key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from("AGGREGATION"))),
            Frame::BulkString(Some(Bytes::from(match self.aggregation {
                Aggregator::Avg => "AVG",
                Aggregator::Sum => "SUM",
                Aggregator::Min => "MIN",
                Aggregator::Max => "MAX",
                Aggregator::Range => "RANGE",
                Aggregator::Count => "COUNT",
                Aggregator::First => "FIRST",
                Aggregator::Last => "LAST",
                Aggregator::StdP => "STD.P",
                Aggregator::StdS => "STD.S",
                Aggregator::VarP => "VAR.P",
                Aggregator::VarS => "VAR.S",
                Aggregator::Twa => "TWA",
            }))),
            Frame::BulkString(Some(Bytes::from(self.bucket_duration.to_string()))),
        ];

        if let Some(ts) = self.align_timestamp {
            frames.push(Frame::BulkString(Some(Bytes::from(ts.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// TS.DELETERULE - Delete downsampling rule
// ============================================================================

/// TS.DELETERULE - Delete a compaction/downsampling rule
///
/// Available since: RedisTimeSeries 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::timeseries::TsDeleteRule;
///
/// let cmd = TsDeleteRule::new("temperature:raw", "temperature:hourly");
/// ```
#[derive(Debug, Clone)]
pub struct TsDeleteRule {
    source_key: String,
    dest_key: String,
}

impl TsDeleteRule {
    /// Create a new TS.DELETERULE command
    pub fn new(source_key: impl Into<String>, dest_key: impl Into<String>) -> Self {
        Self {
            source_key: source_key.into(),
            dest_key: dest_key.into(),
        }
    }
}

impl Command for TsDeleteRule {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("TS.DELETERULE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.source_key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.dest_key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ts_create_basic() {
        let cmd = TsCreate::new("temperature:room1");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TS.CREATE"))));
                assert_eq!(
                    parts[1],
                    Frame::BulkString(Some(Bytes::from("temperature:room1")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_create_with_options() {
        let cmd = TsCreate::new("temp")
            .retention(86400000)
            .encoding(Encoding::Compressed)
            .duplicate_policy(DuplicatePolicy::Last)
            .label("room", "living");

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("RETENTION"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("COMPRESSED"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("LAST"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("LABELS"))))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_add_basic() {
        let cmd = TsAdd::new("temp", "*", 23.5);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TS.ADD"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("temp"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("*"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("23.5"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_madd() {
        let cmd = TsMAdd::new()
            .add("temp1", "*", 23.5)
            .add("temp2", "1000", 24.0);

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TS.MADD"))));
                // Should have 1 command + 2 samples * 3 args each = 7 total
                assert_eq!(parts.len(), 7);
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_range_basic() {
        let cmd = TsRange::new("temp", "-", "+");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TS.RANGE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("temp"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("-"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("+"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_range_with_aggregation() {
        let cmd = TsRange::new("temp", "1000", "2000")
            .aggregation(Aggregator::Avg, 60000)
            .bucket_timestamp(BucketTimestamp::Mid);

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("AGGREGATION"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("AVG"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("60000"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("BUCKETTIMESTAMP"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("~"))))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_get_basic() {
        let cmd = TsGet::new("temp");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TS.GET"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("temp"))));
                assert_eq!(parts.len(), 2);
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_get_with_latest() {
        let cmd = TsGet::new("temp").latest();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("LATEST"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_mget_basic() {
        let cmd = TsMGet::new().filter("sensor=temp");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TS.MGET"))));
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("FILTER"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("sensor=temp"))))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_mget_with_labels() {
        let cmd = TsMGet::new().with_labels().filter("room=bedroom");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("WITHLABELS"))))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_mrange_basic() {
        let cmd = TsMRange::new("-", "+").filter("sensor=temp");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TS.MRANGE"))));
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("FILTER"))))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_mrange_with_group_by() {
        let cmd = TsMRange::new("-", "+")
            .filter("sensor=temp")
            .group_by("room", Aggregator::Avg);

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("GROUPBY"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("room"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("REDUCE"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("AVG"))))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_info_basic() {
        let cmd = TsInfo::new("temp");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TS.INFO"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("temp"))));
                assert_eq!(parts.len(), 2);
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_info_with_debug() {
        let cmd = TsInfo::new("temp").debug();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("DEBUG"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_queryindex() {
        let cmd = TsQueryIndex::new()
            .filter("room=bedroom")
            .filter("sensor=temp");

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("TS.QUERYINDEX")))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("room=bedroom"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("sensor=temp"))))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_alter_retention() {
        let cmd = TsAlter::new("temp").retention(172800000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TS.ALTER"))));
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("RETENTION"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("172800000"))))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_del() {
        let cmd = TsDel::new("temp", "1000", "2000");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TS.DEL"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("temp"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("1000"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("2000"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_createrule_basic() {
        let cmd = TsCreateRule::new("temp:raw", "temp:hourly", Aggregator::Avg, 3600000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("TS.CREATERULE")))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("temp:raw"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("temp:hourly"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("AGGREGATION"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("AVG"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("3600000"))))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_createrule_with_alignment() {
        let cmd = TsCreateRule::new("temp:raw", "temp:daily", Aggregator::Max, 86400000)
            .align_timestamp(0);

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("0"))))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ts_deleterule() {
        let cmd = TsDeleteRule::new("temp:raw", "temp:hourly");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("TS.DELETERULE")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("temp:raw"))));
                assert_eq!(
                    parts[2],
                    Frame::BulkString(Some(Bytes::from("temp:hourly")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_sample_response_parsing() {
        // Test TS.RANGE response parsing
        let response = Frame::Array(vec![
            Frame::Array(vec![
                Frame::Integer(1000),
                Frame::BulkString(Some(Bytes::from("23.5"))),
            ]),
            Frame::Array(vec![
                Frame::Integer(2000),
                Frame::BulkString(Some(Bytes::from("24.2"))),
            ]),
        ]);

        let samples = TsRange::parse_response(response).unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].timestamp, 1000);
        assert_eq!(samples[0].value, 23.5);
        assert_eq!(samples[1].timestamp, 2000);
        assert_eq!(samples[1].value, 24.2);
    }

    #[test]
    fn test_aggregator_encoding() {
        assert_eq!(
            match Aggregator::StdP {
                Aggregator::StdP => "STD.P",
                _ => "",
            },
            "STD.P"
        );
        assert_eq!(
            match Aggregator::VarS {
                Aggregator::VarS => "VAR.S",
                _ => "",
            },
            "VAR.S"
        );
    }
}
