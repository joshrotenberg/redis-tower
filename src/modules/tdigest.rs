//! Redis T-Digest commands
//!
//! T-Digest is a probabilistic data structure for accurate estimation of quantiles
//! and percentiles from streaming data. It's particularly useful for monitoring and
//! observability use cases where you need accurate percentiles (p50, p95, p99, etc.).
//!
//! # Key Features
//! - **Accurate Percentiles**: More accurate than histograms, especially at extremes
//! - **Space Efficient**: Compact representation of distribution
//! - **Mergeable**: Combine multiple t-digests
//! - **Streaming**: Process data points one at a time
//!
//! # Use Cases
//! - API latency monitoring (p50, p95, p99)
//! - Database query performance
//! - Network metrics
//! - SLA tracking
//! - A/B testing analysis
//!
//! # Examples
//! ```no_run
//! use redis_tower::modules::tdigest::{TDigestCreate, TDigestAdd, TDigestQuantile};
//! use redis_tower::RedisClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("localhost:6379").await?;
//!
//! // Create t-digest for latency tracking
//! client.call(TDigestCreate::new("api_latency")
//!     .compression(100) // Higher = more accurate
//! ).await?;
//!
//! // Add latency measurements (in ms)
//! client.call(TDigestAdd::new("api_latency", vec![
//!     45.2, 52.1, 38.9, 91.3, 102.5
//! ])).await?;
//!
//! // Get p95 latency
//! let p95: Vec<f64> = client.call(TDigestQuantile::new("api_latency", vec![0.95])).await?;
//! println!("p95 latency: {}ms", p95[0]);
//! # Ok(())
//! # }
//! ```

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// TDIGEST.CREATE - Create a new t-digest
///
/// Creates a new t-digest with optional compression parameter.
/// Higher compression = more accuracy but more memory.
///
/// # Arguments
/// * `key` - T-digest key name
///
/// # Optional
/// * `compression` - Compression parameter (default: 100, range: 10-10000)
///
/// # Example
/// ```no_run
/// use redis_tower::modules::tdigest::TDigestCreate;
///
/// // Default compression (100)
/// let cmd = TDigestCreate::new("latency");
///
/// // Higher accuracy
/// let cmd = TDigestCreate::new("latency").compression(500);
/// ```
#[derive(Debug, Clone)]
pub struct TDigestCreate {
    key: String,
    compression: Option<i64>,
}

impl TDigestCreate {
    /// Create a new TDIGEST.CREATE command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            compression: None,
        }
    }

    /// Set compression parameter
    pub fn compression(mut self, compression: i64) -> Self {
        self.compression = Some(compression);
        self
    }
}

impl Command for TDigestCreate {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.CREATE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(c) = self.compression {
            frames.push(Frame::BulkString(Some(Bytes::from("COMPRESSION"))));
            frames.push(Frame::BulkString(Some(Bytes::from(c.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TDIGEST.RESET - Reset a t-digest
///
/// Clears all data from the t-digest while keeping the structure.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::tdigest::TDigestReset;
///
/// let cmd = TDigestReset::new("latency");
/// ```
#[derive(Debug, Clone)]
pub struct TDigestReset {
    key: String,
}

impl TDigestReset {
    /// Create a new TDIGEST.RESET command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for TDigestReset {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.RESET"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TDIGEST.ADD - Add one or more values to the t-digest
///
/// Adds values to the t-digest. Values can represent measurements like latencies,
/// response times, sizes, etc.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::tdigest::TDigestAdd;
///
/// // Add single value
/// let cmd = TDigestAdd::new("latency", vec![45.2]);
///
/// // Add multiple values
/// let cmd = TDigestAdd::new("latency", vec![45.2, 52.1, 38.9, 91.3]);
/// ```
#[derive(Debug, Clone)]
pub struct TDigestAdd {
    key: String,
    values: Vec<f64>,
}

impl TDigestAdd {
    /// Create a new TDIGEST.ADD command
    pub fn new(key: impl Into<String>, values: Vec<f64>) -> Self {
        Self {
            key: key.into(),
            values,
        }
    }

    /// Create from a single value
    pub fn single(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            values: vec![value],
        }
    }
}

impl Command for TDigestAdd {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.ADD"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for value in &self.values {
            frames.push(Frame::BulkString(Some(Bytes::from(value.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TDIGEST.MERGE - Merge multiple t-digests
///
/// Merges source t-digests into a destination. All t-digests are preserved.
/// Optionally apply compression to the result and/or override existing destination.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::tdigest::TDigestMerge;
///
/// // Merge two t-digests
/// let cmd = TDigestMerge::new("merged", vec!["digest1".into(), "digest2".into()]);
///
/// // With compression and override
/// let cmd = TDigestMerge::new("merged", vec!["digest1".into(), "digest2".into()])
///     .compression(200)
///     .override_dest();
/// ```
#[derive(Debug, Clone)]
pub struct TDigestMerge {
    dest: String,
    sources: Vec<String>,
    compression: Option<i64>,
    override_dest: bool,
}

impl TDigestMerge {
    /// Create a new TDIGEST.MERGE command
    pub fn new(dest: impl Into<String>, sources: Vec<String>) -> Self {
        Self {
            dest: dest.into(),
            sources,
            compression: None,
            override_dest: false,
        }
    }

    /// Set compression for the merged result
    pub fn compression(mut self, compression: i64) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Override existing destination (otherwise merge with it)
    pub fn override_dest(mut self) -> Self {
        self.override_dest = true;
        self
    }
}

impl Command for TDigestMerge {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.MERGE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.dest.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.sources.len().to_string()))),
        ];

        for source in &self.sources {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                source.as_bytes(),
            ))));
        }

        if let Some(c) = self.compression {
            frames.push(Frame::BulkString(Some(Bytes::from("COMPRESSION"))));
            frames.push(Frame::BulkString(Some(Bytes::from(c.to_string()))));
        }

        if self.override_dest {
            frames.push(Frame::BulkString(Some(Bytes::from("OVERRIDE"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TDIGEST.MIN - Get minimum value
///
/// Returns the minimum value seen by the t-digest.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::tdigest::TDigestMin;
///
/// let cmd = TDigestMin::new("latency");
/// // Response: 12.5 (minimum latency)
/// ```
#[derive(Debug, Clone)]
pub struct TDigestMin {
    key: String,
}

impl TDigestMin {
    /// Create a new TDIGEST.MIN command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for TDigestMin {
    type Response = f64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.MIN"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(&data);
                s.parse::<f64>().map_err(|_| RedisError::UnexpectedResponse)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TDIGEST.MAX - Get maximum value
///
/// Returns the maximum value seen by the t-digest.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::tdigest::TDigestMax;
///
/// let cmd = TDigestMax::new("latency");
/// // Response: 523.7 (maximum latency)
/// ```
#[derive(Debug, Clone)]
pub struct TDigestMax {
    key: String,
}

impl TDigestMax {
    /// Create a new TDIGEST.MAX command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for TDigestMax {
    type Response = f64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.MAX"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(&data);
                s.parse::<f64>().map_err(|_| RedisError::UnexpectedResponse)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TDIGEST.QUANTILE - Get values at specified quantiles
///
/// Returns the estimated values at the specified quantiles (0.0 to 1.0).
/// Commonly used for percentiles: 0.50 (p50/median), 0.95 (p95), 0.99 (p99).
///
/// # Example
/// ```no_run
/// use redis_tower::modules::tdigest::TDigestQuantile;
///
/// // Get p50, p95, p99
/// let cmd = TDigestQuantile::new("latency", vec![0.50, 0.95, 0.99]);
/// // Response: vec![45.2, 112.3, 205.1]
/// ```
#[derive(Debug, Clone)]
pub struct TDigestQuantile {
    key: String,
    quantiles: Vec<f64>,
}

impl TDigestQuantile {
    /// Create a new TDIGEST.QUANTILE command
    pub fn new(key: impl Into<String>, quantiles: Vec<f64>) -> Self {
        Self {
            key: key.into(),
            quantiles,
        }
    }

    /// Get a single quantile
    pub fn single(key: impl Into<String>, quantile: f64) -> Self {
        Self {
            key: key.into(),
            quantiles: vec![quantile],
        }
    }
}

impl Command for TDigestQuantile {
    type Response = Vec<f64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.QUANTILE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for q in &self.quantiles {
            frames.push(Frame::BulkString(Some(Bytes::from(q.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            let s = String::from_utf8_lossy(&data);
                            let val = s
                                .parse::<f64>()
                                .map_err(|_| RedisError::UnexpectedResponse)?;
                            results.push(val);
                        }
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TDIGEST.CDF - Get cumulative distribution function values
///
/// Returns the fraction of values <= each specified value.
/// Opposite of QUANTILE: given values, returns their percentiles.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::tdigest::TDigestCdf;
///
/// // What percentile are these latencies at?
/// let cmd = TDigestCdf::new("latency", vec![50.0, 100.0, 200.0]);
/// // Response: vec![0.52, 0.87, 0.98] - 50ms is p52, 100ms is p87, etc.
/// ```
#[derive(Debug, Clone)]
pub struct TDigestCdf {
    key: String,
    values: Vec<f64>,
}

impl TDigestCdf {
    /// Create a new TDIGEST.CDF command
    pub fn new(key: impl Into<String>, values: Vec<f64>) -> Self {
        Self {
            key: key.into(),
            values,
        }
    }

    /// Get CDF for a single value
    pub fn single(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            values: vec![value],
        }
    }
}

impl Command for TDigestCdf {
    type Response = Vec<f64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.CDF"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for v in &self.values {
            frames.push(Frame::BulkString(Some(Bytes::from(v.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            let s = String::from_utf8_lossy(&data);
                            let val = s
                                .parse::<f64>()
                                .map_err(|_| RedisError::UnexpectedResponse)?;
                            results.push(val);
                        }
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TDIGEST.TRIMMED_MEAN - Get trimmed mean
///
/// Returns the mean after removing values from the low and high end.
/// Useful for robust statistics that ignore outliers.
///
/// # Arguments
/// * `key` - T-digest key
/// * `low_quantile` - Fraction to trim from low end (e.g., 0.05)
/// * `high_quantile` - Fraction to trim from high end (e.g., 0.95)
///
/// # Example
/// ```no_run
/// use redis_tower::modules::tdigest::TDigestTrimmedMean;
///
/// // Mean of middle 90% (trim bottom 5% and top 5%)
/// let cmd = TDigestTrimmedMean::new("latency", 0.05, 0.95);
/// // Response: 62.3 (average latency excluding outliers)
/// ```
#[derive(Debug, Clone)]
pub struct TDigestTrimmedMean {
    key: String,
    low_quantile: f64,
    high_quantile: f64,
}

impl TDigestTrimmedMean {
    /// Create a new TDIGEST.TRIMMED_MEAN command
    pub fn new(key: impl Into<String>, low_quantile: f64, high_quantile: f64) -> Self {
        Self {
            key: key.into(),
            low_quantile,
            high_quantile,
        }
    }
}

impl Command for TDigestTrimmedMean {
    type Response = f64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.TRIMMED_MEAN"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.low_quantile.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.high_quantile.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(&data);
                s.parse::<f64>().map_err(|_| RedisError::UnexpectedResponse)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TDIGEST.RANK - Get ranks of values
///
/// Returns the rank (number of values <= given value) for each value.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::tdigest::TDigestRank;
///
/// let cmd = TDigestRank::new("latency", vec![50.0, 100.0]);
/// // Response: vec![520, 870] - 520 values <= 50ms, 870 values <= 100ms
/// ```
#[derive(Debug, Clone)]
pub struct TDigestRank {
    key: String,
    values: Vec<f64>,
}

impl TDigestRank {
    /// Create a new TDIGEST.RANK command
    pub fn new(key: impl Into<String>, values: Vec<f64>) -> Self {
        Self {
            key: key.into(),
            values,
        }
    }
}

impl Command for TDigestRank {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.RANK"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for v in &self.values {
            frames.push(Frame::BulkString(Some(Bytes::from(v.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::Integer(n) => results.push(n),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TDIGEST.REVRANK - Get reverse ranks of values
///
/// Returns the reverse rank (number of values >= given value) for each value.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::tdigest::TDigestRevRank;
///
/// let cmd = TDigestRevRank::new("latency", vec![50.0, 100.0]);
/// // Response: vec![480, 130] - 480 values >= 50ms, 130 values >= 100ms
/// ```
#[derive(Debug, Clone)]
pub struct TDigestRevRank {
    key: String,
    values: Vec<f64>,
}

impl TDigestRevRank {
    /// Create a new TDIGEST.REVRANK command
    pub fn new(key: impl Into<String>, values: Vec<f64>) -> Self {
        Self {
            key: key.into(),
            values,
        }
    }
}

impl Command for TDigestRevRank {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.REVRANK"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for v in &self.values {
            frames.push(Frame::BulkString(Some(Bytes::from(v.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::Integer(n) => results.push(n),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TDIGEST.BYRANK - Get values by rank
///
/// Returns the values at the specified ranks.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::tdigest::TDigestByRank;
///
/// // Get values at ranks 500 and 950 (out of 1000)
/// let cmd = TDigestByRank::new("latency", vec![500, 950]);
/// // Response: vec![52.3, 187.2] - median and p95 approximations
/// ```
#[derive(Debug, Clone)]
pub struct TDigestByRank {
    key: String,
    ranks: Vec<i64>,
}

impl TDigestByRank {
    /// Create a new TDIGEST.BYRANK command
    pub fn new(key: impl Into<String>, ranks: Vec<i64>) -> Self {
        Self {
            key: key.into(),
            ranks,
        }
    }
}

impl Command for TDigestByRank {
    type Response = Vec<f64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.BYRANK"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for r in &self.ranks {
            frames.push(Frame::BulkString(Some(Bytes::from(r.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            let s = String::from_utf8_lossy(&data);
                            let val = s
                                .parse::<f64>()
                                .map_err(|_| RedisError::UnexpectedResponse)?;
                            results.push(val);
                        }
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TDIGEST.BYREVRANK - Get values by reverse rank
///
/// Returns the values at the specified reverse ranks.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::tdigest::TDigestByRevRank;
///
/// // Get values at reverse ranks 50 and 10 (top values)
/// let cmd = TDigestByRevRank::new("latency", vec![50, 10]);
/// ```
#[derive(Debug, Clone)]
pub struct TDigestByRevRank {
    key: String,
    ranks: Vec<i64>,
}

impl TDigestByRevRank {
    /// Create a new TDIGEST.BYREVRANK command
    pub fn new(key: impl Into<String>, ranks: Vec<i64>) -> Self {
        Self {
            key: key.into(),
            ranks,
        }
    }
}

impl Command for TDigestByRevRank {
    type Response = Vec<f64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.BYREVRANK"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for r in &self.ranks {
            frames.push(Frame::BulkString(Some(Bytes::from(r.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            let s = String::from_utf8_lossy(&data);
                            let val = s
                                .parse::<f64>()
                                .map_err(|_| RedisError::UnexpectedResponse)?;
                            results.push(val);
                        }
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// Read-only trait implementations
use crate::read_preference::ReadOnly;

impl ReadOnly for TDigestMin {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for TDigestMax {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for TDigestQuantile {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for TDigestCdf {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for TDigestTrimmedMean {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for TDigestRank {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for TDigestRevRank {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for TDigestByRank {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for TDigestByRevRank {
    fn is_read_only(&self) -> bool {
        true
    }
}

/// T-Digest sketch information from TDIGEST.INFO
#[derive(Debug, Clone, PartialEq)]
pub struct TDigestInfoResult {
    /// Compression setting (trade-off between accuracy and memory)
    pub compression: i64,
    /// Buffer size for storing centroids and observations
    pub capacity: i64,
    /// Count of merged observations
    pub merged_nodes: i64,
    /// Count of buffered (uncompressed) observations
    pub unmerged_nodes: i64,
    /// Weight total of merged node values
    pub merged_weight: i64,
    /// Weight total of unmerged node values
    pub unmerged_weight: i64,
    /// Total observations added to the sketch
    pub observations: i64,
    /// Compression operation count
    pub total_compressions: i64,
    /// Bytes allocated for the sketch
    pub memory_usage: i64,
}

/// TDIGEST.INFO command - Get information about a t-digest sketch
///
/// Returns detailed information about the t-digest including compression settings,
/// node counts, weights, and memory usage.
///
/// Available since: RedisBloom 2.4.0
///
/// # Example
/// ```rust,no_run
/// # use redis_tower::modules::tdigest::TDigestInfo;
/// let cmd = TDigestInfo::new("latency");
/// ```
#[derive(Debug, Clone)]
pub struct TDigestInfo {
    key: String,
}

impl TDigestInfo {
    /// Create a new TDIGEST.INFO command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for TDigestInfo {
    type Response = TDigestInfoResult;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("TDIGEST.INFO"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut compression = 0;
                let mut capacity = 0;
                let mut merged_nodes = 0;
                let mut unmerged_nodes = 0;
                let mut merged_weight = 0;
                let mut unmerged_weight = 0;
                let mut observations = 0;
                let mut total_compressions = 0;
                let mut memory_usage = 0;

                let mut i = 0;
                while i < items.len() {
                    if let Frame::BulkString(Some(key)) = &items[i] {
                        let key_str = String::from_utf8_lossy(key);
                        if i + 1 < items.len() {
                            match key_str.as_ref() {
                                "Compression" => {
                                    if let Frame::Integer(v) = items[i + 1] {
                                        compression = v;
                                    }
                                }
                                "Capacity" => {
                                    if let Frame::Integer(v) = items[i + 1] {
                                        capacity = v;
                                    }
                                }
                                "Merged nodes" => {
                                    if let Frame::Integer(v) = items[i + 1] {
                                        merged_nodes = v;
                                    }
                                }
                                "Unmerged nodes" => {
                                    if let Frame::Integer(v) = items[i + 1] {
                                        unmerged_nodes = v;
                                    }
                                }
                                "Merged weight" => {
                                    if let Frame::Integer(v) = items[i + 1] {
                                        merged_weight = v;
                                    }
                                }
                                "Unmerged weight" => {
                                    if let Frame::Integer(v) = items[i + 1] {
                                        unmerged_weight = v;
                                    }
                                }
                                "Observations" => {
                                    if let Frame::Integer(v) = items[i + 1] {
                                        observations = v;
                                    }
                                }
                                "Total compressions" => {
                                    if let Frame::Integer(v) = items[i + 1] {
                                        total_compressions = v;
                                    }
                                }
                                "Memory usage" => {
                                    if let Frame::Integer(v) = items[i + 1] {
                                        memory_usage = v;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    i += 2;
                }

                Ok(TDigestInfoResult {
                    compression,
                    capacity,
                    merged_nodes,
                    unmerged_nodes,
                    merged_weight,
                    unmerged_weight,
                    observations,
                    total_compressions,
                    memory_usage,
                })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for TDigestInfo {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands
impl ReadOnly for TDigestCreate {}
impl ReadOnly for TDigestReset {}
impl ReadOnly for TDigestAdd {}
impl ReadOnly for TDigestMerge {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tdigest_create() {
        let cmd = TDigestCreate::new("latency");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("TDIGEST.CREATE")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_tdigest_create_with_compression() {
        let cmd = TDigestCreate::new("latency").compression(500);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("COMPRESSION")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("500")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_tdigest_add() {
        let cmd = TDigestAdd::new("latency", vec![45.2, 52.1, 38.9]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("TDIGEST.ADD")))
                );
                assert_eq!(parts.len(), 5); // TDIGEST.ADD + key + 3 values
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_tdigest_quantile() {
        let cmd = TDigestQuantile::new("latency", vec![0.50, 0.95, 0.99]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("TDIGEST.QUANTILE")))
                );
                assert_eq!(parts.len(), 5); // CMD + key + 3 quantiles
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_tdigest_cdf() {
        let cmd = TDigestCdf::new("latency", vec![50.0, 100.0]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("TDIGEST.CDF")))
                );
                assert_eq!(parts.len(), 4);
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_tdigest_trimmed_mean() {
        let cmd = TDigestTrimmedMean::new("latency", 0.05, 0.95);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("TDIGEST.TRIMMED_MEAN")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_tdigest_min_response() {
        let frame = Frame::BulkString(Some(Bytes::from("12.5")));
        let result = TDigestMin::parse_response(frame).unwrap();
        assert_eq!(result, 12.5);
    }

    #[test]
    fn test_tdigest_quantile_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("45.2"))),
            Frame::BulkString(Some(Bytes::from("112.3"))),
        ]);
        let result = TDigestQuantile::parse_response(frame).unwrap();
        assert_eq!(result, vec![45.2, 112.3]);
    }

    #[test]
    fn test_tdigest_rank_response() {
        let frame = Frame::Array(vec![Frame::Integer(520), Frame::Integer(870)]);
        let result = TDigestRank::parse_response(frame).unwrap();
        assert_eq!(result, vec![520, 870]);
    }

    #[test]
    fn test_tdigest_info_frame() {
        let cmd = TDigestInfo::new("latency");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("TDIGEST.INFO")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("latency"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_tdigest_info_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("Compression"))),
            Frame::Integer(100),
            Frame::BulkString(Some(Bytes::from("Capacity"))),
            Frame::Integer(600),
            Frame::BulkString(Some(Bytes::from("Merged nodes"))),
            Frame::Integer(50),
            Frame::BulkString(Some(Bytes::from("Unmerged nodes"))),
            Frame::Integer(10),
            Frame::BulkString(Some(Bytes::from("Merged weight"))),
            Frame::Integer(1000),
            Frame::BulkString(Some(Bytes::from("Unmerged weight"))),
            Frame::Integer(100),
            Frame::BulkString(Some(Bytes::from("Observations"))),
            Frame::Integer(1100),
            Frame::BulkString(Some(Bytes::from("Total compressions"))),
            Frame::Integer(5),
            Frame::BulkString(Some(Bytes::from("Memory usage"))),
            Frame::Integer(4096),
        ]);

        let result = TDigestInfo::parse_response(frame).unwrap();
        assert_eq!(result.compression, 100);
        assert_eq!(result.capacity, 600);
        assert_eq!(result.merged_nodes, 50);
        assert_eq!(result.unmerged_nodes, 10);
        assert_eq!(result.merged_weight, 1000);
        assert_eq!(result.unmerged_weight, 100);
        assert_eq!(result.observations, 1100);
        assert_eq!(result.total_compressions, 5);
        assert_eq!(result.memory_usage, 4096);
    }
}
