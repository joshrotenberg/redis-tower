//! Redis Count-Min Sketch commands
//!
//! Count-Min Sketch is a probabilistic data structure for frequency estimation.
//! It uses multiple hash functions and a compact array to estimate the frequency
//! of items in a stream with configurable accuracy.
//!
//! # Key Features
//! - **Space Efficient**: Uses much less memory than exact counters
//! - **Fast Updates**: O(1) increment operations
//! - **Never Underestimates**: May overestimate but never underestimates counts
//! - **Mergeable**: Multiple sketches can be combined
//!
//! # Use Cases
//! - Heavy hitters detection (most frequent items)
//! - Frequency analysis in streams
//! - Rate limiting / quota tracking
//! - Network traffic analysis
//! - Cache admission policies
//!
//! # Examples
//! ```no_run
//! use redis_tower::modules::cms::{CmsInitByDim, CmsIncrBy, CmsQuery};
//! use redis_tower::RedisClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("localhost:6379").await?;
//!
//! // Initialize with dimensions (width x depth)
//! client.call(CmsInitByDim::new("pageviews", 2000, 5)).await?;
//!
//! // Increment counts for items
//! client.call(CmsIncrBy::new("pageviews")
//!     .item("/home", 10)
//!     .item("/about", 5)
//! ).await?;
//!
//! // Query count estimate
//! let count: i64 = client.call(CmsQuery::new("pageviews", "/home")).await?;
//! println!("Approximate count: {}", count);
//! # Ok(())
//! # }
//! ```

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// CMS.INITBYDIM - Initialize Count-Min Sketch by dimensions
///
/// Creates a new Count-Min Sketch with specified width and depth.
/// - Width: Number of counters per row (affects space usage)
/// - Depth: Number of hash functions (affects accuracy)
///
/// Larger dimensions = better accuracy but more memory.
///
/// # Arguments
/// * `key` - Sketch key name
/// * `width` - Number of counters per row (typically 1000-10000)
/// * `depth` - Number of rows/hash functions (typically 5-10)
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cms::CmsInitByDim;
///
/// // 2000 width, 5 depth = good balance for most use cases
/// let cmd = CmsInitByDim::new("mysketch", 2000, 5);
/// ```
#[derive(Debug, Clone)]
pub struct CmsInitByDim {
    key: String,
    width: i64,
    depth: i64,
}

impl CmsInitByDim {
    /// Create a new CMS.INITBYDIM command
    pub fn new(key: impl Into<String>, width: i64, depth: i64) -> Self {
        Self {
            key: key.into(),
            width,
            depth,
        }
    }
}

impl Command for CmsInitByDim {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CMS.INITBYDIM"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.width.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.depth.to_string()))),
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

/// CMS.INITBYPROB - Initialize Count-Min Sketch by error probability
///
/// Creates a new Count-Min Sketch by specifying desired error rate and probability.
/// The dimensions are calculated automatically to achieve the specified accuracy.
///
/// # Arguments
/// * `key` - Sketch key name
/// * `error` - Acceptable error rate (e.g., 0.001 for 0.1% error)
/// * `probability` - Confidence level (e.g., 0.99 for 99% confidence)
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cms::CmsInitByProb;
///
/// // 0.1% error with 99% confidence
/// let cmd = CmsInitByProb::new("mysketch", 0.001, 0.99);
/// ```
#[derive(Debug, Clone)]
pub struct CmsInitByProb {
    key: String,
    error: f64,
    probability: f64,
}

impl CmsInitByProb {
    /// Create a new CMS.INITBYPROB command
    pub fn new(key: impl Into<String>, error: f64, probability: f64) -> Self {
        Self {
            key: key.into(),
            error,
            probability,
        }
    }
}

impl Command for CmsInitByProb {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CMS.INITBYPROB"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.error.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.probability.to_string()))),
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

/// CMS.INCRBY - Increment counts for one or more items
///
/// Increments the count for one or more items by specified amounts.
/// Returns the estimated count for each item after incrementing.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cms::CmsIncrBy;
///
/// // Increment multiple items
/// let cmd = CmsIncrBy::new("pageviews")
///     .item("/home", 10)
///     .item("/about", 5)
///     .item("/contact", 1);
/// // Response: vec![10, 5, 1] - estimated counts after increment
/// ```
#[derive(Debug, Clone)]
pub struct CmsIncrBy {
    key: String,
    items: Vec<(Bytes, i64)>, // (item, increment)
}

impl CmsIncrBy {
    /// Create a new CMS.INCRBY command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            items: Vec::new(),
        }
    }

    /// Add an item with increment amount
    pub fn item(mut self, item: impl Into<Bytes>, increment: i64) -> Self {
        self.items.push((item.into(), increment));
        self
    }

    /// Add multiple items at once
    pub fn items(mut self, items: Vec<(Bytes, i64)>) -> Self {
        self.items.extend(items);
        self
    }
}

impl Command for CmsIncrBy {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CMS.INCRBY"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for (item, count) in &self.items {
            frames.push(Frame::BulkString(Some(item.clone())));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
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

/// CMS.QUERY - Query the count of one or more items
///
/// Returns the estimated count for each queried item.
/// The estimate may be higher than the actual count (never lower).
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cms::CmsQuery;
///
/// // Query single item
/// let cmd = CmsQuery::new("pageviews", vec![b"/home".to_vec()]);
/// // Response: vec![42] - estimated count
///
/// // Query multiple items
/// let cmd = CmsQuery::new("pageviews", vec![
///     b"/home".to_vec(),
///     b"/about".to_vec(),
/// ]);
/// // Response: vec![42, 17] - estimated counts
/// ```
#[derive(Debug, Clone)]
pub struct CmsQuery {
    key: String,
    items: Vec<Bytes>,
}

impl CmsQuery {
    /// Create a new CMS.QUERY command
    pub fn new(key: impl Into<String>, items: Vec<Bytes>) -> Self {
        Self {
            key: key.into(),
            items,
        }
    }

    /// Create from a single item
    pub fn single(key: impl Into<String>, item: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            items: vec![item.into()],
        }
    }
}

impl Command for CmsQuery {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CMS.QUERY"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for item in &self.items {
            frames.push(Frame::BulkString(Some(item.clone())));
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

/// CMS.MERGE - Merge multiple Count-Min Sketches
///
/// Merges multiple sketches into a destination sketch. All sketches must have
/// the same dimensions. Optionally apply weights to each source sketch.
///
/// # Arguments
/// * `dest` - Destination sketch key
/// * `sources` - Number of source sketches
/// * `src_keys` - Source sketch keys
///
/// # Optional
/// * `weights` - Weight for each source (default: 1 for all)
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cms::CmsMerge;
///
/// // Simple merge (equal weights)
/// let cmd = CmsMerge::new("merged", 2, vec!["sketch1".into(), "sketch2".into()]);
///
/// // Merge with weights
/// let cmd = CmsMerge::new("merged", 2, vec!["sketch1".into(), "sketch2".into()])
///     .weights(vec![2, 1]); // sketch1 weighted 2x
/// ```
#[derive(Debug, Clone)]
pub struct CmsMerge {
    dest: String,
    num_keys: i64,
    src_keys: Vec<String>,
    weights: Option<Vec<i64>>,
}

impl CmsMerge {
    /// Create a new CMS.MERGE command
    pub fn new(dest: impl Into<String>, num_keys: i64, src_keys: Vec<String>) -> Self {
        Self {
            dest: dest.into(),
            num_keys,
            src_keys,
            weights: None,
        }
    }

    /// Set weights for source sketches
    pub fn weights(mut self, weights: Vec<i64>) -> Self {
        self.weights = Some(weights);
        self
    }
}

impl Command for CmsMerge {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CMS.MERGE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.dest.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.num_keys.to_string()))),
        ];

        for key in &self.src_keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        if let Some(ref weights) = self.weights {
            frames.push(Frame::BulkString(Some(Bytes::from("WEIGHTS"))));
            for weight in weights {
                frames.push(Frame::BulkString(Some(Bytes::from(weight.to_string()))));
            }
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

/// CMS.INFO - Get information about a Count-Min Sketch
///
/// Returns metadata about the sketch including width, depth, and total count.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cms::CmsInfo;
///
/// let cmd = CmsInfo::new("mysketch");
/// // Response: CmsInfoResult with sketch statistics
/// ```
#[derive(Debug, Clone)]
pub struct CmsInfo {
    key: String,
}

impl CmsInfo {
    /// Create a new CMS.INFO command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

/// Result from CMS.INFO command
#[derive(Debug, Clone, PartialEq)]
pub struct CmsInfoResult {
    /// Width (counters per row)
    pub width: i64,
    /// Depth (number of rows/hash functions)
    pub depth: i64,
    /// Total count across all items
    pub count: i64,
}

impl Command for CmsInfo {
    type Response = CmsInfoResult;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CMS.INFO"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                // CMS.INFO returns array of alternating field names and values
                let mut width = 0;
                let mut depth = 0;
                let mut count = 0;

                let mut i = 0;
                while i < items.len() {
                    if i + 1 >= items.len() {
                        break;
                    }

                    let field_name = match &items[i] {
                        Frame::BulkString(Some(name)) => String::from_utf8_lossy(name),
                        _ => {
                            i += 2;
                            continue;
                        }
                    };

                    let value = match &items[i + 1] {
                        Frame::Integer(n) => *n,
                        _ => {
                            i += 2;
                            continue;
                        }
                    };

                    match field_name.as_ref() {
                        "width" => width = value,
                        "depth" => depth = value,
                        "count" => count = value,
                        _ => {}
                    }

                    i += 2;
                }

                Ok(CmsInfoResult {
                    width,
                    depth,
                    count,
                })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// Read-only trait implementations
use crate::read_preference::ReadOnly;

impl ReadOnly for CmsQuery {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for CmsInfo {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands
impl ReadOnly for CmsInitByDim {}
impl ReadOnly for CmsInitByProb {}
impl ReadOnly for CmsIncrBy {}
impl ReadOnly for CmsMerge {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cms_initbydim() {
        let cmd = CmsInitByDim::new("mysketch", 2000, 5);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("CMS.INITBYDIM")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("mysketch"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("2000"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("5"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cms_initbyprob() {
        let cmd = CmsInitByProb::new("mysketch", 0.001, 0.99);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("CMS.INITBYPROB")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("mysketch"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cms_incrby() {
        let cmd = CmsIncrBy::new("pageviews")
            .item("/home", 10)
            .item("/about", 5);

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CMS.INCRBY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("pageviews"))));
                assert_eq!(parts.len(), 6); // CMS.INCRBY + key + 2*(item+count)
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cms_query_single() {
        let cmd = CmsQuery::single("pageviews", Bytes::from("/home"));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CMS.QUERY"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cms_query_multiple() {
        let cmd = CmsQuery::new(
            "pageviews",
            vec![Bytes::from("/home"), Bytes::from("/about")],
        );
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4); // CMS.QUERY + key + 2 items
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cms_merge() {
        let cmd = CmsMerge::new("merged", 2, vec!["sketch1".into(), "sketch2".into()]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CMS.MERGE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("merged"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("2"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cms_merge_with_weights() {
        let cmd = CmsMerge::new("merged", 2, vec!["sketch1".into(), "sketch2".into()])
            .weights(vec![2, 1]);

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("WEIGHTS")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cms_info() {
        let cmd = CmsInfo::new("mysketch");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CMS.INFO"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cms_incrby_response() {
        let frame = Frame::Array(vec![Frame::Integer(10), Frame::Integer(5)]);
        let result = CmsIncrBy::parse_response(frame).unwrap();
        assert_eq!(result, vec![10, 5]);
    }

    #[test]
    fn test_cms_query_response() {
        let frame = Frame::Array(vec![Frame::Integer(42), Frame::Integer(17)]);
        let result = CmsQuery::parse_response(frame).unwrap();
        assert_eq!(result, vec![42, 17]);
    }

    #[test]
    fn test_cms_info_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("width"))),
            Frame::Integer(2000),
            Frame::BulkString(Some(Bytes::from("depth"))),
            Frame::Integer(5),
            Frame::BulkString(Some(Bytes::from("count"))),
            Frame::Integer(1000),
        ]);

        let result = CmsInfo::parse_response(frame).unwrap();
        assert_eq!(result.width, 2000);
        assert_eq!(result.depth, 5);
        assert_eq!(result.count, 1000);
    }
}
