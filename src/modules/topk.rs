//! Redis Top-K commands
//!
//! Top-K is a probabilistic data structure that tracks the K most frequent items
//! in a stream. It uses a combination of techniques including heavy hitters
//! algorithms to efficiently maintain the top items.
//!
//! # Key Features
//! - **Space Efficient**: Tracks top-K items without storing all items
//! - **Fast Updates**: O(1) add and increment operations
//! - **Approximate**: May occasionally miss some top items but very accurate
//! - **Configurable**: Tune width/depth for accuracy vs memory tradeoff
//!
//! # Use Cases
//! - Trending topics / hashtags
//! - Most active users
//! - Popular products
//! - Frequent search queries
//! - Heavy hitter detection in network traffic
//!
//! # Examples
//! ```no_run
//! use redis_tower::modules::topk::{TopKReserve, TopKAdd, TopKList};
//! use redis_tower::RedisClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("localhost:6379").await?;
//!
//! // Create top-10 tracker
//! client.call(TopKReserve::new("trending", 10)
//!     .width(1000)
//!     .depth(5)
//! ).await?;
//!
//! // Add items (returns evicted items if any)
//! client.call(TopKAdd::new("trending", vec![b"#rust".to_vec()])).await?;
//!
//! // Get top-K list
//! let top: Vec<String> = client.call(TopKList::new("trending")).await?;
//! # Ok(())
//! # }
//! ```

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// TOPK.RESERVE - Create a Top-K filter
///
/// Creates a new Top-K filter that tracks the K most frequent items.
///
/// # Arguments
/// * `key` - Filter key name
/// * `topk` - Number of top items to track (K)
///
/// # Optional Parameters
/// * `width` - Number of counters (default: 8)
/// * `depth` - Number of hash functions (default: 7)
/// * `decay` - Decay factor for counters (default: 0.9)
///
/// # Example
/// ```no_run
/// use redis_tower::modules::topk::TopKReserve;
///
/// // Track top 10 items
/// let cmd = TopKReserve::new("trending", 10);
///
/// // With custom parameters
/// let cmd = TopKReserve::new("trending", 10)
///     .width(1000)
///     .depth(5)
///     .decay(0.95);
/// ```
#[derive(Debug, Clone)]
pub struct TopKReserve {
    key: String,
    topk: i64,
    width: Option<i64>,
    depth: Option<i64>,
    decay: Option<f64>,
}

impl TopKReserve {
    /// Create a new TOPK.RESERVE command
    pub fn new(key: impl Into<String>, topk: i64) -> Self {
        Self {
            key: key.into(),
            topk,
            width: None,
            depth: None,
            decay: None,
        }
    }

    /// Set width (number of counters)
    pub fn width(mut self, width: i64) -> Self {
        self.width = Some(width);
        self
    }

    /// Set depth (number of hash functions)
    pub fn depth(mut self, depth: i64) -> Self {
        self.depth = Some(depth);
        self
    }

    /// Set decay factor
    pub fn decay(mut self, decay: f64) -> Self {
        self.decay = Some(decay);
        self
    }
}

impl Command for TopKReserve {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TOPK.RESERVE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.topk.to_string()))),
        ];

        if let Some(w) = self.width {
            frames.push(Frame::BulkString(Some(Bytes::from(w.to_string()))));

            if let Some(d) = self.depth {
                frames.push(Frame::BulkString(Some(Bytes::from(d.to_string()))));

                if let Some(decay) = self.decay {
                    frames.push(Frame::BulkString(Some(Bytes::from(decay.to_string()))));
                }
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

/// TOPK.ADD - Add one or more items to the Top-K filter
///
/// Adds items to the filter. If the filter is full and an item is not in the
/// top-K, it may evict another item. Returns the items that were evicted (if any).
///
/// # Example
/// ```no_run
/// use redis_tower::modules::topk::TopKAdd;
///
/// let cmd = TopKAdd::new("trending", vec![
///     b"#rust".to_vec(),
///     b"#redis".to_vec(),
/// ]);
/// // Response: vec![None, None] - no evictions
/// // or vec![Some(b"#old".to_vec()), None] - first item evicted #old
/// ```
#[derive(Debug, Clone)]
pub struct TopKAdd {
    key: String,
    items: Vec<Bytes>,
}

impl TopKAdd {
    /// Create a new TOPK.ADD command
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

impl Command for TopKAdd {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TOPK.ADD"))),
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
                        Frame::BulkString(Some(data)) => results.push(Some(data)),
                        Frame::BulkString(None) => results.push(None),
                        Frame::Null => results.push(None),
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

/// TOPK.INCRBY - Increment the score of one or more items
///
/// Increments the count of items by specified amounts. Returns items that
/// were evicted from the top-K (if any).
///
/// # Example
/// ```no_run
/// use redis_tower::modules::topk::TopKIncrBy;
///
/// let cmd = TopKIncrBy::new("trending")
///     .item("#rust", 10)
///     .item("#redis", 5);
/// // Response: vec![None, None] - no evictions
/// ```
#[derive(Debug, Clone)]
pub struct TopKIncrBy {
    key: String,
    items: Vec<(Bytes, i64)>, // (item, increment)
}

impl TopKIncrBy {
    /// Create a new TOPK.INCRBY command
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

impl Command for TopKIncrBy {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TOPK.INCRBY"))),
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
                        Frame::BulkString(Some(data)) => results.push(Some(data)),
                        Frame::BulkString(None) => results.push(None),
                        Frame::Null => results.push(None),
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

/// TOPK.QUERY - Check if items are in the Top-K
///
/// Checks whether one or more items are in the top-K.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::topk::TopKQuery;
///
/// let cmd = TopKQuery::new("trending", vec![
///     b"#rust".to_vec(),
///     b"#unknown".to_vec(),
/// ]);
/// // Response: vec![true, false] - first is in top-K, second is not
/// ```
#[derive(Debug, Clone)]
pub struct TopKQuery {
    key: String,
    items: Vec<Bytes>,
}

impl TopKQuery {
    /// Create a new TOPK.QUERY command
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

impl Command for TopKQuery {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TOPK.QUERY"))),
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
                        Frame::Integer(n) => results.push(n != 0),
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

/// TOPK.COUNT - Get counts of one or more items
///
/// Returns the approximate count for each item. Items not in the top-K return 0.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::topk::TopKCount;
///
/// let cmd = TopKCount::new("trending", vec![
///     b"#rust".to_vec(),
///     b"#redis".to_vec(),
/// ]);
/// // Response: vec![142, 87] - approximate counts
/// ```
#[derive(Debug, Clone)]
pub struct TopKCount {
    key: String,
    items: Vec<Bytes>,
}

impl TopKCount {
    /// Create a new TOPK.COUNT command
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

impl Command for TopKCount {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TOPK.COUNT"))),
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

/// TOPK.LIST - Get the list of top-K items
///
/// Returns the current list of top-K items. Optionally include their counts.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::topk::TopKList;
///
/// // Just the items
/// let cmd = TopKList::new("trending");
/// // Response: vec!["#rust", "#redis", "#tokio"]
///
/// // With counts
/// let cmd = TopKList::new("trending").with_count();
/// // Response: TopKListResult with items and counts
/// ```
#[derive(Debug, Clone)]
pub struct TopKList {
    key: String,
    with_count: bool,
}

impl TopKList {
    /// Create a new TOPK.LIST command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            with_count: false,
        }
    }

    /// Include counts in the response
    pub fn with_count(mut self) -> Self {
        self.with_count = true;
        self
    }
}

/// Result from TOPK.LIST command
#[derive(Debug, Clone, PartialEq)]
pub struct TopKListResult {
    /// The top-K items with their counts
    pub items: Vec<(Bytes, i64)>,
}

impl Command for TopKList {
    type Response = TopKListResult;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("TOPK.LIST"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if self.with_count {
            frames.push(Frame::BulkString(Some(Bytes::from("WITHCOUNT"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result_items = Vec::new();

                if items.is_empty() {
                    return Ok(TopKListResult {
                        items: result_items,
                    });
                }

                // Check if first item is an array (WITHCOUNT format)
                if matches!(items.first(), Some(Frame::Array(_))) {
                    // WITHCOUNT format: array of [item, count] pairs
                    for item in items {
                        match item {
                            Frame::Array(mut pair) if pair.len() == 2 => {
                                let count = pair.pop().unwrap();
                                let key = pair.pop().unwrap();

                                match (key, count) {
                                    (Frame::BulkString(Some(k)), Frame::BulkString(Some(c))) => {
                                        // Count is returned as string, parse it
                                        let count_str = String::from_utf8_lossy(&c);
                                        let count_val = count_str.parse::<i64>().unwrap_or(0);
                                        result_items.push((k, count_val));
                                    }
                                    _ => return Err(RedisError::UnexpectedResponse),
                                }
                            }
                            _ => return Err(RedisError::UnexpectedResponse),
                        }
                    }
                } else {
                    // Without count: just array of items
                    for item in items {
                        match item {
                            Frame::BulkString(Some(data)) => result_items.push((data, 0)),
                            _ => return Err(RedisError::UnexpectedResponse),
                        }
                    }
                }

                Ok(TopKListResult {
                    items: result_items,
                })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TOPK.INFO - Get information about a Top-K filter
///
/// Returns metadata about the filter including K, width, depth, and decay.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::topk::TopKInfo;
///
/// let cmd = TopKInfo::new("trending");
/// // Response: TopKInfoResult with filter statistics
/// ```
#[derive(Debug, Clone)]
pub struct TopKInfo {
    key: String,
}

impl TopKInfo {
    /// Create a new TOPK.INFO command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

/// Result from TOPK.INFO command
#[derive(Debug, Clone, PartialEq)]
pub struct TopKInfoResult {
    /// The K value (number of top items tracked)
    pub k: i64,
    /// Width (number of counters)
    pub width: i64,
    /// Depth (number of hash functions)
    pub depth: i64,
    /// Decay factor
    pub decay: f64,
}

impl Command for TopKInfo {
    type Response = TopKInfoResult;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("TOPK.INFO"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                // TOPK.INFO returns array of alternating field names and values
                let mut k = 0;
                let mut width = 0;
                let mut depth = 0;
                let mut decay = 0.0;

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

                    match field_name.as_ref() {
                        "k" => {
                            if let Frame::Integer(n) = &items[i + 1] {
                                k = *n;
                            }
                        }
                        "width" => {
                            if let Frame::Integer(n) = &items[i + 1] {
                                width = *n;
                            }
                        }
                        "depth" => {
                            if let Frame::Integer(n) = &items[i + 1] {
                                depth = *n;
                            }
                        }
                        "decay" => {
                            if let Frame::BulkString(Some(d)) = &items[i + 1] {
                                let decay_str = String::from_utf8_lossy(d);
                                decay = decay_str.parse::<f64>().unwrap_or(0.0);
                            }
                        }
                        _ => {}
                    }

                    i += 2;
                }

                Ok(TopKInfoResult {
                    k,
                    width,
                    depth,
                    decay,
                })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// Read-only trait implementations
use crate::read_preference::ReadOnly;

impl ReadOnly for TopKQuery {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for TopKCount {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for TopKList {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for TopKInfo {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands
impl ReadOnly for TopKReserve {}
impl ReadOnly for TopKAdd {}
impl ReadOnly for TopKIncrBy {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topk_reserve_basic() {
        let cmd = TopKReserve::new("trending", 10);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("TOPK.RESERVE")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("trending"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("10"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_topk_reserve_with_params() {
        let cmd = TopKReserve::new("trending", 10)
            .width(1000)
            .depth(5)
            .decay(0.95);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 6);
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("1000")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("5")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_topk_add() {
        let cmd = TopKAdd::new(
            "trending",
            vec![Bytes::from("#rust"), Bytes::from("#redis")],
        );
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TOPK.ADD"))));
                assert_eq!(parts.len(), 4); // TOPK.ADD + key + 2 items
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_topk_incrby() {
        let cmd = TopKIncrBy::new("trending")
            .item("#rust", 10)
            .item("#redis", 5);

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("TOPK.INCRBY")))
                );
                assert_eq!(parts.len(), 6); // TOPK.INCRBY + key + 2*(item+count)
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_topk_query() {
        let cmd = TopKQuery::new("trending", vec![Bytes::from("#rust")]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TOPK.QUERY"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_topk_count() {
        let cmd = TopKCount::new("trending", vec![Bytes::from("#rust")]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TOPK.COUNT"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_topk_list() {
        let cmd = TopKList::new("trending");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TOPK.LIST"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_topk_list_with_count() {
        let cmd = TopKList::new("trending").with_count();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("WITHCOUNT")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_topk_info() {
        let cmd = TopKInfo::new("trending");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TOPK.INFO"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_topk_add_response() {
        let frame = Frame::Array(vec![Frame::BulkString(None), Frame::BulkString(None)]);
        let result = TopKAdd::parse_response(frame).unwrap();
        assert_eq!(result, vec![None, None]);
    }

    #[test]
    fn test_topk_query_response() {
        let frame = Frame::Array(vec![Frame::Integer(1), Frame::Integer(0)]);
        let result = TopKQuery::parse_response(frame).unwrap();
        assert_eq!(result, vec![true, false]);
    }

    #[test]
    fn test_topk_count_response() {
        let frame = Frame::Array(vec![Frame::Integer(142), Frame::Integer(87)]);
        let result = TopKCount::parse_response(frame).unwrap();
        assert_eq!(result, vec![142, 87]);
    }

    #[test]
    fn test_topk_list_response_without_count() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("#rust"))),
            Frame::BulkString(Some(Bytes::from("#redis"))),
        ]);
        let result = TopKList::parse_response(frame).unwrap();
        assert_eq!(result.items.len(), 2);
        assert_eq!(result.items[0].0, Bytes::from("#rust"));
        assert_eq!(result.items[0].1, 0); // No count
    }
}
