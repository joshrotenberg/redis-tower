//! Redis Bloom Filter module commands
//!
//! Provides probabilistic data structures for membership testing with configurable
//! false positive rates. Requires RedisBloom module to be loaded on the Redis server.
//!
//! # Feature Gate
//! This module is only available when the `bloom` feature is enabled:
//! ```toml
//! redis-tower = { version = "0.1", features = ["bloom"] }
//! ```
//!
//! # Key Commands
//! - `BF.ADD` - Add item to bloom filter
//! - `BF.MADD` - Add multiple items
//! - `BF.EXISTS` - Check if item exists
//! - `BF.MEXISTS` - Check multiple items
//! - `BF.RESERVE` - Create filter with custom parameters
//! - `BF.INFO` - Get filter information
//!
//! # Example
//! ```no_run
//! use redis_tower::modules::bloom::{BfAdd, BfExists, BfReserve};
//! use redis_tower::RedisClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("localhost:6379").await?;
//!
//! // Create a bloom filter with 0.01 error rate and 1000 capacity
//! client.call(BfReserve::new("myfilter", 0.01, 1000)).await?;
//!
//! // Add items
//! let added = client.call(BfAdd::new("myfilter", "item1")).await?;
//! println!("Item added: {}", added); // true if new, false if existed
//!
//! // Check existence
//! let exists = client.call(BfExists::new("myfilter", "item1")).await?;
//! println!("Item exists: {}", exists);
//! # Ok(())
//! # }
//! ```

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// BF.RESERVE - Create a bloom filter with custom parameters
///
/// Creates a new bloom filter with specified error rate and initial capacity.
/// Must be called before adding items if you want custom parameters.
///
/// # Arguments
/// * `key` - Filter key name
/// * `error_rate` - Desired false positive rate (e.g., 0.01 for 1%)
/// * `capacity` - Initial capacity (number of expected items)
///
/// # Optional Parameters
/// * `expansion` - Expansion factor when capacity is reached (default: 2)
/// * `nonscaling` - Prevent auto-scaling when capacity exceeded
///
/// # Example
/// ```no_run
/// use redis_tower::modules::bloom::BfReserve;
///
/// // Basic: 0.1% error rate, 10000 items
/// let cmd = BfReserve::new("myfilter", 0.001, 10000);
///
/// // With expansion factor
/// let cmd = BfReserve::new("myfilter", 0.01, 1000).expansion(4);
///
/// // Non-scaling (fixed size)
/// let cmd = BfReserve::new("myfilter", 0.01, 1000).nonscaling();
/// ```
#[derive(Debug, Clone)]
pub struct BfReserve {
    key: String,
    error_rate: f64,
    capacity: i64,
    expansion: Option<i64>,
    nonscaling: bool,
}

impl BfReserve {
    /// Create a new BF.RESERVE command
    pub fn new(key: impl Into<String>, error_rate: f64, capacity: i64) -> Self {
        Self {
            key: key.into(),
            error_rate,
            capacity,
            expansion: None,
            nonscaling: false,
        }
    }

    /// Set expansion factor for auto-scaling
    pub fn expansion(mut self, factor: i64) -> Self {
        self.expansion = Some(factor);
        self
    }

    /// Disable auto-scaling (filter becomes fixed size)
    pub fn nonscaling(mut self) -> Self {
        self.nonscaling = true;
        self
    }
}

impl Command for BfReserve {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("BF.RESERVE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.error_rate.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.capacity.to_string()))),
        ];

        if let Some(exp) = self.expansion {
            frames.push(Frame::BulkString(Some(Bytes::from("EXPANSION"))));
            frames.push(Frame::BulkString(Some(Bytes::from(exp.to_string()))));
        }

        if self.nonscaling {
            frames.push(Frame::BulkString(Some(Bytes::from("NONSCALING"))));
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

/// BF.ADD - Add an item to a bloom filter
///
/// Adds an item to the bloom filter. If the filter doesn't exist, it's created
/// with default parameters (error_rate=0.01, capacity=100).
///
/// Returns true if the item was probably not in the filter before (new),
/// false if it probably was already there.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::bloom::BfAdd;
///
/// let cmd = BfAdd::new("myfilter", "user123");
/// // Response: true if newly added, false if already existed
/// ```
#[derive(Debug, Clone)]
pub struct BfAdd {
    key: String,
    item: Bytes,
}

impl BfAdd {
    /// Create a new BF.ADD command
    pub fn new(key: impl Into<String>, item: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for BfAdd {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("BF.ADD"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(self.item.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n != 0),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// BF.MADD - Add multiple items to a bloom filter
///
/// Adds multiple items to the bloom filter in a single command.
/// Returns a boolean array indicating for each item whether it was newly added.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::bloom::BfMadd;
///
/// let cmd = BfMadd::new("myfilter", vec![
///     b"user123".to_vec(),
///     b"user456".to_vec(),
///     b"user789".to_vec(),
/// ]);
/// // Response: vec![true, true, false] - first two new, third existed
/// ```
#[derive(Debug, Clone)]
pub struct BfMadd {
    key: String,
    items: Vec<Bytes>,
}

impl BfMadd {
    /// Create a new BF.MADD command
    pub fn new(key: impl Into<String>, items: Vec<Bytes>) -> Self {
        Self {
            key: key.into(),
            items,
        }
    }

    /// Create from items that implement Into<Bytes>
    pub fn from_items<I, T>(key: impl Into<String>, items: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Bytes>,
    {
        Self {
            key: key.into(),
            items: items.into_iter().map(|i| i.into()).collect(),
        }
    }
}

impl Command for BfMadd {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("BF.MADD"))),
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

/// BF.EXISTS - Check if an item exists in a bloom filter
///
/// Checks if an item probably exists in the filter.
/// - Returns true: item MAY be in the set (possible false positive)
/// - Returns false: item is DEFINITELY NOT in the set (no false negatives)
///
/// # Example
/// ```no_run
/// use redis_tower::modules::bloom::BfExists;
///
/// let cmd = BfExists::new("myfilter", "user123");
/// // Response: true if probably exists, false if definitely doesn't
/// ```
#[derive(Debug, Clone)]
pub struct BfExists {
    key: String,
    item: Bytes,
}

impl BfExists {
    /// Create a new BF.EXISTS command
    pub fn new(key: impl Into<String>, item: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for BfExists {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("BF.EXISTS"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(self.item.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n != 0),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// BF.MEXISTS - Check if multiple items exist in a bloom filter
///
/// Checks multiple items in a single command.
/// Returns a boolean array with results for each item.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::bloom::BfMexists;
///
/// let cmd = BfMexists::new("myfilter", vec![
///     b"user123".to_vec(),
///     b"user456".to_vec(),
/// ]);
/// // Response: vec![true, false] - first exists, second doesn't
/// ```
#[derive(Debug, Clone)]
pub struct BfMexists {
    key: String,
    items: Vec<Bytes>,
}

impl BfMexists {
    /// Create a new BF.MEXISTS command
    pub fn new(key: impl Into<String>, items: Vec<Bytes>) -> Self {
        Self {
            key: key.into(),
            items,
        }
    }

    /// Create from items that implement Into<Bytes>
    pub fn from_items<I, T>(key: impl Into<String>, items: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Bytes>,
    {
        Self {
            key: key.into(),
            items: items.into_iter().map(|i| i.into()).collect(),
        }
    }
}

impl Command for BfMexists {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("BF.MEXISTS"))),
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

/// BF.INFO - Get information about a bloom filter
///
/// Returns metadata about the filter including capacity, size, number of filters,
/// number of items inserted, and expansion rate.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::bloom::BfInfo;
///
/// let cmd = BfInfo::new("myfilter");
/// // Response: BfInfoResult with filter statistics
/// ```
#[derive(Debug, Clone)]
pub struct BfInfo {
    key: String,
}

impl BfInfo {
    /// Create a new BF.INFO command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

/// Result from BF.INFO command
#[derive(Debug, Clone, PartialEq)]
pub struct BfInfoResult {
    /// Total capacity of the filter
    pub capacity: i64,
    /// Total size in bytes
    pub size: i64,
    /// Number of sub-filters
    pub num_filters: i64,
    /// Number of items inserted
    pub num_items_inserted: i64,
    /// Expansion rate when capacity is reached
    pub expansion_rate: i64,
}

impl Command for BfInfo {
    type Response = BfInfoResult;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("BF.INFO"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                // BF.INFO returns array of alternating field names and values
                let mut capacity = 0;
                let mut size = 0;
                let mut num_filters = 0;
                let mut num_items_inserted = 0;
                let mut expansion_rate = 0;

                let mut i = 0;
                while i < items.len() {
                    if i + 1 >= items.len() {
                        break;
                    }

                    let field_name = match &items[i] {
                        Frame::BulkString(Some(name)) => String::from_utf8_lossy(name),
                        _ => continue,
                    };

                    let value = match &items[i + 1] {
                        Frame::Integer(n) => *n,
                        _ => {
                            i += 2;
                            continue;
                        }
                    };

                    match field_name.as_ref() {
                        "Capacity" => capacity = value,
                        "Size" => size = value,
                        "Number of filters" => num_filters = value,
                        "Number of items inserted" => num_items_inserted = value,
                        "Expansion rate" => expansion_rate = value,
                        _ => {}
                    }

                    i += 2;
                }

                Ok(BfInfoResult {
                    capacity,
                    size,
                    num_filters,
                    num_items_inserted,
                    expansion_rate,
                })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// BF.INSERT - Add items with creation options
///
/// More flexible version of BF.ADD that allows creating filters on-the-fly
/// with custom parameters and adding multiple items.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::bloom::BfInsert;
///
/// // Simple insert (creates filter with defaults if needed)
/// let cmd = BfInsert::new("myfilter", vec![b"item1".to_vec()]);
///
/// // With custom parameters if filter doesn't exist
/// let cmd = BfInsert::new("myfilter", vec![b"item1".to_vec()])
///     .capacity(10000)
///     .error(0.001)
///     .nocreate(); // Fail if filter doesn't exist
/// ```
#[derive(Debug, Clone)]
pub struct BfInsert {
    key: String,
    items: Vec<Bytes>,
    capacity: Option<i64>,
    error: Option<f64>,
    expansion: Option<i64>,
    nocreate: bool,
    nonscaling: bool,
}

impl BfInsert {
    /// Create a new BF.INSERT command
    pub fn new(key: impl Into<String>, items: Vec<Bytes>) -> Self {
        Self {
            key: key.into(),
            items,
            capacity: None,
            error: None,
            expansion: None,
            nocreate: false,
            nonscaling: false,
        }
    }

    /// Set capacity if filter needs to be created
    pub fn capacity(mut self, capacity: i64) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// Set error rate if filter needs to be created
    pub fn error(mut self, error: f64) -> Self {
        self.error = Some(error);
        self
    }

    /// Set expansion factor if filter needs to be created
    pub fn expansion(mut self, expansion: i64) -> Self {
        self.expansion = Some(expansion);
        self
    }

    /// Don't create filter if it doesn't exist (fail instead)
    pub fn nocreate(mut self) -> Self {
        self.nocreate = true;
        self
    }

    /// Prevent auto-scaling
    pub fn nonscaling(mut self) -> Self {
        self.nonscaling = true;
        self
    }
}

impl Command for BfInsert {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("BF.INSERT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(cap) = self.capacity {
            frames.push(Frame::BulkString(Some(Bytes::from("CAPACITY"))));
            frames.push(Frame::BulkString(Some(Bytes::from(cap.to_string()))));
        }

        if let Some(err) = self.error {
            frames.push(Frame::BulkString(Some(Bytes::from("ERROR"))));
            frames.push(Frame::BulkString(Some(Bytes::from(err.to_string()))));
        }

        if let Some(exp) = self.expansion {
            frames.push(Frame::BulkString(Some(Bytes::from("EXPANSION"))));
            frames.push(Frame::BulkString(Some(Bytes::from(exp.to_string()))));
        }

        if self.nocreate {
            frames.push(Frame::BulkString(Some(Bytes::from("NOCREATE"))));
        }

        if self.nonscaling {
            frames.push(Frame::BulkString(Some(Bytes::from("NONSCALING"))));
        }

        frames.push(Frame::BulkString(Some(Bytes::from("ITEMS"))));

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

/// BF.CARD - Get the cardinality (approximate count of items) in the filter
///
/// Returns an approximation of the number of unique items added to the filter.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::bloom::BfCard;
///
/// let cmd = BfCard::new("myfilter");
/// // Response: i64 - approximate count of items
/// ```
#[derive(Debug, Clone)]
pub struct BfCard {
    key: String,
}

impl BfCard {
    /// Create a new BF.CARD command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for BfCard {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("BF.CARD"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
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

/// BF.SCANDUMP - Begin incremental save of the bloom filter
///
/// Returns an iterator (state) and data chunk. Used for migrating bloom filters.
/// Call repeatedly with the returned iterator until it returns 0.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::bloom::BfScanDump;
///
/// let cmd = BfScanDump::new("myfilter", 0); // Start with iterator 0
/// // Response: (next_iterator, data_chunk)
/// // Keep calling with returned iterator until it's 0
/// ```
#[derive(Debug, Clone)]
pub struct BfScanDump {
    key: String,
    iterator: i64,
}

impl BfScanDump {
    /// Create a new BF.SCANDUMP command
    /// Start with iterator = 0, then use returned iterator for next call
    pub fn new(key: impl Into<String>, iterator: i64) -> Self {
        Self {
            key: key.into(),
            iterator,
        }
    }
}

/// Result from BF.SCANDUMP - (next_iterator, data)
/// When next_iterator is 0, the dump is complete
pub type BfScanDumpResult = (i64, Bytes);

impl Command for BfScanDump {
    type Response = BfScanDumpResult;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("BF.SCANDUMP"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.iterator.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(mut items) if items.len() == 2 => {
                let data = items.pop().unwrap();
                let iter = items.pop().unwrap();

                match (iter, data) {
                    (Frame::Integer(i), Frame::BulkString(Some(d))) => Ok((i, d)),
                    (Frame::Integer(i), Frame::BulkString(None)) => Ok((i, Bytes::new())),
                    _ => Err(RedisError::UnexpectedResponse),
                }
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// BF.LOADCHUNK - Restore a filter chunk from BF.SCANDUMP
///
/// Used to restore a filter that was dumped with BF.SCANDUMP.
/// Call this for each chunk returned by BF.SCANDUMP.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::bloom::BfLoadChunk;
///
/// let cmd = BfLoadChunk::new("myfilter", 1, b"chunk_data".to_vec());
/// // Load each chunk with its iterator value
/// ```
#[derive(Debug, Clone)]
pub struct BfLoadChunk {
    key: String,
    iterator: i64,
    data: Bytes,
}

impl BfLoadChunk {
    /// Create a new BF.LOADCHUNK command
    pub fn new(key: impl Into<String>, iterator: i64, data: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            iterator,
            data: data.into(),
        }
    }
}

impl Command for BfLoadChunk {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("BF.LOADCHUNK"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.iterator.to_string()))),
            Frame::BulkString(Some(self.data.clone())),
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

/// BF.DEBUG - Get internal debug information
///
/// Returns debugging information about the bloom filter internals.
/// Mainly useful for development and troubleshooting.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::bloom::BfDebug;
///
/// let cmd = BfDebug::new("myfilter");
/// // Response: String with debug information
/// ```
#[derive(Debug, Clone)]
pub struct BfDebug {
    key: String,
}

impl BfDebug {
    /// Create a new BF.DEBUG command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for BfDebug {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("BF.DEBUG"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).to_string()),
            Frame::SimpleString(data) => Ok(String::from_utf8_lossy(&data).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// Read-only trait implementations
use crate::read_preference::ReadOnly;

impl ReadOnly for BfExists {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for BfMexists {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for BfInfo {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for BfCard {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for BfScanDump {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for BfDebug {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands
impl ReadOnly for BfReserve {}
impl ReadOnly for BfAdd {}
impl ReadOnly for BfMadd {}
impl ReadOnly for BfInsert {}
impl ReadOnly for BfLoadChunk {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bf_reserve_frame() {
        let cmd = BfReserve::new("myfilter", 0.01, 1000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("BF.RESERVE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myfilter"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("0.01"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("1000"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bf_reserve_with_expansion() {
        let cmd = BfReserve::new("myfilter", 0.01, 1000).expansion(4);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 6);
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("EXPANSION"))));
                assert_eq!(parts[5], Frame::BulkString(Some(Bytes::from("4"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bf_reserve_nonscaling() {
        let cmd = BfReserve::new("myfilter", 0.01, 1000).nonscaling();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("NONSCALING"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bf_add_frame() {
        let cmd = BfAdd::new("myfilter", b"item1".to_vec());
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("BF.ADD"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myfilter"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("item1"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bf_add_response_added() {
        let frame = Frame::Integer(1);
        let result = BfAdd::parse_response(frame).unwrap();
        assert!(result);
    }

    #[test]
    fn test_bf_add_response_existed() {
        let frame = Frame::Integer(0);
        let result = BfAdd::parse_response(frame).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_bf_madd_frame() {
        let cmd = BfMadd::from_items(
            "myfilter",
            vec![
                Bytes::from("item1"),
                Bytes::from("item2"),
                Bytes::from("item3"),
            ],
        );
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5); // BF.MADD + key + 3 items
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("BF.MADD"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bf_madd_response() {
        let frame = Frame::Array(vec![
            Frame::Integer(1),
            Frame::Integer(0),
            Frame::Integer(1),
        ]);
        let result = BfMadd::parse_response(frame).unwrap();
        assert_eq!(result, vec![true, false, true]);
    }

    #[test]
    fn test_bf_exists_frame() {
        let cmd = BfExists::new("myfilter", b"item1".to_vec());
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("BF.EXISTS"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bf_exists_response() {
        let frame = Frame::Integer(1);
        let result = BfExists::parse_response(frame).unwrap();
        assert!(result);
    }

    #[test]
    fn test_bf_mexists_response() {
        let frame = Frame::Array(vec![Frame::Integer(1), Frame::Integer(0)]);
        let result = BfMexists::parse_response(frame).unwrap();
        assert_eq!(result, vec![true, false]);
    }

    #[test]
    fn test_bf_info_frame() {
        let cmd = BfInfo::new("myfilter");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("BF.INFO"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bf_info_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("Capacity"))),
            Frame::Integer(1000),
            Frame::BulkString(Some(Bytes::from("Size"))),
            Frame::Integer(512),
            Frame::BulkString(Some(Bytes::from("Number of filters"))),
            Frame::Integer(1),
            Frame::BulkString(Some(Bytes::from("Number of items inserted"))),
            Frame::Integer(42),
            Frame::BulkString(Some(Bytes::from("Expansion rate"))),
            Frame::Integer(2),
        ]);

        let result = BfInfo::parse_response(frame).unwrap();
        assert_eq!(result.capacity, 1000);
        assert_eq!(result.size, 512);
        assert_eq!(result.num_filters, 1);
        assert_eq!(result.num_items_inserted, 42);
        assert_eq!(result.expansion_rate, 2);
    }

    #[test]
    fn test_bf_insert_frame() {
        let cmd = BfInsert::new("myfilter", vec![Bytes::from("item1"), Bytes::from("item2")])
            .capacity(10000)
            .error(0.001)
            .nocreate();

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("BF.INSERT"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myfilter"))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("CAPACITY")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("ERROR")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("NOCREATE")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("ITEMS")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bf_insert_response() {
        let frame = Frame::Array(vec![Frame::Integer(1), Frame::Integer(0)]);
        let result = BfInsert::parse_response(frame).unwrap();
        assert_eq!(result, vec![true, false]);
    }

    #[test]
    fn test_bf_card_frame() {
        let cmd = BfCard::new("myfilter");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("BF.CARD"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myfilter"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bf_card_response() {
        let frame = Frame::Integer(42);
        let result = BfCard::parse_response(frame).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_bf_scandump_frame() {
        let cmd = BfScanDump::new("myfilter", 0);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("BF.SCANDUMP")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myfilter"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("0"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bf_scandump_response() {
        let frame = Frame::Array(vec![
            Frame::Integer(5),
            Frame::BulkString(Some(Bytes::from("chunk_data"))),
        ]);
        let result = BfScanDump::parse_response(frame).unwrap();
        assert_eq!(result.0, 5);
        assert_eq!(result.1, Bytes::from("chunk_data"));
    }

    #[test]
    fn test_bf_scandump_response_complete() {
        let frame = Frame::Array(vec![Frame::Integer(0), Frame::BulkString(None)]);
        let result = BfScanDump::parse_response(frame).unwrap();
        assert_eq!(result.0, 0);
        assert_eq!(result.1, Bytes::new());
    }

    #[test]
    fn test_bf_loadchunk_frame() {
        let cmd = BfLoadChunk::new("myfilter", 1, b"chunk_data".to_vec());
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("BF.LOADCHUNK")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myfilter"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("1"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("chunk_data"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bf_loadchunk_response() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let result = BfLoadChunk::parse_response(frame);
        assert!(result.is_ok());
    }

    #[test]
    fn test_bf_debug_frame() {
        let cmd = BfDebug::new("myfilter");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("BF.DEBUG"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myfilter"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bf_debug_response() {
        let frame = Frame::BulkString(Some(Bytes::from("debug info here")));
        let result = BfDebug::parse_response(frame).unwrap();
        assert_eq!(result, "debug info here");
    }

    #[test]
    fn test_bf_debug_response_simple_string() {
        let frame = Frame::SimpleString(Bytes::from("debug info"));
        let result = BfDebug::parse_response(frame).unwrap();
        assert_eq!(result, "debug info");
    }
}
