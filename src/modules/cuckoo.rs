//! Redis Cuckoo Filter commands
//!
//! Cuckoo filters are probabilistic data structures similar to Bloom filters,
//! but with better support for deletion. They use cuckoo hashing to achieve
//! high space efficiency while allowing items to be removed.
//!
//! # Key Advantages over Bloom Filters
//! - **Deletion Support**: Unlike Bloom filters, items can be deleted
//! - **Better Lookup Performance**: Typically faster membership queries
//! - **Bounded False Positive Rate**: More predictable error rates
//!
//! # Use Cases
//! - Cache admission policies (with eviction)
//! - Temporary blacklists that need updates
//! - Deduplication with item removal
//!
//! # Examples
//! ```no_run
//! use redis_tower::modules::cuckoo::{CfAdd, CfExists, CfDel, CfReserve};
//! use redis_tower::RedisClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("localhost:6379").await?;
//!
//! // Create cuckoo filter with 1000 capacity
//! client.call(CfReserve::new("myfilter", 1000)).await?;
//!
//! // Add item
//! let added: bool = client.call(CfAdd::new("myfilter", "item1")).await?;
//!
//! // Check existence
//! let exists: bool = client.call(CfExists::new("myfilter", "item1")).await?;
//!
//! // Delete item (unlike Bloom filters!)
//! let deleted: bool = client.call(CfDel::new("myfilter", "item1")).await?;
//! # Ok(())
//! # }
//! ```

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// CF.RESERVE - Create a cuckoo filter
///
/// Creates a new cuckoo filter with specified capacity. Optional parameters
///allow fine-tuning of bucket size, max iterations, and expansion factor.
///
/// # Arguments
/// * `key` - Filter key name
/// * `capacity` - Initial capacity (number of expected items)
///
/// # Optional Parameters
/// * `bucketsize` - Items per bucket (default: 2)
/// * `maxiterations` - Max iterations during insertion (default: 20)
/// * `expansion` - Expansion factor when full (default: 1)
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cuckoo::CfReserve;
///
/// // Basic: 10000 items
/// let cmd = CfReserve::new("myfilter", 10000);
///
/// // With custom parameters
/// let cmd = CfReserve::new("myfilter", 10000)
///     .bucketsize(4)
///     .maxiterations(50)
///     .expansion(2);
/// ```
#[derive(Debug, Clone)]
pub struct CfReserve {
    key: String,
    capacity: i64,
    bucketsize: Option<i64>,
    maxiterations: Option<i64>,
    expansion: Option<i64>,
}

impl CfReserve {
    /// Create a new CF.RESERVE command
    pub fn new(key: impl Into<String>, capacity: i64) -> Self {
        Self {
            key: key.into(),
            capacity,
            bucketsize: None,
            maxiterations: None,
            expansion: None,
        }
    }

    /// Set bucket size (items per bucket)
    pub fn bucketsize(mut self, size: i64) -> Self {
        self.bucketsize = Some(size);
        self
    }

    /// Set max iterations for insertion attempts
    pub fn maxiterations(mut self, iterations: i64) -> Self {
        self.maxiterations = Some(iterations);
        self
    }

    /// Set expansion factor when capacity is reached
    pub fn expansion(mut self, factor: i64) -> Self {
        self.expansion = Some(factor);
        self
    }
}

impl Command for CfReserve {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CF.RESERVE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.capacity.to_string()))),
        ];

        if let Some(size) = self.bucketsize {
            frames.push(Frame::BulkString(Some(Bytes::from("BUCKETSIZE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(size.to_string()))));
        }

        if let Some(iters) = self.maxiterations {
            frames.push(Frame::BulkString(Some(Bytes::from("MAXITERATIONS"))));
            frames.push(Frame::BulkString(Some(Bytes::from(iters.to_string()))));
        }

        if let Some(exp) = self.expansion {
            frames.push(Frame::BulkString(Some(Bytes::from("EXPANSION"))));
            frames.push(Frame::BulkString(Some(Bytes::from(exp.to_string()))));
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

/// CF.ADD - Add an item to a cuckoo filter
///
/// Adds an item to the cuckoo filter. If the filter doesn't exist, it's created
/// with default parameters (capacity=1024).
///
/// Returns true if the item was added, false if it already existed.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cuckoo::CfAdd;
///
/// let cmd = CfAdd::new("myfilter", "user123");
/// // Response: true if newly added, false if already existed
/// ```
#[derive(Debug, Clone)]
pub struct CfAdd {
    key: String,
    item: Bytes,
}

impl CfAdd {
    /// Create a new CF.ADD command
    pub fn new(key: impl Into<String>, item: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for CfAdd {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CF.ADD"))),
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

/// CF.ADDNX - Add an item only if it doesn't exist
///
/// Adds an item to the filter only if it doesn't already exist.
///
/// Returns true if the item was added, false if it already existed.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cuckoo::CfAddNx;
///
/// let cmd = CfAddNx::new("myfilter", "user123");
/// // Response: true if added, false if already existed
/// ```
#[derive(Debug, Clone)]
pub struct CfAddNx {
    key: String,
    item: Bytes,
}

impl CfAddNx {
    /// Create a new CF.ADDNX command
    pub fn new(key: impl Into<String>, item: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for CfAddNx {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CF.ADDNX"))),
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

/// CF.INSERT - Add multiple items with creation options
///
/// More flexible version of CF.ADD that allows creating filters on-the-fly
/// with custom parameters and adding multiple items.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cuckoo::CfInsert;
///
/// // Simple insert (creates filter with defaults if needed)
/// let cmd = CfInsert::new("myfilter", vec![b"item1".to_vec()]);
///
/// // With custom parameters if filter doesn't exist
/// let cmd = CfInsert::new("myfilter", vec![b"item1".to_vec()])
///     .capacity(10000)
///     .nocreate(); // Fail if filter doesn't exist
/// ```
#[derive(Debug, Clone)]
pub struct CfInsert {
    key: String,
    items: Vec<Bytes>,
    capacity: Option<i64>,
    nocreate: bool,
}

impl CfInsert {
    /// Create a new CF.INSERT command
    pub fn new(key: impl Into<String>, items: Vec<Bytes>) -> Self {
        Self {
            key: key.into(),
            items,
            capacity: None,
            nocreate: false,
        }
    }

    /// Set capacity if filter needs to be created
    pub fn capacity(mut self, capacity: i64) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// Don't create filter if it doesn't exist (fail instead)
    pub fn nocreate(mut self) -> Self {
        self.nocreate = true;
        self
    }
}

impl Command for CfInsert {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CF.INSERT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(cap) = self.capacity {
            frames.push(Frame::BulkString(Some(Bytes::from("CAPACITY"))));
            frames.push(Frame::BulkString(Some(Bytes::from(cap.to_string()))));
        }

        if self.nocreate {
            frames.push(Frame::BulkString(Some(Bytes::from("NOCREATE"))));
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

/// CF.INSERTNX - Add multiple items only if they don't exist
///
/// Similar to CF.INSERT but only adds items that don't already exist.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cuckoo::CfInsertNx;
///
/// let cmd = CfInsertNx::new("myfilter", vec![b"item1".to_vec(), b"item2".to_vec()])
///     .capacity(10000);
/// // Response: vec![true, false] - first added, second existed
/// ```
#[derive(Debug, Clone)]
pub struct CfInsertNx {
    key: String,
    items: Vec<Bytes>,
    capacity: Option<i64>,
    nocreate: bool,
}

impl CfInsertNx {
    /// Create a new CF.INSERTNX command
    pub fn new(key: impl Into<String>, items: Vec<Bytes>) -> Self {
        Self {
            key: key.into(),
            items,
            capacity: None,
            nocreate: false,
        }
    }

    /// Set capacity if filter needs to be created
    pub fn capacity(mut self, capacity: i64) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// Don't create filter if it doesn't exist (fail instead)
    pub fn nocreate(mut self) -> Self {
        self.nocreate = true;
        self
    }
}

impl Command for CfInsertNx {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CF.INSERTNX"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(cap) = self.capacity {
            frames.push(Frame::BulkString(Some(Bytes::from("CAPACITY"))));
            frames.push(Frame::BulkString(Some(Bytes::from(cap.to_string()))));
        }

        if self.nocreate {
            frames.push(Frame::BulkString(Some(Bytes::from("NOCREATE"))));
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

/// CF.EXISTS - Check if an item exists in a cuckoo filter
///
/// Checks if an item probably exists in the filter.
/// Like Bloom filters, may return false positives but never false negatives.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cuckoo::CfExists;
///
/// let cmd = CfExists::new("myfilter", "user123");
/// // Response: true if probably exists, false if definitely doesn't
/// ```
#[derive(Debug, Clone)]
pub struct CfExists {
    key: String,
    item: Bytes,
}

impl CfExists {
    /// Create a new CF.EXISTS command
    pub fn new(key: impl Into<String>, item: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for CfExists {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CF.EXISTS"))),
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

/// CF.DEL - Delete an item from a cuckoo filter
///
/// Deletes an item from the filter. This is the key advantage of cuckoo filters
/// over Bloom filters - items can be removed.
///
/// Returns true if the item was deleted, false if it didn't exist.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cuckoo::CfDel;
///
/// let cmd = CfDel::new("myfilter", "user123");
/// // Response: true if deleted, false if didn't exist
/// ```
#[derive(Debug, Clone)]
pub struct CfDel {
    key: String,
    item: Bytes,
}

impl CfDel {
    /// Create a new CF.DEL command
    pub fn new(key: impl Into<String>, item: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for CfDel {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CF.DEL"))),
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

/// CF.COUNT - Get the count of an item in the filter
///
/// Returns the number of times an item may be in the filter.
/// Due to hash collisions, this may be an overestimate.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cuckoo::CfCount;
///
/// let cmd = CfCount::new("myfilter", "user123");
/// // Response: i64 - count of item (may be overestimate)
/// ```
#[derive(Debug, Clone)]
pub struct CfCount {
    key: String,
    item: Bytes,
}

impl CfCount {
    /// Create a new CF.COUNT command
    pub fn new(key: impl Into<String>, item: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for CfCount {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CF.COUNT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(self.item.clone())),
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

/// CF.INFO - Get information about a cuckoo filter
///
/// Returns metadata about the filter including size, bucket count,
/// filter count, expansion rate, and max iterations.
///
/// # Example
/// ```no_run
/// use redis_tower::modules::cuckoo::CfInfo;
///
/// let cmd = CfInfo::new("myfilter");
/// // Response: CfInfoResult with filter statistics
/// ```
#[derive(Debug, Clone)]
pub struct CfInfo {
    key: String,
}

impl CfInfo {
    /// Create a new CF.INFO command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

/// Result from CF.INFO command
#[derive(Debug, Clone, PartialEq)]
pub struct CfInfoResult {
    /// Total size in bytes
    pub size: i64,
    /// Number of buckets
    pub num_buckets: i64,
    /// Number of sub-filters
    pub num_filters: i64,
    /// Number of items inserted
    pub num_items_inserted: i64,
    /// Number of items deleted
    pub num_items_deleted: i64,
    /// Bucket size (items per bucket)
    pub bucket_size: i64,
    /// Expansion rate
    pub expansion_rate: i64,
    /// Max iterations for insertion
    pub max_iterations: i64,
}

impl Command for CfInfo {
    type Response = CfInfoResult;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CF.INFO"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                // CF.INFO returns array of alternating field names and values
                let mut size = 0;
                let mut num_buckets = 0;
                let mut num_filters = 0;
                let mut num_items_inserted = 0;
                let mut num_items_deleted = 0;
                let mut bucket_size = 0;
                let mut expansion_rate = 0;
                let mut max_iterations = 0;

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
                        "Size" => size = value,
                        "Number of buckets" => num_buckets = value,
                        "Number of filters" => num_filters = value,
                        "Number of items inserted" => num_items_inserted = value,
                        "Number of items deleted" => num_items_deleted = value,
                        "Bucket size" => bucket_size = value,
                        "Expansion rate" => expansion_rate = value,
                        "Max iterations" => max_iterations = value,
                        _ => {}
                    }

                    i += 2;
                }

                Ok(CfInfoResult {
                    size,
                    num_buckets,
                    num_filters,
                    num_items_inserted,
                    num_items_deleted,
                    bucket_size,
                    expansion_rate,
                    max_iterations,
                })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// Read-only trait implementations
use crate::read_preference::ReadOnly;

impl ReadOnly for CfExists {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for CfCount {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for CfInfo {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands
impl ReadOnly for CfReserve {}
impl ReadOnly for CfAdd {}
impl ReadOnly for CfAddNx {}
impl ReadOnly for CfInsert {}
impl ReadOnly for CfInsertNx {}
impl ReadOnly for CfDel {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cf_reserve_basic() {
        let cmd = CfReserve::new("myfilter", 1000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CF.RESERVE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myfilter"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("1000"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cf_reserve_with_options() {
        let cmd = CfReserve::new("myfilter", 1000)
            .bucketsize(4)
            .maxiterations(50)
            .expansion(2);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 9);
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("BUCKETSIZE")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("4")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("MAXITERATIONS")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("50")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("EXPANSION")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("2")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cf_add() {
        let cmd = CfAdd::new("myfilter", b"item1".to_vec());
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CF.ADD"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cf_addnx() {
        let cmd = CfAddNx::new("myfilter", b"item1".to_vec());
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CF.ADDNX"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cf_insert() {
        let cmd = CfInsert::new("myfilter", vec![Bytes::from("item1"), Bytes::from("item2")])
            .capacity(10000)
            .nocreate();

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CF.INSERT"))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("CAPACITY")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("NOCREATE")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("ITEMS")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cf_insertnx() {
        let cmd = CfInsertNx::new("myfilter", vec![Bytes::from("item1")]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("CF.INSERTNX")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cf_exists() {
        let cmd = CfExists::new("myfilter", b"item1".to_vec());
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CF.EXISTS"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cf_del() {
        let cmd = CfDel::new("myfilter", b"item1".to_vec());
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CF.DEL"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cf_count() {
        let cmd = CfCount::new("myfilter", b"item1".to_vec());
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CF.COUNT"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cf_info() {
        let cmd = CfInfo::new("myfilter");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CF.INFO"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cf_add_response() {
        let frame = Frame::Integer(1);
        let result = CfAdd::parse_response(frame).unwrap();
        assert!(result);
    }

    #[test]
    fn test_cf_insert_response() {
        let frame = Frame::Array(vec![Frame::Integer(1), Frame::Integer(0)]);
        let result = CfInsert::parse_response(frame).unwrap();
        assert_eq!(result, vec![true, false]);
    }

    #[test]
    fn test_cf_count_response() {
        let frame = Frame::Integer(5);
        let result = CfCount::parse_response(frame).unwrap();
        assert_eq!(result, 5);
    }
}
