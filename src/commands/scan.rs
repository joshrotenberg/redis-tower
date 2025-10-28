//! Scan commands for iterating over large datasets
//!
//! The SCAN family of commands (SCAN, SSCAN, HSCAN, ZSCAN) provide cursor-based iteration
//! over Redis data structures without blocking the server. Unlike KEYS, SMEMBERS, HGETALL,
//! and ZRANGE, these commands work incrementally and are safe to use in production.
//!
//! # Key Characteristics
//!
//! - **Non-blocking**: Each call does a small amount of work
//! - **Cursor-based**: Maintains iteration state via cursor (0 = start/complete)
//! - **No guarantees**: Elements may appear multiple times or be missed if modified during iteration
//! - **Pattern matching**: Optional MATCH filter for selecting specific elements
//! - **Count hint**: Optional COUNT to suggest number of elements per call (not a hard limit)
//!
//! # Common Pattern
//!
//! ```no_run
//! use redis_tower::commands::scan::{Scan, ScanResult};
//! use redis_tower::RedisClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let client = RedisClient::connect("127.0.0.1:6379").await?;
//! let mut cursor = 0;
//! let mut all_keys = Vec::new();
//!
//! loop {
//!     let result: ScanResult = client.call(Scan::new(cursor)).await?;
//!     all_keys.extend(result.keys);
//!
//!     cursor = result.cursor;
//!     if cursor == 0 {
//!         break; // Iteration complete
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # When to Use SCAN vs Alternatives
//!
//! **Use SCAN family when**:
//! - You need to iterate over large datasets (>1000 elements)
//! - You can't afford to block the server
//! - You can tolerate duplicate/missing elements during iteration
//!
//! **Use blocking alternatives when**:
//! - Dataset is small (<1000 elements)
//! - You need exact snapshot semantics
//! - Performance isn't critical

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// SCAN command - Incrementally iterate over all keys in the database
///
/// Iterates over the keyspace of the currently selected database using a cursor. Unlike
/// KEYS, SCAN is safe to use in production as it doesn't block the server. The iteration
/// is stateless on the server - the cursor encodes all the state.
///
/// **Important**: SCAN may return duplicate keys or miss keys that are modified during
/// iteration. Elements present from start to finish are guaranteed to be returned.
///
/// # Request
/// - `cursor`: Cursor position (0 to start new iteration)
/// - `pattern` (optional): MATCH pattern to filter keys (glob-style: *, ?, [])
/// - `count` (optional): COUNT hint for elements per call (default ~10, not a hard limit)
///
/// # Response
/// Returns `ScanResult`:
/// - `cursor`: Next cursor position (0 indicates iteration complete)
/// - `keys`: Keys found in this iteration (may be empty even when cursor != 0)
///
/// # Redis Version
/// Available since Redis 2.8.0
///
/// # Examples
///
/// Basic iteration over all keys:
/// ```no_run
/// use redis_tower::commands::scan::{Scan, ScanResult};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
/// let mut all_keys = Vec::new();
///
/// loop {
///     let result: ScanResult = client.call(Scan::new(cursor)).await?;
///     all_keys.extend(result.keys);
///     println!("Scanned {} keys, cursor now at {}", all_keys.len(), result.cursor);
///
///     cursor = result.cursor;
///     if cursor == 0 {
///         break; // Iteration complete
///     }
/// }
///
/// println!("Total keys found: {}", all_keys.len());
/// # Ok(())
/// # }
/// ```
///
/// Pattern matching with MATCH:
/// ```no_run
/// use redis_tower::commands::scan::Scan;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
/// let mut user_keys = Vec::new();
///
/// loop {
///     // Only return keys matching pattern "user:*"
///     let result = client.call(
///         Scan::new(cursor).pattern("user:*")
///     ).await?;
///
///     user_keys.extend(result.keys);
///     cursor = result.cursor;
///     if cursor == 0 { break; }
/// }
///
/// println!("Found {} user keys", user_keys.len());
/// # Ok(())
/// # }
/// ```
///
/// Using COUNT hint for performance tuning:
/// ```no_run
/// use redis_tower::commands::scan::Scan;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
///
/// loop {
///     // Request ~100 keys per call (hint, not guarantee)
///     let result = client.call(
///         Scan::new(cursor).count(100)
///     ).await?;
///
///     println!("Got {} keys in this batch", result.keys.len());
///
///     cursor = result.cursor;
///     if cursor == 0 { break; }
/// }
/// # Ok(())
/// # }
/// ```
///
/// Combined pattern and count:
/// ```no_run
/// use redis_tower::commands::scan::Scan;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
/// let mut session_keys = Vec::new();
///
/// loop {
///     let result = client.call(
///         Scan::new(cursor)
///             .pattern("session:*")
///             .count(50)  // Request 50 keys per iteration
///     ).await?;
///
///     session_keys.extend(result.keys);
///     cursor = result.cursor;
///     if cursor == 0 { break; }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Scan {
    pub(crate) cursor: u64,
    pub(crate) pattern: Option<String>,
    pub(crate) count: Option<usize>,
}

impl Scan {
    /// Create a new SCAN command starting from the given cursor
    pub fn new(cursor: u64) -> Self {
        Self {
            cursor,
            pattern: None,
            count: None,
        }
    }

    /// Filter keys by pattern (MATCH option)
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Hint for number of elements to return per call (COUNT option)
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }
}

/// Response from SCAN command
///
/// Contains the next cursor and the list of keys found.
/// When cursor is 0, the iteration is complete.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanResult {
    /// Next cursor (0 means iteration complete)
    pub cursor: u64,
    /// Keys found in this iteration
    pub keys: Vec<Bytes>,
}

impl Command for Scan {
    type Response = ScanResult;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("SCAN"))),
            Frame::BulkString(Some(Bytes::from(self.cursor.to_string()))),
        ];

        // Add MATCH pattern if specified
        if let Some(ref pattern) = self.pattern {
            frames.push(Frame::BulkString(Some(Bytes::from("MATCH"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                pattern.as_bytes(),
            ))));
        }

        // Add COUNT if specified
        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        // SCAN returns an array with two elements:
        // [0] = cursor (as bulk string)
        // [1] = array of keys
        match frame {
            Frame::Array(mut elements) if elements.len() == 2 => {
                let keys_frame = elements.pop().unwrap();
                let cursor_frame = elements.pop().unwrap();

                // Parse cursor
                let cursor = match cursor_frame {
                    Frame::BulkString(Some(cursor_bytes)) => {
                        let cursor_str = String::from_utf8_lossy(&cursor_bytes);
                        cursor_str
                            .parse::<u64>()
                            .map_err(|_| RedisError::Protocol("Invalid cursor".to_string()))?
                    }
                    _ => {
                        return Err(RedisError::Protocol(
                            "SCAN cursor must be bulk string".to_string(),
                        ));
                    }
                };

                // Parse keys array
                let keys = match keys_frame {
                    Frame::Array(key_frames) => {
                        let mut keys = Vec::with_capacity(key_frames.len());
                        for key_frame in key_frames {
                            match key_frame {
                                Frame::BulkString(Some(key)) => keys.push(key),
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "SCAN key must be bulk string".to_string(),
                                    ));
                                }
                            }
                        }
                        keys
                    }
                    _ => return Err(RedisError::Protocol("SCAN keys must be array".to_string())),
                };

                Ok(ScanResult { cursor, keys })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SSCAN command - Incrementally iterate over set members
///
/// Iterates over the members of a set using a cursor. Unlike SMEMBERS, SSCAN doesn't
/// block the server and is safe to use with large sets in production. The cursor is
/// specific to the set being scanned.
///
/// **Important**: Like all SCAN commands, SSCAN may return duplicate members or miss
/// members that are added/removed during iteration.
///
/// # Request
/// - `key`: The set key to scan
/// - `cursor`: Cursor position (0 to start new iteration)
/// - `pattern` (optional): MATCH pattern to filter members (glob-style: *, ?, [])
/// - `count` (optional): COUNT hint for elements per call (default ~10, not a hard limit)
///
/// # Response
/// Returns `SScanResult`:
/// - `cursor`: Next cursor position (0 indicates iteration complete)
/// - `members`: Set members found in this iteration (may be empty even when cursor != 0)
///
/// # Redis Version
/// Available since Redis 2.8.0
///
/// # Examples
///
/// Basic set member iteration:
/// ```no_run
/// use redis_tower::commands::scan::{SScan, SScanResult};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
/// let mut all_members = Vec::new();
///
/// loop {
///     let result: SScanResult = client.call(
///         SScan::new("active_users", cursor)
///     ).await?;
///
///     all_members.extend(result.members);
///     cursor = result.cursor;
///     if cursor == 0 { break; }
/// }
///
/// println!("Found {} active users", all_members.len());
/// # Ok(())
/// # }
/// ```
///
/// Pattern matching members:
/// ```no_run
/// use redis_tower::commands::scan::SScan;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
/// let mut admin_users = Vec::new();
///
/// loop {
///     // Only return members matching pattern "admin:*"
///     let result = client.call(
///         SScan::new("users", cursor).pattern("admin:*")
///     ).await?;
///
///     admin_users.extend(result.members);
///     cursor = result.cursor;
///     if cursor == 0 { break; }
/// }
/// # Ok(())
/// # }
/// ```
///
/// Using COUNT for large sets:
/// ```no_run
/// use redis_tower::commands::scan::SScan;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
///
/// loop {
///     let result = client.call(
///         SScan::new("large_set", cursor)
///             .count(1000)  // Process in larger batches
///     ).await?;
///
///     // Process batch
///     for member in result.members {
///         println!("Processing: {}", String::from_utf8_lossy(&member));
///     }
///
///     cursor = result.cursor;
///     if cursor == 0 { break; }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct SScan {
    pub(crate) key: String,
    pub(crate) cursor: u64,
    pub(crate) pattern: Option<String>,
    pub(crate) count: Option<usize>,
}

impl SScan {
    /// Create a new SSCAN command.
    ///
    /// # Arguments
    ///
    /// * `key` - The set key to scan
    /// * `cursor` - The cursor position (0 to start)
    pub fn new(key: impl Into<String>, cursor: u64) -> Self {
        Self {
            key: key.into(),
            cursor,
            pattern: None,
            count: None,
        }
    }

    /// Set the MATCH pattern for filtering members.
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Set the COUNT hint for number of members to return.
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }
}

/// Result from SSCAN command containing cursor and members.
#[derive(Debug, Clone, PartialEq)]
pub struct SScanResult {
    /// Next cursor position (0 indicates iteration complete)
    pub cursor: u64,
    /// Set members matching the scan
    pub members: Vec<Bytes>,
}

impl Command for SScan {
    type Response = SScanResult;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(b"SSCAN".to_vec().into())),
            Frame::BulkString(Some(self.key.as_bytes().to_vec().into())),
            Frame::BulkString(Some(self.cursor.to_string().as_bytes().to_vec().into())),
        ];

        if let Some(pattern) = &self.pattern {
            args.push(Frame::BulkString(Some(b"MATCH".to_vec().into())));
            args.push(Frame::BulkString(Some(pattern.as_bytes().to_vec().into())));
        }

        if let Some(count) = self.count {
            args.push(Frame::BulkString(Some(b"COUNT".to_vec().into())));
            args.push(Frame::BulkString(Some(
                count.to_string().as_bytes().to_vec().into(),
            )));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(mut parts) if parts.len() == 2 => {
                let cursor = match parts.remove(0) {
                    Frame::BulkString(Some(data)) => String::from_utf8_lossy(&data)
                        .parse::<u64>()
                        .map_err(|_| RedisError::Protocol("Invalid cursor".to_string()))?,
                    _ => {
                        return Err(RedisError::Protocol(
                            "SSCAN cursor must be bulk string".to_string(),
                        ));
                    }
                };

                let members = match parts.remove(0) {
                    Frame::Array(members) => {
                        let mut result = Vec::with_capacity(members.len());
                        for member in members {
                            match member {
                                Frame::BulkString(Some(value)) => {
                                    result.push(value);
                                }
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "SSCAN members must be bulk strings".to_string(),
                                    ));
                                }
                            }
                        }
                        result
                    }
                    _ => {
                        return Err(RedisError::Protocol(
                            "SSCAN members must be array".to_string(),
                        ));
                    }
                };

                Ok(SScanResult { cursor, members })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZSCAN command - Incrementally iterate over sorted set members with scores
///
/// Iterates over the members and scores of a sorted set using a cursor. Unlike ZRANGE,
/// ZSCAN doesn't block the server and is safe to use with large sorted sets in production.
/// Returns members with their scores as tuples.
///
/// **Important**: Like all SCAN commands, ZSCAN may return duplicate members or miss
/// members that are added/removed during iteration.
///
/// # Request
/// - `key`: The sorted set key to scan
/// - `cursor`: Cursor position (0 to start new iteration)
/// - `pattern` (optional): MATCH pattern to filter members (glob-style: *, ?, [])
/// - `count` (optional): COUNT hint for elements per call (default ~10, not a hard limit)
///
/// # Response
/// Returns `ZScanResult`:
/// - `cursor`: Next cursor position (0 indicates iteration complete)
/// - `members`: Vec of (member, score) tuples found in this iteration
///
/// # Redis Version
/// Available since Redis 2.8.0
///
/// # Examples
///
/// Basic sorted set iteration:
/// ```no_run
/// use redis_tower::commands::scan::{ZScan, ZScanResult};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
/// let mut all_entries = Vec::new();
///
/// loop {
///     let result: ZScanResult = client.call(
///         ZScan::new("leaderboard", cursor)
///     ).await?;
///
///     for (member, score) in &result.members {
///         println!("{}: {}", String::from_utf8_lossy(member), score);
///     }
///
///     all_entries.extend(result.members);
///     cursor = result.cursor;
///     if cursor == 0 { break; }
/// }
/// # Ok(())
/// # }
/// ```
///
/// Pattern matching with scores:
/// ```no_run
/// use redis_tower::commands::scan::ZScan;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
/// let mut premium_users = Vec::new();
///
/// loop {
///     // Only return members matching pattern "premium:*"
///     let result = client.call(
///         ZScan::new("user_scores", cursor).pattern("premium:*")
///     ).await?;
///
///     premium_users.extend(result.members);
///     cursor = result.cursor;
///     if cursor == 0 { break; }
/// }
///
/// // Sort by score (ZSCAN doesn't guarantee order)
/// premium_users.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
/// # Ok(())
/// # }
/// ```
///
/// Processing large leaderboard in batches:
/// ```no_run
/// use redis_tower::commands::scan::ZScan;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
/// let mut total_score = 0.0;
/// let mut count = 0;
///
/// loop {
///     let result = client.call(
///         ZScan::new("game_scores", cursor)
///             .count(500)  // Process 500 entries per batch
///     ).await?;
///
///     for (_member, score) in result.members {
///         total_score += score;
///         count += 1;
///     }
///
///     cursor = result.cursor;
///     if cursor == 0 { break; }
/// }
///
/// let average = total_score / count as f64;
/// println!("Average score: {:.2}", average);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ZScan {
    pub(crate) key: String,
    pub(crate) cursor: u64,
    pub(crate) pattern: Option<String>,
    pub(crate) count: Option<usize>,
}

impl ZScan {
    /// Create a new ZSCAN command.
    ///
    /// # Arguments
    ///
    /// * `key` - The sorted set key to scan
    /// * `cursor` - The cursor position (0 to start)
    pub fn new(key: impl Into<String>, cursor: u64) -> Self {
        Self {
            key: key.into(),
            cursor,
            pattern: None,
            count: None,
        }
    }

    /// Set the MATCH pattern for filtering members.
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Set the COUNT hint for number of members to return.
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }
}

/// Result from ZSCAN command containing cursor and member-score pairs.
#[derive(Debug, Clone, PartialEq)]
pub struct ZScanResult {
    /// Next cursor position (0 indicates iteration complete)
    pub cursor: u64,
    /// Sorted set members with scores
    pub members: Vec<(Bytes, f64)>,
}

impl Command for ZScan {
    type Response = ZScanResult;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(b"ZSCAN".to_vec().into())),
            Frame::BulkString(Some(self.key.as_bytes().to_vec().into())),
            Frame::BulkString(Some(self.cursor.to_string().as_bytes().to_vec().into())),
        ];

        if let Some(pattern) = &self.pattern {
            args.push(Frame::BulkString(Some(b"MATCH".to_vec().into())));
            args.push(Frame::BulkString(Some(pattern.as_bytes().to_vec().into())));
        }

        if let Some(count) = self.count {
            args.push(Frame::BulkString(Some(b"COUNT".to_vec().into())));
            args.push(Frame::BulkString(Some(
                count.to_string().as_bytes().to_vec().into(),
            )));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(mut parts) if parts.len() == 2 => {
                let cursor = match parts.remove(0) {
                    Frame::BulkString(Some(data)) => String::from_utf8_lossy(&data)
                        .parse::<u64>()
                        .map_err(|_| RedisError::Protocol("Invalid cursor".to_string()))?,
                    _ => {
                        return Err(RedisError::Protocol(
                            "ZSCAN cursor must be bulk string".to_string(),
                        ));
                    }
                };

                let members = match parts.remove(0) {
                    Frame::Array(elements) if elements.len() % 2 == 0 => {
                        let mut result = Vec::with_capacity(elements.len() / 2);
                        let mut iter = elements.into_iter();
                        while let (Some(member), Some(score)) = (iter.next(), iter.next()) {
                            match (member, score) {
                                (
                                    Frame::BulkString(Some(member)),
                                    Frame::BulkString(Some(score)),
                                ) => {
                                    let score_val =
                                        String::from_utf8_lossy(&score).parse::<f64>().map_err(
                                            |_| RedisError::Protocol("Invalid score".to_string()),
                                        )?;
                                    result.push((member, score_val));
                                }
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "ZSCAN elements must be bulk strings".to_string(),
                                    ));
                                }
                            }
                        }
                        result
                    }
                    _ => {
                        return Err(RedisError::Protocol(
                            "ZSCAN members must be array with even length".to_string(),
                        ));
                    }
                };

                Ok(ZScanResult { cursor, members })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// HSCAN command - Incrementally iterate over hash fields and values
///
/// Iterates over the field-value pairs of a hash using a cursor. Unlike HGETALL, HSCAN
/// doesn't block the server and is safe to use with large hashes in production. Returns
/// field-value pairs as tuples.
///
/// **Important**: Like all SCAN commands, HSCAN may return duplicate fields or miss
/// fields that are added/removed during iteration.
///
/// # Request
/// - `key`: The hash key to scan
/// - `cursor`: Cursor position (0 to start new iteration)
/// - `pattern` (optional): MATCH pattern to filter field names (glob-style: *, ?, [])
/// - `count` (optional): COUNT hint for elements per call (default ~10, not a hard limit)
///
/// # Response
/// Returns `HScanResult`:
/// - `cursor`: Next cursor position (0 indicates iteration complete)
/// - `fields`: Vec of (field, value) tuples found in this iteration
///
/// # Redis Version
/// Available since Redis 2.8.0
///
/// # Examples
///
/// Basic hash field iteration:
/// ```no_run
/// use redis_tower::commands::scan::{HScan, HScanResult};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
/// let mut all_fields = Vec::new();
///
/// loop {
///     let result: HScanResult = client.call(
///         HScan::new("user:1000", cursor)
///     ).await?;
///
///     for (field, value) in &result.fields {
///         println!("{}: {}",
///             String::from_utf8_lossy(field),
///             String::from_utf8_lossy(value)
///         );
///     }
///
///     all_fields.extend(result.fields);
///     cursor = result.cursor;
///     if cursor == 0 { break; }
/// }
/// # Ok(())
/// # }
/// ```
///
/// Pattern matching hash fields:
/// ```no_run
/// use redis_tower::commands::scan::HScan;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
/// let mut metadata_fields = Vec::new();
///
/// loop {
///     // Only return fields matching pattern "meta:*"
///     let result = client.call(
///         HScan::new("object:1", cursor).pattern("meta:*")
///     ).await?;
///
///     metadata_fields.extend(result.fields);
///     cursor = result.cursor;
///     if cursor == 0 { break; }
/// }
/// # Ok(())
/// # }
/// ```
///
/// Processing large hash in batches:
/// ```no_run
/// use redis_tower::commands::scan::HScan;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
///
/// loop {
///     let result = client.call(
///         HScan::new("large_config", cursor)
///             .count(100)  // Process 100 fields per batch
///     ).await?;
///
///     // Validate or process each field
///     for (field, value) in result.fields {
///         let field_name = String::from_utf8_lossy(&field);
///         let field_value = String::from_utf8_lossy(&value);
///
///         if field_value.is_empty() {
///             println!("Warning: Empty value for field {}", field_name);
///         }
///     }
///
///     cursor = result.cursor;
///     if cursor == 0 { break; }
/// }
/// # Ok(())
/// # }
/// ```
///
/// Collecting specific fields:
/// ```no_run
/// use redis_tower::commands::scan::HScan;
/// use redis_tower::RedisClient;
/// use std::collections::HashMap;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let mut cursor = 0;
/// let mut config = HashMap::new();
///
/// loop {
///     let result = client.call(
///         HScan::new("app:config", cursor).pattern("feature:*")
///     ).await?;
///
///     for (field, value) in result.fields {
///         config.insert(
///             String::from_utf8_lossy(&field).to_string(),
///             String::from_utf8_lossy(&value).to_string()
///         );
///     }
///
///     cursor = result.cursor;
///     if cursor == 0 { break; }
/// }
///
/// println!("Found {} feature flags", config.len());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct HScan {
    pub(crate) key: String,
    pub(crate) cursor: u64,
    pub(crate) pattern: Option<String>,
    pub(crate) count: Option<usize>,
}

impl HScan {
    /// Create a new HSCAN command for the given key
    pub fn new(key: impl Into<String>, cursor: u64) -> Self {
        Self {
            key: key.into(),
            cursor,
            pattern: None,
            count: None,
        }
    }

    /// Filter fields by pattern (MATCH option)
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Hint for number of elements to return per call (COUNT option)
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }
}

/// Response from HSCAN command
#[derive(Debug, Clone, PartialEq)]
pub struct HScanResult {
    /// Next cursor (0 means iteration complete)
    pub cursor: u64,
    /// Field-value pairs found in this iteration
    pub fields: Vec<(Bytes, Bytes)>,
}

impl Command for HScan {
    type Response = HScanResult;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HSCAN"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.cursor.to_string()))),
        ];

        if let Some(ref pattern) = self.pattern {
            frames.push(Frame::BulkString(Some(Bytes::from("MATCH"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                pattern.as_bytes(),
            ))));
        }

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(mut elements) if elements.len() == 2 => {
                let fields_frame = elements.pop().unwrap();
                let cursor_frame = elements.pop().unwrap();

                // Parse cursor
                let cursor = match cursor_frame {
                    Frame::BulkString(Some(cursor_bytes)) => {
                        let cursor_str = String::from_utf8_lossy(&cursor_bytes);
                        cursor_str
                            .parse::<u64>()
                            .map_err(|_| RedisError::Protocol("Invalid cursor".to_string()))?
                    }
                    _ => {
                        return Err(RedisError::Protocol(
                            "HSCAN cursor must be bulk string".to_string(),
                        ));
                    }
                };

                // Parse field-value pairs (flat array)
                let fields = match fields_frame {
                    Frame::Array(pair_frames) => {
                        if pair_frames.len() % 2 != 0 {
                            return Err(RedisError::Protocol(
                                "HSCAN must return even number of elements".to_string(),
                            ));
                        }

                        let mut fields = Vec::with_capacity(pair_frames.len() / 2);
                        let mut iter = pair_frames.into_iter();

                        while let (Some(field_frame), Some(value_frame)) =
                            (iter.next(), iter.next())
                        {
                            match (field_frame, value_frame) {
                                (
                                    Frame::BulkString(Some(field)),
                                    Frame::BulkString(Some(value)),
                                ) => {
                                    fields.push((field, value));
                                }
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "HSCAN elements must be bulk strings".to_string(),
                                    ));
                                }
                            }
                        }
                        fields
                    }
                    _ => {
                        return Err(RedisError::Protocol(
                            "HSCAN fields must be array".to_string(),
                        ));
                    }
                };

                Ok(HScanResult { cursor, fields })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}
