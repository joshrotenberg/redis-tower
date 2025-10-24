//! List commands (LPUSH, RPOP, LRANGE, etc.)
//!
//! Includes both non-blocking (LPUSH, LPOP) and blocking (BLPOP, BRPOP) operations.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// LPUSH command - push elements to the head of a list
#[derive(Debug, Clone)]
pub struct LPush {
    pub(crate) key: String,
    pub(crate) values: Vec<Bytes>,
}

impl LPush {
    /// Create a new LPUSH command
    pub fn new(key: impl Into<String>, values: Vec<Bytes>) -> Self {
        Self {
            key: key.into(),
            values,
        }
    }

    /// Convenience method for pushing a single value
    pub fn single(key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            values: vec![value.into()],
        }
    }
}

impl Command for LPush {
    type Response = i64; // Returns the length of the list after push

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("LPUSH"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];
        for value in &self.values {
            frames.push(Frame::BulkString(Some(value.clone())));
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

/// RPUSH command - push elements to the tail of a list
#[derive(Debug, Clone)]
pub struct RPush {
    pub(crate) key: String,
    pub(crate) values: Vec<Bytes>,
}

impl RPush {
    /// Create a new RPUSH command
    pub fn new(key: impl Into<String>, values: Vec<Bytes>) -> Self {
        Self {
            key: key.into(),
            values,
        }
    }

    /// Convenience method for pushing a single value
    pub fn single(key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            values: vec![value.into()],
        }
    }
}

impl Command for RPush {
    type Response = i64; // Returns the length of the list after push

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("RPUSH"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];
        for value in &self.values {
            frames.push(Frame::BulkString(Some(value.clone())));
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

/// LPOP command - pop an element from the head of a list
#[derive(Debug, Clone)]
pub struct LPop {
    pub(crate) key: String,
}

impl LPop {
    /// Create a new LPOP command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for LPop {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LPOP"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(data)),
            Frame::BulkString(None) | Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// RPOP command - pop an element from the tail of a list
#[derive(Debug, Clone)]
pub struct RPop {
    pub(crate) key: String,
}

impl RPop {
    /// Create a new RPOP command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for RPop {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("RPOP"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(data)),
            Frame::BulkString(None) | Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// LRANGE command - get a range of elements from a list
#[derive(Debug, Clone)]
pub struct LRange {
    pub(crate) key: String,
    pub(crate) start: i64,
    pub(crate) stop: i64,
}

impl LRange {
    /// Create a new LRANGE command
    pub fn new(key: impl Into<String>, start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            start,
            stop,
        }
    }

    /// Get all elements in the list
    pub fn all(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            start: 0,
            stop: -1,
        }
    }
}

impl Command for LRange {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LRANGE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.start.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.stop.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(elements) => {
                let mut results = Vec::with_capacity(elements.len());
                for element in elements {
                    match element {
                        Frame::BulkString(Some(data)) => results.push(data),
                        Frame::Error(e) => {
                            let err_str = String::from_utf8_lossy(&e).to_string();
                            return Err(RedisError::Redis(err_str));
                        }
                        _ => {
                            return Err(RedisError::Protocol(
                                "LRANGE unexpected element type".to_string(),
                            ));
                        }
                    }
                }
                Ok(results)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ========== Blocking List Operations (Level 4) ==========

/// BLPOP command - blocking pop from head of list
///
/// Blocks until an element is available or timeout is reached.
/// Returns the key and value, or None if timeout occurs.
///
/// # Level 4 Complexity
/// - Blocks the connection until data arrives or timeout
/// - Returns (key, value) tuple instead of just value
/// - Timeout of 0 means block indefinitely
///
/// # Example
/// ```no_run
/// use redis_tower::commands::lists::BLPop;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisClient::connect("localhost:6379").await?;
///
/// // Block for up to 5 seconds
/// let result = client.call(BLPop::new(vec!["queue".to_string()], 5)).await?;
///
/// match result {
///     Some((key, value)) => println!("Got {} from {}",
///         String::from_utf8_lossy(&value),
///         String::from_utf8_lossy(&key)
///     ),
///     None => println!("Timeout - no data available"),
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct BLPop {
    pub(crate) keys: Vec<String>,
    pub(crate) timeout: u64, // seconds (0 = block forever)
}

impl BLPop {
    /// Create a new BLPOP command
    ///
    /// # Arguments
    /// * `keys` - List of keys to check (in order)
    /// * `timeout` - Timeout in seconds (0 to block indefinitely)
    pub fn new(keys: Vec<String>, timeout: u64) -> Self {
        Self { keys, timeout }
    }
}

/// Result from blocking pop operations
///
/// Contains the key that provided the value and the value itself.
/// None indicates timeout occurred.
pub type BlockingPopResult = Option<(Bytes, Bytes)>;

impl Command for BLPop {
    type Response = BlockingPopResult;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("BLPOP")))];

        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        frames.push(Frame::BulkString(Some(Bytes::from(
            self.timeout.to_string(),
        ))));

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // Timeout - returns null
            Frame::Null | Frame::BulkString(None) => Ok(None),

            // Success - returns array [key, value]
            Frame::Array(mut elements) if elements.len() == 2 => {
                let value = elements.pop().unwrap();
                let key = elements.pop().unwrap();

                match (key, value) {
                    (Frame::BulkString(Some(k)), Frame::BulkString(Some(v))) => Ok(Some((k, v))),
                    _ => Err(RedisError::Protocol(
                        "BLPOP response must be two bulk strings".to_string(),
                    )),
                }
            }

            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),

            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// Read-only trait implementations for cluster read-from-replica support
use crate::cluster::read_preference::ReadOnly;

impl ReadOnly for LRange {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for LLen {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for LIndex {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for LPos {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands - explicitly implement with default (false) for clarity
impl ReadOnly for LPush {}
impl ReadOnly for RPush {}
impl ReadOnly for LPop {}
impl ReadOnly for RPop {}
impl ReadOnly for BLPop {}
impl ReadOnly for BRPop {}
impl ReadOnly for LSet {}
impl ReadOnly for LInsert {}
impl ReadOnly for LRem {}
impl ReadOnly for LTrim {}

/// LLEN command - get list length
#[derive(Debug, Clone)]
pub struct LLen {
    pub(crate) key: String,
}

impl LLen {
    /// Create a new LLEN command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for LLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LLEN"))),
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

/// LINDEX command - get element by index
#[derive(Debug, Clone)]
pub struct LIndex {
    pub(crate) key: String,
    pub(crate) index: i64,
}

impl LIndex {
    /// Create a new LINDEX command
    pub fn new(key: impl Into<String>, index: i64) -> Self {
        Self {
            key: key.into(),
            index,
        }
    }
}

impl Command for LIndex {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LINDEX"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.index.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(data)),
            Frame::BulkString(None) | Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// LSET command - set element at index
#[derive(Debug, Clone)]
pub struct LSet {
    pub(crate) key: String,
    pub(crate) index: i64,
    pub(crate) value: Bytes,
}

impl LSet {
    /// Create a new LSET command
    pub fn new(key: impl Into<String>, index: i64, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            index,
            value: value.into(),
        }
    }
}

impl Command for LSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LSET"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.index.to_string()))),
            Frame::BulkString(Some(self.value.clone())),
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

/// LINSERT command - insert before or after pivot
#[derive(Debug, Clone)]
pub struct LInsert {
    pub(crate) key: String,
    pub(crate) position: InsertPosition,
    pub(crate) pivot: Bytes,
    pub(crate) value: Bytes,
}

/// Position for LINSERT
#[derive(Debug, Clone)]
pub enum InsertPosition {
    /// Insert before pivot
    Before,
    /// Insert after pivot
    After,
}

impl LInsert {
    /// Create a new LINSERT BEFORE command
    pub fn before(
        key: impl Into<String>,
        pivot: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Self {
        Self {
            key: key.into(),
            position: InsertPosition::Before,
            pivot: pivot.into(),
            value: value.into(),
        }
    }

    /// Create a new LINSERT AFTER command
    pub fn after(key: impl Into<String>, pivot: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            position: InsertPosition::After,
            pivot: pivot.into(),
            value: value.into(),
        }
    }
}

impl Command for LInsert {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let position_str = match self.position {
            InsertPosition::Before => "BEFORE",
            InsertPosition::After => "AFTER",
        };

        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LINSERT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(position_str))),
            Frame::BulkString(Some(self.pivot.clone())),
            Frame::BulkString(Some(self.value.clone())),
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

/// LREM command - remove elements
#[derive(Debug, Clone)]
pub struct LRem {
    pub(crate) key: String,
    pub(crate) count: i64,
    pub(crate) value: Bytes,
}

impl LRem {
    /// Create a new LREM command
    ///
    /// count > 0: Remove elements equal to value moving from head to tail
    /// count < 0: Remove elements equal to value moving from tail to head
    /// count = 0: Remove all elements equal to value
    pub fn new(key: impl Into<String>, count: i64, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            count,
            value: value.into(),
        }
    }

    /// Remove all occurrences
    pub fn all(key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self::new(key, 0, value)
    }
}

impl Command for LRem {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LREM"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.count.to_string()))),
            Frame::BulkString(Some(self.value.clone())),
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

/// LTRIM command - trim list to range
#[derive(Debug, Clone)]
pub struct LTrim {
    pub(crate) key: String,
    pub(crate) start: i64,
    pub(crate) stop: i64,
}

impl LTrim {
    /// Create a new LTRIM command
    pub fn new(key: impl Into<String>, start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            start,
            stop,
        }
    }
}

impl Command for LTrim {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LTRIM"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.start.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.stop.to_string()))),
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

/// LPOS command - find position of element
#[derive(Debug, Clone)]
pub struct LPos {
    pub(crate) key: String,
    pub(crate) element: Bytes,
    pub(crate) rank: Option<i64>,
    pub(crate) count: Option<i64>,
    pub(crate) maxlen: Option<i64>,
}

impl LPos {
    /// Create a new LPOS command
    pub fn new(key: impl Into<String>, element: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
            rank: None,
            count: None,
            maxlen: None,
        }
    }

    /// Specify rank (1 = first, 2 = second, -1 = first from tail)
    pub fn rank(mut self, rank: i64) -> Self {
        self.rank = Some(rank);
        self
    }

    /// Return up to count matches
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Limit search to maxlen elements
    pub fn maxlen(mut self, maxlen: i64) -> Self {
        self.maxlen = Some(maxlen);
        self
    }
}

impl Command for LPos {
    type Response = Option<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("LPOS"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(self.element.clone())),
        ];

        if let Some(rank) = self.rank {
            frames.push(Frame::BulkString(Some(Bytes::from("RANK"))));
            frames.push(Frame::BulkString(Some(Bytes::from(rank.to_string()))));
        }

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        if let Some(maxlen) = self.maxlen {
            frames.push(Frame::BulkString(Some(Bytes::from("MAXLEN"))));
            frames.push(Frame::BulkString(Some(Bytes::from(maxlen.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(Some(n)),
            Frame::Null | Frame::BulkString(None) => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// BRPOP command - blocking pop from tail of list
///
/// Same behavior as BLPOP but pops from the tail instead of head.
#[derive(Debug, Clone)]
pub struct BRPop {
    pub(crate) keys: Vec<String>,
    pub(crate) timeout: u64,
}

impl BRPop {
    /// Create a new BRPOP command
    ///
    /// # Arguments
    /// * `keys` - List of keys to check (in order)
    /// * `timeout` - Timeout in seconds (0 to block indefinitely)
    pub fn new(keys: Vec<String>, timeout: u64) -> Self {
        Self { keys, timeout }
    }
}

impl Command for BRPop {
    type Response = BlockingPopResult;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("BRPOP")))];

        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        frames.push(Frame::BulkString(Some(Bytes::from(
            self.timeout.to_string(),
        ))));

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Null | Frame::BulkString(None) => Ok(None),

            Frame::Array(mut elements) if elements.len() == 2 => {
                let value = elements.pop().unwrap();
                let key = elements.pop().unwrap();

                match (key, value) {
                    (Frame::BulkString(Some(k)), Frame::BulkString(Some(v))) => Ok(Some((k, v))),
                    _ => Err(RedisError::Protocol(
                        "BRPOP response must be two bulk strings".to_string(),
                    )),
                }
            }

            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),

            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}
