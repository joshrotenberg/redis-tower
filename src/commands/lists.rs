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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lpush_single() {
        let cmd = LPush::single("mylist", Bytes::from("value1"));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LPUSH"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("mylist"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("value1"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_lpush_multiple() {
        let cmd = LPush::new("mylist", vec![Bytes::from("v1"), Bytes::from("v2")]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LPUSH"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_rpush_single() {
        let cmd = RPush::single("mylist", Bytes::from("value1"));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("RPUSH"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_lpop_single() {
        let cmd = LPop::new("mylist");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LPOP"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("mylist"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_rpop_single() {
        let cmd = RPop::new("mylist");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("RPOP"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_lrange_frame() {
        let cmd = LRange::new("mylist", 0, -1);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LRANGE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("mylist"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("0"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("-1"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_blpop_frame() {
        let cmd = BLPop::new(vec!["list1".to_string(), "list2".to_string()], 5);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("BLPOP"))));
                assert_eq!(parts.len(), 4); // BLPOP list1 list2 5
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_brpop_frame() {
        let cmd = BRPop::new(vec!["list1".to_string()], 10);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("BRPOP"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("10"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_lpushx_frame() {
        let cmd = LPushX::single("mylist", Bytes::from("value1"));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LPUSHX"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_rpushx_frame() {
        let cmd = RPushX::single("mylist", Bytes::from("value1"));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("RPUSHX"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_lmove_frame() {
        let cmd = LMove::new("src", "dst", MoveDirection::Left, MoveDirection::Right);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LMOVE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("src"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("dst"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("LEFT"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("RIGHT"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_blmove_frame() {
        let cmd = BLMove::new("src", "dst", MoveDirection::Right, MoveDirection::Left, 5.0);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 6);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("BLMOVE"))));
                assert_eq!(parts[5], Frame::BulkString(Some(Bytes::from("5"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_lmpop_frame() {
        let cmd = LMPop::new(vec!["list1".to_string()], MoveDirection::Left).count(2);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LMPOP"))));
                assert!(parts.len() >= 5); // LMPOP 1 list1 LEFT COUNT 2
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_blmpop_frame() {
        let cmd = BLMPop::new(vec!["list1".to_string()], MoveDirection::Right, 5.0);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("BLMPOP"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("5"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    #[cfg(feature = "deprecated")]
    #[allow(deprecated)]
    fn test_rpoplpush_frame() {
        let cmd = RPopLPush::new("source", "destination");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("RPOPLPUSH"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("source"))));
                assert_eq!(
                    parts[2],
                    Frame::BulkString(Some(Bytes::from("destination")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    #[cfg(feature = "deprecated")]
    #[allow(deprecated)]
    fn test_brpoplpush_frame() {
        let cmd = BRPopLPush::new("source", "destination", 10.0);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("BRPOPLPUSH"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("10"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_llen_frame() {
        let cmd = LLen::new("mylist");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LLEN"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("mylist"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_lindex_frame() {
        let cmd = LIndex::new("mylist", 5);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LINDEX"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("5"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_lset_frame() {
        let cmd = LSet::new("mylist", 2, Bytes::from("newvalue"));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LSET"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("2"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("newvalue"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_linsert_before() {
        let cmd = LInsert::before("mylist", Bytes::from("pivot"), Bytes::from("value"));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LINSERT"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("BEFORE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_linsert_after() {
        let cmd = LInsert::after("mylist", Bytes::from("pivot"), Bytes::from("value"));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("AFTER"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_lrem_frame() {
        let cmd = LRem::new("mylist", 2, Bytes::from("value"));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LREM"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("2"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_ltrim_frame() {
        let cmd = LTrim::new("mylist", 0, 99);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LTRIM"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("0"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("99"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_lpos_basic() {
        let cmd = LPos::new("mylist", Bytes::from("element"));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LPOS"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("mylist"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_lpos_with_options() {
        let cmd = LPos::new("mylist", Bytes::from("element"))
            .rank(2)
            .count(3)
            .maxlen(100);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LPOS"))));
                assert!(parts.len() > 3); // Has RANK, COUNT, MAXLEN options
            }
            _ => panic!("Expected Array frame"),
        }
    }
}

/// LPUSHX command - push to head only if list exists
///
/// Similar to LPUSH but only pushes if the key already exists and holds a list.
/// Returns the length of the list after push, or 0 if key doesn't exist.
#[derive(Debug, Clone)]
pub struct LPushX {
    pub(crate) key: String,
    pub(crate) values: Vec<Bytes>,
}

impl LPushX {
    /// Create a new LPUSHX command
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

impl Command for LPushX {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("LPUSHX"))),
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

/// RPUSHX command - push to tail only if list exists
///
/// Similar to RPUSH but only pushes if the key already exists and holds a list.
/// Returns the length of the list after push, or 0 if key doesn't exist.
#[derive(Debug, Clone)]
pub struct RPushX {
    pub(crate) key: String,
    pub(crate) values: Vec<Bytes>,
}

impl RPushX {
    /// Create a new RPUSHX command
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

impl Command for RPushX {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("RPUSHX"))),
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

/// Direction for LMOVE/BLMOVE
#[derive(Debug, Clone, Copy)]
pub enum MoveDirection {
    /// Pop from/push to left (head)
    Left,
    /// Pop from/push to right (tail)
    Right,
}

impl MoveDirection {
    fn as_str(&self) -> &'static str {
        match self {
            MoveDirection::Left => "LEFT",
            MoveDirection::Right => "RIGHT",
        }
    }
}

/// LMOVE command - atomically move element between lists
///
/// Atomically pops an element from source list and pushes to destination list.
/// Returns the element that was moved, or None if source list is empty.
///
/// # Redis 6.2.0+
///
/// # Example
/// ```no_run
/// use redis_tower::commands::lists::{LMove, MoveDirection};
///
/// // Move from tail of source to head of destination
/// let cmd = LMove::new("source", "dest", MoveDirection::Right, MoveDirection::Left);
/// ```
#[derive(Debug, Clone)]
pub struct LMove {
    pub(crate) source: String,
    pub(crate) destination: String,
    pub(crate) from: MoveDirection,
    pub(crate) to: MoveDirection,
}

impl LMove {
    /// Create a new LMOVE command
    pub fn new(
        source: impl Into<String>,
        destination: impl Into<String>,
        from: MoveDirection,
        to: MoveDirection,
    ) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
            from,
            to,
        }
    }
}

impl Command for LMove {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("LMOVE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.source.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.destination.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.from.as_str()))),
            Frame::BulkString(Some(Bytes::from(self.to.as_str()))),
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

/// BLMOVE command - blocking version of LMOVE
///
/// Like LMOVE but blocks if source list is empty.
/// Returns the element that was moved, or None if timeout occurred.
///
/// # Redis 6.2.0+
///
/// # Example
/// ```no_run
/// use redis_tower::commands::lists::{BLMove, MoveDirection};
///
/// // Block for up to 5 seconds
/// let cmd = BLMove::new("source", "dest", MoveDirection::Left, MoveDirection::Right, 5.0);
/// ```
#[derive(Debug, Clone)]
pub struct BLMove {
    pub(crate) source: String,
    pub(crate) destination: String,
    pub(crate) from: MoveDirection,
    pub(crate) to: MoveDirection,
    pub(crate) timeout: f64, // seconds (0 = block forever)
}

impl BLMove {
    /// Create a new BLMOVE command
    ///
    /// # Arguments
    /// * `source` - Source list key
    /// * `destination` - Destination list key
    /// * `from` - Direction to pop from source (Left or Right)
    /// * `to` - Direction to push to destination (Left or Right)
    /// * `timeout` - Timeout in seconds (0.0 to block indefinitely)
    pub fn new(
        source: impl Into<String>,
        destination: impl Into<String>,
        from: MoveDirection,
        to: MoveDirection,
        timeout: f64,
    ) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
            from,
            to,
            timeout,
        }
    }
}

impl Command for BLMove {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("BLMOVE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.source.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.destination.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.from.as_str()))),
            Frame::BulkString(Some(Bytes::from(self.to.as_str()))),
            Frame::BulkString(Some(Bytes::from(self.timeout.to_string()))),
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

/// LMPOP command - pop elements from multiple lists
///
/// Pops one or more elements from the first non-empty list.
/// Returns the key and popped elements, or None if all lists are empty.
///
/// # Redis 7.0.0+
///
/// # Example
/// ```no_run
/// use redis_tower::commands::lists::{LMPop, MoveDirection};
///
/// // Pop one element from left of first non-empty list
/// let cmd = LMPop::new(vec!["list1".to_string(), "list2".to_string()], MoveDirection::Left);
///
/// // Pop up to 3 elements
/// let cmd = LMPop::new(vec!["list1".to_string()], MoveDirection::Right).count(3);
/// ```
#[derive(Debug, Clone)]
pub struct LMPop {
    pub(crate) keys: Vec<String>,
    pub(crate) direction: MoveDirection,
    pub(crate) count: Option<i64>,
}

impl LMPop {
    /// Create a new LMPOP command
    pub fn new(keys: Vec<String>, direction: MoveDirection) -> Self {
        Self {
            keys,
            direction,
            count: None,
        }
    }

    /// Set the number of elements to pop
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

/// Result from LMPOP - contains key and popped elements
pub type LMPopResult = Option<(String, Vec<Bytes>)>;

impl Command for LMPop {
    type Response = LMPopResult;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("LMPOP"))),
            Frame::BulkString(Some(Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        frames.push(Frame::BulkString(Some(Bytes::from(
            self.direction.as_str(),
        ))));

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Null | Frame::BulkString(None) => Ok(None),

            Frame::Array(mut outer) if outer.len() == 2 => {
                let elements = outer.pop().unwrap();
                let key = outer.pop().unwrap();

                match (key, elements) {
                    (Frame::BulkString(Some(k)), Frame::Array(items)) => {
                        let key_str = String::from_utf8_lossy(&k).to_string();
                        let mut values = Vec::with_capacity(items.len());

                        for item in items {
                            match item {
                                Frame::BulkString(Some(v)) => values.push(v),
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "LMPOP elements must be bulk strings".to_string(),
                                    ));
                                }
                            }
                        }

                        Ok(Some((key_str, values)))
                    }
                    _ => Err(RedisError::Protocol(
                        "LMPOP response format incorrect".to_string(),
                    )),
                }
            }

            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),

            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// BLMPOP command - blocking version of LMPOP
///
/// Like LMPOP but blocks if all lists are empty.
///
/// # Redis 7.0.0+
///
/// # Example
/// ```no_run
/// use redis_tower::commands::lists::{BLMPop, MoveDirection};
///
/// // Block for up to 5 seconds
/// let cmd = BLMPop::new(vec!["list1".to_string()], MoveDirection::Left, 5.0);
/// ```
#[derive(Debug, Clone)]
pub struct BLMPop {
    pub(crate) keys: Vec<String>,
    pub(crate) direction: MoveDirection,
    pub(crate) timeout: f64,
    pub(crate) count: Option<i64>,
}

impl BLMPop {
    /// Create a new BLMPOP command
    ///
    /// # Arguments
    /// * `keys` - List keys to check (in order)
    /// * `direction` - Direction to pop from (Left or Right)
    /// * `timeout` - Timeout in seconds (0.0 to block indefinitely)
    pub fn new(keys: Vec<String>, direction: MoveDirection, timeout: f64) -> Self {
        Self {
            keys,
            direction,
            timeout,
            count: None,
        }
    }

    /// Set the number of elements to pop
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for BLMPop {
    type Response = LMPopResult;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("BLMPOP"))),
            Frame::BulkString(Some(Bytes::from(self.timeout.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        frames.push(Frame::BulkString(Some(Bytes::from(
            self.direction.as_str(),
        ))));

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Null | Frame::BulkString(None) => Ok(None),

            Frame::Array(mut outer) if outer.len() == 2 => {
                let elements = outer.pop().unwrap();
                let key = outer.pop().unwrap();

                match (key, elements) {
                    (Frame::BulkString(Some(k)), Frame::Array(items)) => {
                        let key_str = String::from_utf8_lossy(&k).to_string();
                        let mut values = Vec::with_capacity(items.len());

                        for item in items {
                            match item {
                                Frame::BulkString(Some(v)) => values.push(v),
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "BLMPOP elements must be bulk strings".to_string(),
                                    ));
                                }
                            }
                        }

                        Ok(Some((key_str, values)))
                    }
                    _ => Err(RedisError::Protocol(
                        "BLMPOP response format incorrect".to_string(),
                    )),
                }
            }

            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),

            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// DEPRECATED COMMANDS (feature-gated with "deprecated")
// ============================================================================

#[cfg(feature = "deprecated")]
/// RPOPLPUSH command - Pop from source and push to destination (DEPRECATED)
///
/// **DEPRECATED**: As of Redis 6.2.0, use `LMove` instead.
///
/// Atomically returns and removes the last element of the source list,
/// and pushes the element to the destination list.
///
/// # Migration Guide
///
/// ```no_run
/// use redis_tower::commands::LMove;
/// use redis_tower::commands::MoveDirection;
///
/// // Old (deprecated - requires "deprecated" feature):
/// // use redis_tower::commands::RPopLPush;
/// // let cmd = RPopLPush::new("source", "dest");
///
/// // New (preferred):
/// let cmd = LMove::new("source", "dest", MoveDirection::Right, MoveDirection::Left);
/// ```
#[derive(Debug, Clone)]
pub struct RPopLPush {
    source: String,
    destination: String,
}

#[cfg(feature = "deprecated")]
impl RPopLPush {
    /// Create a new RPOPLPUSH command
    #[deprecated(since = "6.2.0", note = "Use LMove instead")]
    pub fn new(source: impl Into<String>, destination: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
        }
    }
}

#[cfg(feature = "deprecated")]
impl Command for RPopLPush {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("RPOPLPUSH"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.source.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.destination.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(data) => Ok(data),
            Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(feature = "deprecated")]
/// BRPOPLPUSH command - Blocking RPOPLPUSH (DEPRECATED)
///
/// **DEPRECATED**: As of Redis 6.2.0, use `BLMove` instead.
///
/// Blocking version of RPOPLPUSH. Waits for an element to be available
/// or timeout expires.
///
/// # Migration Guide
///
/// ```no_run
/// use redis_tower::commands::BLMove;
/// use redis_tower::commands::MoveDirection;
///
/// // Old (deprecated - requires "deprecated" feature):
/// // use redis_tower::commands::BRPopLPush;
/// // let cmd = BRPopLPush::new("source", "dest", 5.0);
///
/// // New (preferred):
/// let cmd = BLMove::new("source", "dest", MoveDirection::Right, MoveDirection::Left, 5.0);
/// ```
#[derive(Debug, Clone)]
pub struct BRPopLPush {
    source: String,
    destination: String,
    timeout: f64,
}

#[cfg(feature = "deprecated")]
impl BRPopLPush {
    /// Create a new BRPOPLPUSH command
    #[deprecated(since = "6.2.0", note = "Use BLMove instead")]
    pub fn new(source: impl Into<String>, destination: impl Into<String>, timeout: f64) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
            timeout,
        }
    }
}

#[cfg(feature = "deprecated")]
impl Command for BRPopLPush {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("BRPOPLPUSH"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.source.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.destination.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.timeout.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(data) => Ok(data),
            Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// Read-only trait implementations for cluster read-from-replica support
use crate::read_preference::ReadOnly;

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
impl ReadOnly for LPushX {}
impl ReadOnly for RPushX {}
impl ReadOnly for LPop {}
impl ReadOnly for RPop {}
impl ReadOnly for BLPop {}
impl ReadOnly for BRPop {}
impl ReadOnly for LMove {}
impl ReadOnly for BLMove {}
impl ReadOnly for LMPop {}
impl ReadOnly for BLMPop {}
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
