//! String commands (GET, SET, DEL, etc.)

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// GET command - retrieve a value
#[derive(Debug, Clone)]
pub struct Get {
    pub(crate) key: String,
}

impl Get {
    /// Create a new GET command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Get {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("GET"))),
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

/// INCRBY command - increment by amount
#[derive(Debug, Clone)]
pub struct IncrBy {
    pub(crate) key: String,
    pub(crate) increment: i64,
}

impl IncrBy {
    /// Create a new INCRBY command
    pub fn new(key: impl Into<String>, increment: i64) -> Self {
        Self {
            key: key.into(),
            increment,
        }
    }
}

impl Command for IncrBy {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("INCRBY"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.increment.to_string()))),
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

/// DECRBY command - decrement by amount
#[derive(Debug, Clone)]
pub struct DecrBy {
    pub(crate) key: String,
    pub(crate) decrement: i64,
}

impl DecrBy {
    /// Create a new DECRBY command
    pub fn new(key: impl Into<String>, decrement: i64) -> Self {
        Self {
            key: key.into(),
            decrement,
        }
    }
}

impl Command for DecrBy {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("DECRBY"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.decrement.to_string()))),
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

/// INCRBYFLOAT command - increment by floating point amount
#[derive(Debug, Clone)]
pub struct IncrByFloat {
    pub(crate) key: String,
    pub(crate) increment: f64,
}

impl IncrByFloat {
    /// Create a new INCRBYFLOAT command
    pub fn new(key: impl Into<String>, increment: f64) -> Self {
        Self {
            key: key.into(),
            increment,
        }
    }
}

impl Command for IncrByFloat {
    type Response = f64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("INCRBYFLOAT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.increment.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(&data);
                s.parse::<f64>()
                    .map_err(|_| RedisError::Protocol("Invalid float response".to_string()))
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// APPEND command - append value to key
#[derive(Debug, Clone)]
pub struct Append {
    pub(crate) key: String,
    pub(crate) value: Bytes,
}

impl Append {
    /// Create a new APPEND command
    pub fn new(key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl Command for Append {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("APPEND"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
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

/// STRLEN command - get string length
#[derive(Debug, Clone)]
pub struct StrLen {
    pub(crate) key: String,
}

impl StrLen {
    /// Create a new STRLEN command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for StrLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("STRLEN"))),
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

/// GETRANGE command - get substring
#[derive(Debug, Clone)]
pub struct GetRange {
    pub(crate) key: String,
    pub(crate) start: i64,
    pub(crate) end: i64,
}

impl GetRange {
    /// Create a new GETRANGE command
    pub fn new(key: impl Into<String>, start: i64, end: i64) -> Self {
        Self {
            key: key.into(),
            start,
            end,
        }
    }
}

impl Command for GetRange {
    type Response = Bytes;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("GETRANGE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.start.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.end.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(data),
            Frame::BulkString(None) | Frame::Null => Ok(Bytes::new()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SETRANGE command - overwrite part of string
#[derive(Debug, Clone)]
pub struct SetRange {
    pub(crate) key: String,
    pub(crate) offset: i64,
    pub(crate) value: Bytes,
}

impl SetRange {
    /// Create a new SETRANGE command
    pub fn new(key: impl Into<String>, offset: i64, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            offset,
            value: value.into(),
        }
    }
}

impl Command for SetRange {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SETRANGE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.offset.to_string()))),
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

/// GETEX command - get with expiration options
#[derive(Debug, Clone)]
pub struct GetEx {
    pub(crate) key: String,
    pub(crate) expiration: Option<GetExExpiration>,
}

/// Expiration options for GETEX
#[derive(Debug, Clone)]
pub enum GetExExpiration {
    /// EX seconds
    Ex(u64),
    /// PX milliseconds
    Px(u64),
    /// EXAT timestamp seconds
    ExAt(u64),
    /// PXAT timestamp milliseconds
    PxAt(u64),
    /// PERSIST - remove expiration
    Persist,
}

impl GetEx {
    /// Create a new GETEX command without expiration
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            expiration: None,
        }
    }

    /// Set expiration in seconds
    pub fn ex(mut self, seconds: u64) -> Self {
        self.expiration = Some(GetExExpiration::Ex(seconds));
        self
    }

    /// Set expiration in milliseconds
    pub fn px(mut self, milliseconds: u64) -> Self {
        self.expiration = Some(GetExExpiration::Px(milliseconds));
        self
    }

    /// Set expiration at Unix timestamp (seconds)
    pub fn exat(mut self, timestamp: u64) -> Self {
        self.expiration = Some(GetExExpiration::ExAt(timestamp));
        self
    }

    /// Set expiration at Unix timestamp (milliseconds)
    pub fn pxat(mut self, timestamp: u64) -> Self {
        self.expiration = Some(GetExExpiration::PxAt(timestamp));
        self
    }

    /// Remove expiration
    pub fn persist(mut self) -> Self {
        self.expiration = Some(GetExExpiration::Persist);
        self
    }
}

impl Command for GetEx {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("GETEX"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(exp) = &self.expiration {
            match exp {
                GetExExpiration::Ex(seconds) => {
                    frames.push(Frame::BulkString(Some(Bytes::from("EX"))));
                    frames.push(Frame::BulkString(Some(Bytes::from(seconds.to_string()))));
                }
                GetExExpiration::Px(millis) => {
                    frames.push(Frame::BulkString(Some(Bytes::from("PX"))));
                    frames.push(Frame::BulkString(Some(Bytes::from(millis.to_string()))));
                }
                GetExExpiration::ExAt(timestamp) => {
                    frames.push(Frame::BulkString(Some(Bytes::from("EXAT"))));
                    frames.push(Frame::BulkString(Some(Bytes::from(timestamp.to_string()))));
                }
                GetExExpiration::PxAt(timestamp) => {
                    frames.push(Frame::BulkString(Some(Bytes::from("PXAT"))));
                    frames.push(Frame::BulkString(Some(Bytes::from(timestamp.to_string()))));
                }
                GetExExpiration::Persist => {
                    frames.push(Frame::BulkString(Some(Bytes::from("PERSIST"))));
                }
            }
        }

        Frame::Array(frames)
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

/// GETDEL command - get and delete
#[derive(Debug, Clone)]
pub struct GetDel {
    pub(crate) key: String,
}

impl GetDel {
    /// Create a new GETDEL command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for GetDel {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("GETDEL"))),
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

/// PING command - test connection
///
/// # Examples
/// ```no_run
/// use redis_tower::commands::Ping;
///
/// // Simple ping
/// let cmd = Ping::new();
/// // Response: "PONG"
///
/// // Ping with message
/// let cmd = Ping::with_message("hello");
/// // Response: "hello"
/// ```
#[derive(Debug, Clone)]
pub struct Ping {
    pub(crate) message: Option<String>,
}

impl Ping {
    /// Create a new PING command
    pub fn new() -> Self {
        Self { message: None }
    }

    /// Create a PING command with a custom message
    pub fn with_message(message: impl Into<String>) -> Self {
        Self {
            message: Some(message.into()),
        }
    }
}

impl Default for Ping {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Ping {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![Frame::BulkString(Some(Bytes::from("PING")))];

        if let Some(msg) = &self.message {
            parts.push(Frame::BulkString(Some(Bytes::from(msg.clone()))));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::BulkString(Some(bytes)) => Ok(String::from_utf8_lossy(&bytes).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ECHO command - echo a message
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Echo;
///
/// let cmd = Echo::new("hello world");
/// // Response: "hello world"
/// ```
#[derive(Debug, Clone)]
pub struct Echo {
    pub(crate) message: String,
}

impl Echo {
    /// Create a new ECHO command
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Command for Echo {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("ECHO"))),
            Frame::BulkString(Some(Bytes::from(self.message.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(bytes)) => Ok(String::from_utf8_lossy(&bytes).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// EXISTS command - check if keys exist
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Exists;
///
/// // Check single key
/// let cmd = Exists::new("mykey");
/// // Response: 1 if exists, 0 if not
///
/// // Check multiple keys
/// let cmd = Exists::multiple(vec!["key1", "key2", "key3"]);
/// // Response: count of existing keys (0-3)
/// ```
#[derive(Debug, Clone)]
pub struct Exists {
    pub(crate) keys: Vec<String>,
}

impl Exists {
    /// Create a new EXISTS command for a single key
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    /// Create a new EXISTS command for multiple keys
    pub fn multiple<I, S>(keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for Exists {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![Frame::BulkString(Some(Bytes::from("EXISTS")))];

        for key in &self.keys {
            parts.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(count) => Ok(count),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TTL command - get time to live in seconds
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Ttl;
///
/// let cmd = Ttl::new("mykey");
/// // Response: -2 if key doesn't exist
/// //          -1 if key has no expiration
/// //          positive integer for TTL in seconds
/// ```
#[derive(Debug, Clone)]
pub struct Ttl {
    pub(crate) key: String,
}

impl Ttl {
    /// Create a new TTL command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Ttl {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("TTL"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(ttl) => Ok(ttl),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// EXPIRE command - set key expiration in seconds
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Expire;
///
/// let cmd = Expire::new("mykey", 60); // Expire in 60 seconds
/// // Response: 1 if timeout was set, 0 if key doesn't exist
/// ```
#[derive(Debug, Clone)]
pub struct Expire {
    pub(crate) key: String,
    pub(crate) seconds: u64,
}

impl Expire {
    /// Create a new EXPIRE command
    pub fn new(key: impl Into<String>, seconds: u64) -> Self {
        Self {
            key: key.into(),
            seconds,
        }
    }
}

impl Command for Expire {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("EXPIRE"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(Bytes::from(self.seconds.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(result) => Ok(result != 0),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// MSET command - set multiple key-value pairs
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Mset;
///
/// let cmd = Mset::new()
///     .pair("key1", b"value1".to_vec())
///     .pair("key2", b"value2".to_vec())
///     .pair("key3", b"value3".to_vec());
/// // Response: "OK"
/// ```
#[derive(Debug, Clone)]
pub struct Mset {
    pub(crate) pairs: Vec<(String, Bytes)>,
}

impl Mset {
    /// Create a new MSET command
    pub fn new() -> Self {
        Self { pairs: Vec::new() }
    }

    /// Add a key-value pair
    pub fn pair(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.pairs.push((key.into(), value.into()));
        self
    }

    /// Add multiple key-value pairs
    pub fn pairs<I, K, V>(mut self, pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<Bytes>,
    {
        self.pairs
            .extend(pairs.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }
}

impl Default for Mset {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Mset {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![Frame::BulkString(Some(Bytes::from("MSET")))];

        for (key, value) in &self.pairs {
            parts.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
            parts.push(Frame::BulkString(Some(value.clone())));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SET command - set a value
#[derive(Debug, Clone)]
pub struct Set {
    pub(crate) key: String,
    pub(crate) value: Bytes,
}

impl Set {
    /// Create a new SET command
    pub fn new(key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl Command for Set {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(self.value.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == b"OK"[..] => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// DEL command - delete one or more keys
#[derive(Debug, Clone)]
pub struct Del {
    pub(crate) keys: Vec<String>,
}

impl Del {
    /// Create a new DEL command
    pub fn new(keys: Vec<String>) -> Self {
        Self { keys }
    }
}

impl Command for Del {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("DEL")))];
        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
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

/// INCR command - increment a value atomically
#[derive(Debug, Clone)]
pub struct Incr {
    pub(crate) key: String,
}

impl Incr {
    /// Create a new INCR command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Incr {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("INCR"))),
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

/// DECR command - decrement a value atomically
#[derive(Debug, Clone)]
pub struct Decr {
    pub(crate) key: String,
}

impl Decr {
    /// Create a new DECR command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Decr {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("DECR"))),
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

/// MGET command - get multiple values at once
#[derive(Debug, Clone)]
pub struct MGet {
    pub(crate) keys: Vec<String>,
}

impl MGet {
    /// Create a new MGET command
    pub fn new(keys: Vec<String>) -> Self {
        Self { keys }
    }
}

impl Command for MGet {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("MGET")))];
        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(elements) => {
                let mut results = Vec::with_capacity(elements.len());
                for element in elements {
                    match element {
                        Frame::BulkString(Some(data)) => results.push(Some(data)),
                        Frame::BulkString(None) | Frame::Null => results.push(None),
                        Frame::Error(e) => {
                            let err_str = String::from_utf8_lossy(&e).to_string();
                            return Err(RedisError::Redis(err_str));
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
