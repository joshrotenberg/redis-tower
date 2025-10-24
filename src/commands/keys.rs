//! Key management commands for Redis
//!
//! Commands for managing key lifetimes, renaming, and introspection.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// PERSIST command - Remove expiration from a key
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Persist;
///
/// let cmd = Persist::new("mykey");
/// ```
#[derive(Debug, Clone)]
pub struct Persist {
    key: String,
}

impl Persist {
    /// Create a new PERSIST command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Persist {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("PERSIST"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),  // Timeout was removed
            Frame::Integer(0) => Ok(false), // Key does not exist or has no timeout
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// PEXPIRE command - Set key expiration in milliseconds
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::PExpire;
///
/// // Expire in 5000 milliseconds (5 seconds)
/// let cmd = PExpire::new("mykey", 5000);
/// ```
#[derive(Debug, Clone)]
pub struct PExpire {
    key: String,
    milliseconds: i64,
}

impl PExpire {
    /// Create a new PEXPIRE command
    pub fn new(key: impl Into<String>, milliseconds: i64) -> Self {
        Self {
            key: key.into(),
            milliseconds,
        }
    }
}

impl Command for PExpire {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("PEXPIRE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.milliseconds.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),  // Timeout was set
            Frame::Integer(0) => Ok(false), // Key does not exist
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// PTTL command - Get key time-to-live in milliseconds
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::PTtl;
///
/// let cmd = PTtl::new("mykey");
/// ```
///
/// # Return values
/// - Positive number: TTL in milliseconds
/// - -1: Key exists but has no expiration
/// - -2: Key does not exist
#[derive(Debug, Clone)]
pub struct PTtl {
    key: String,
}

impl PTtl {
    /// Create a new PTTL command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for PTtl {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("PTTL"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
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

/// EXPIREAT command - Set key expiration as Unix timestamp (seconds)
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ExpireAt;
///
/// // Expire at Unix timestamp 1672531200 (2023-01-01 00:00:00 UTC)
/// let cmd = ExpireAt::new("mykey", 1672531200);
/// ```
#[derive(Debug, Clone)]
pub struct ExpireAt {
    key: String,
    timestamp: i64,
}

impl ExpireAt {
    /// Create a new EXPIREAT command
    pub fn new(key: impl Into<String>, timestamp: i64) -> Self {
        Self {
            key: key.into(),
            timestamp,
        }
    }
}

impl Command for ExpireAt {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("EXPIREAT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.timestamp.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),  // Timeout was set
            Frame::Integer(0) => Ok(false), // Key does not exist
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// PEXPIREAT command - Set key expiration as Unix timestamp (milliseconds)
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::PExpireAt;
///
/// // Expire at Unix timestamp 1672531200000 (2023-01-01 00:00:00 UTC)
/// let cmd = PExpireAt::new("mykey", 1672531200000);
/// ```
#[derive(Debug, Clone)]
pub struct PExpireAt {
    key: String,
    milliseconds_timestamp: i64,
}

impl PExpireAt {
    /// Create a new PEXPIREAT command
    pub fn new(key: impl Into<String>, milliseconds_timestamp: i64) -> Self {
        Self {
            key: key.into(),
            milliseconds_timestamp,
        }
    }
}

impl Command for PExpireAt {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("PEXPIREAT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.milliseconds_timestamp.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),  // Timeout was set
            Frame::Integer(0) => Ok(false), // Key does not exist
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// RENAME command - Rename a key
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Rename;
///
/// let cmd = Rename::new("oldkey", "newkey");
/// ```
///
/// Note: If newkey already exists, it is overwritten.
#[derive(Debug, Clone)]
pub struct Rename {
    key: String,
    new_key: String,
}

impl Rename {
    /// Create a new RENAME command
    pub fn new(key: impl Into<String>, new_key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            new_key: new_key.into(),
        }
    }
}

impl Command for Rename {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("RENAME"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.new_key.as_bytes()))),
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

/// RENAMENX command - Rename a key only if new key does not exist
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::RenameNx;
///
/// let cmd = RenameNx::new("oldkey", "newkey");
/// ```
#[derive(Debug, Clone)]
pub struct RenameNx {
    key: String,
    new_key: String,
}

impl RenameNx {
    /// Create a new RENAMENX command
    pub fn new(key: impl Into<String>, new_key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            new_key: new_key.into(),
        }
    }
}

impl Command for RenameNx {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("RENAMENX"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.new_key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),  // Key was renamed
            Frame::Integer(0) => Ok(false), // New key already exists
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TYPE command - Get the type of a key
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Type;
///
/// let cmd = Type::new("mykey");
/// ```
///
/// # Return values
/// Returns one of: "string", "list", "set", "zset", "hash", "stream", or "none"
#[derive(Debug, Clone)]
pub struct Type {
    key: String,
}

impl Type {
    /// Create a new TYPE command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Type {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("TYPE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).into_owned()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// KEYS command - Find all keys matching a pattern
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Keys;
///
/// // Get all keys
/// let cmd = Keys::new("*");
///
/// // Get keys with prefix
/// let cmd = Keys::new("user:*");
///
/// // Get keys with pattern
/// let cmd = Keys::new("user:[0-9]*");
/// ```
///
/// Warning: This command should be used with caution in production as it blocks
/// the server. Consider using SCAN instead for production use cases.
#[derive(Debug, Clone)]
pub struct Keys {
    pattern: String,
}

impl Keys {
    /// Create a new KEYS command
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
        }
    }
}

impl Command for Keys {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("KEYS"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.pattern.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut keys = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            keys.push(String::from_utf8_lossy(&data).into_owned());
                        }
                        Frame::BulkString(None) | Frame::Null => {
                            // Skip null entries
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(keys)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TOUCH command - Update the access time of one or more keys
///
/// Returns the number of keys that were touched (existed).
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Touch;
///
/// let cmd = Touch::new(vec!["key1".to_string(), "key2".to_string()]);
/// ```
#[derive(Debug, Clone)]
pub struct Touch {
    keys: Vec<String>,
}

impl Touch {
    /// Create a new TOUCH command
    pub fn new(keys: Vec<String>) -> Self {
        Self { keys }
    }

    /// Create a TOUCH command for a single key
    pub fn single(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }
}

impl Command for Touch {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("TOUCH")))];
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

/// UNLINK command - Async delete of one or more keys
///
/// Like DEL but performs the actual memory reclamation in a different thread,
/// so it doesn't block. Returns the number of keys that were unlinked.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Unlink;
///
/// let cmd = Unlink::new(vec!["key1".to_string(), "key2".to_string()]);
/// ```
#[derive(Debug, Clone)]
pub struct Unlink {
    keys: Vec<String>,
}

impl Unlink {
    /// Create a new UNLINK command
    pub fn new(keys: Vec<String>) -> Self {
        Self { keys }
    }

    /// Create an UNLINK command for a single key
    pub fn single(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }
}

impl Command for Unlink {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("UNLINK")))];
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

/// COPY command - Copy a key to a new key (Redis 6.2+)
///
/// Returns true if the copy was successful, false otherwise.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Copy;
///
/// // Simple copy
/// let cmd = Copy::new("source", "dest");
///
/// // Copy with REPLACE option
/// let cmd = Copy::new("source", "dest").replace();
///
/// // Copy to different database
/// let cmd = Copy::new("source", "dest").db(2);
/// ```
#[derive(Debug, Clone)]
pub struct Copy {
    source: String,
    destination: String,
    db: Option<i64>,
    replace: bool,
}

impl Copy {
    /// Create a new COPY command
    pub fn new(source: impl Into<String>, destination: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
            db: None,
            replace: false,
        }
    }

    /// Copy to a specific database
    pub fn db(mut self, db: i64) -> Self {
        self.db = Some(db);
        self
    }

    /// Replace destination key if it exists
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }
}

impl Command for Copy {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("COPY"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.source.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.destination.as_bytes()))),
        ];

        if let Some(db) = self.db {
            frames.push(Frame::BulkString(Some(Bytes::from("DB"))));
            frames.push(Frame::BulkString(Some(Bytes::from(db.to_string()))));
        }

        if self.replace {
            frames.push(Frame::BulkString(Some(Bytes::from("REPLACE"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),
            Frame::Integer(0) => Ok(false),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// MOVE command - Move a key to a different database
///
/// Returns true if the key was moved, false if the key doesn't exist
/// or the target database already has a key with that name.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Move;
///
/// let cmd = Move::new("mykey", 2); // Move to database 2
/// ```
#[derive(Debug, Clone)]
pub struct Move {
    key: String,
    db: i64,
}

impl Move {
    /// Create a new MOVE command
    pub fn new(key: impl Into<String>, db: i64) -> Self {
        Self {
            key: key.into(),
            db,
        }
    }
}

impl Command for Move {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("MOVE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.db.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),
            Frame::Integer(0) => Ok(false),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// EXPIRETIME command - Get the absolute Unix timestamp at which a key will expire
///
/// Returns the expiration Unix timestamp in seconds, or:
/// - -1 if the key exists but has no expiration
/// - -2 if the key does not exist
///
/// Available since Redis 7.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ExpireTime;
///
/// let cmd = ExpireTime::new("mykey");
/// ```
#[derive(Debug, Clone)]
pub struct ExpireTime {
    key: String,
}

impl ExpireTime {
    /// Create a new EXPIRETIME command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ExpireTime {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("EXPIRETIME"))),
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

/// PEXPIRETIME command - Get the absolute Unix timestamp at which a key will expire (milliseconds)
///
/// Returns the expiration Unix timestamp in milliseconds, or:
/// - -1 if the key exists but has no expiration
/// - -2 if the key does not exist
///
/// Available since Redis 7.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::PExpireTime;
///
/// let cmd = PExpireTime::new("mykey");
/// ```
#[derive(Debug, Clone)]
pub struct PExpireTime {
    key: String,
}

impl PExpireTime {
    /// Create a new PEXPIRETIME command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for PExpireTime {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("PEXPIRETIME"))),
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

/// DUMP command - Serialize value at key
///
/// Returns a serialized version of the value stored at the specified key.
/// The returned value can be restored using RESTORE.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Dump;
///
/// let cmd = Dump::new("mykey");
/// ```
#[derive(Debug, Clone)]
pub struct Dump {
    key: String,
}

impl Dump {
    /// Create a new DUMP command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Dump {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("DUMP"))),
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

/// RESTORE command - Deserialize value to key
///
/// Creates a key associated with a value that is obtained via DUMP.
/// Available with TTL and optional REPLACE modifier.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Restore;
/// use bytes::Bytes;
///
/// // Restore without TTL
/// let cmd = Restore::new("mykey", 0, Bytes::from(vec![1, 2, 3]));
///
/// // Restore with 10 second TTL
/// let cmd = Restore::new("mykey", 10000, Bytes::from(vec![1, 2, 3]));
///
/// // Restore with REPLACE option
/// let cmd = Restore::new("mykey", 0, Bytes::from(vec![1, 2, 3])).replace();
///
/// // Restore with absolute TTL (Redis 5.0+)
/// let cmd = Restore::new("mykey", 1735689600000, Bytes::from(vec![1, 2, 3])).absttl();
/// ```
#[derive(Debug, Clone)]
pub struct Restore {
    key: String,
    ttl: i64,
    serialized_value: Bytes,
    replace: bool,
    absttl: bool,
    idletime: Option<i64>,
    freq: Option<i64>,
}

impl Restore {
    /// Create a new RESTORE command
    ///
    /// # Arguments
    /// * `key` - The key to restore
    /// * `ttl` - Time to live in milliseconds (0 for no expiry)
    /// * `serialized_value` - Serialized value from DUMP
    pub fn new(key: impl Into<String>, ttl: i64, serialized_value: Bytes) -> Self {
        Self {
            key: key.into(),
            ttl,
            serialized_value,
            replace: false,
            absttl: false,
            idletime: None,
            freq: None,
        }
    }

    /// Replace existing key if it exists
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }

    /// TTL is absolute Unix timestamp in milliseconds (Redis 5.0+)
    pub fn absttl(mut self) -> Self {
        self.absttl = true;
        self
    }

    /// Set the idle time for LRU eviction (Redis 5.0+)
    pub fn idletime(mut self, seconds: i64) -> Self {
        self.idletime = Some(seconds);
        self
    }

    /// Set the frequency counter for LFU eviction (Redis 5.0+)
    pub fn freq(mut self, frequency: i64) -> Self {
        self.freq = Some(frequency);
        self
    }
}

impl Command for Restore {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("RESTORE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.ttl.to_string()))),
            Frame::BulkString(Some(self.serialized_value.clone())),
        ];

        if self.replace {
            frames.push(Frame::BulkString(Some(Bytes::from("REPLACE"))));
        }

        if self.absttl {
            frames.push(Frame::BulkString(Some(Bytes::from("ABSTTL"))));
        }

        if let Some(idletime) = self.idletime {
            frames.push(Frame::BulkString(Some(Bytes::from("IDLETIME"))));
            frames.push(Frame::BulkString(Some(Bytes::from(idletime.to_string()))));
        }

        if let Some(freq) = self.freq {
            frames.push(Frame::BulkString(Some(Bytes::from("FREQ"))));
            frames.push(Frame::BulkString(Some(Bytes::from(freq.to_string()))));
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

// ReadOnly trait implementations
use crate::read_preference::ReadOnly;

impl ReadOnly for ExpireTime {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for PExpireTime {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Dump {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands (default is_read_only = false)
impl ReadOnly for Touch {}
impl ReadOnly for Unlink {}
impl ReadOnly for Copy {}
impl ReadOnly for Move {}
impl ReadOnly for Restore {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_persist_frame() {
        let cmd = Persist::new("mykey");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 2);
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("PERSIST")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_persist_response() {
        assert!(Persist::parse_response(Frame::Integer(1)).unwrap());
        assert!(!Persist::parse_response(Frame::Integer(0)).unwrap());
    }

    #[test]
    fn test_pexpire_frame() {
        let cmd = PExpire::new("mykey", 5000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3);
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("PEXPIRE")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_pttl_frame() {
        let cmd = PTtl::new("mykey");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 2);
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("PTTL")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_pttl_response() {
        assert_eq!(PTtl::parse_response(Frame::Integer(5000)).unwrap(), 5000);
        assert_eq!(PTtl::parse_response(Frame::Integer(-1)).unwrap(), -1);
        assert_eq!(PTtl::parse_response(Frame::Integer(-2)).unwrap(), -2);
    }

    #[test]
    fn test_expireat_frame() {
        let cmd = ExpireAt::new("mykey", 1672531200);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3);
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("EXPIREAT")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_pexpireat_frame() {
        let cmd = PExpireAt::new("mykey", 1672531200000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3);
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("PEXPIREAT")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_rename_frame() {
        let cmd = Rename::new("oldkey", "newkey");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3);
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("RENAME")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_rename_response() {
        Rename::parse_response(Frame::SimpleString(Bytes::from("OK"))).unwrap();
    }

    #[test]
    fn test_renamenx_frame() {
        let cmd = RenameNx::new("oldkey", "newkey");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3);
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("RENAMENX")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_renamenx_response() {
        assert!(RenameNx::parse_response(Frame::Integer(1)).unwrap());
        assert!(!RenameNx::parse_response(Frame::Integer(0)).unwrap());
    }

    #[test]
    fn test_type_frame() {
        let cmd = Type::new("mykey");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 2);
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("TYPE")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_type_response() {
        let response = Type::parse_response(Frame::SimpleString(Bytes::from("string"))).unwrap();
        assert_eq!(response, "string");

        let response = Type::parse_response(Frame::SimpleString(Bytes::from("list"))).unwrap();
        assert_eq!(response, "list");

        let response = Type::parse_response(Frame::SimpleString(Bytes::from("none"))).unwrap();
        assert_eq!(response, "none");
    }

    #[test]
    fn test_keys_frame() {
        let cmd = Keys::new("user:*");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 2);
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("KEYS")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_keys_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("key1"))),
            Frame::BulkString(Some(Bytes::from("key2"))),
            Frame::BulkString(Some(Bytes::from("key3"))),
        ]);

        let keys = Keys::parse_response(frame).unwrap();
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], "key1");
        assert_eq!(keys[1], "key2");
        assert_eq!(keys[2], "key3");
    }

    #[test]
    fn test_keys_response_empty() {
        let frame = Frame::Array(vec![]);
        let keys = Keys::parse_response(frame).unwrap();
        assert_eq!(keys.len(), 0);
    }

    #[test]
    fn test_touch_single_frame() {
        let cmd = Touch::single("mykey");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 2);
                assert_eq!(args[0], Frame::BulkString(Some(Bytes::from("TOUCH"))));
                assert_eq!(args[1], Frame::BulkString(Some(Bytes::from("mykey"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_touch_multiple_frame() {
        let cmd = Touch::new(vec!["key1".to_string(), "key2".to_string()]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3);
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_touch_response() {
        let frame = Frame::Integer(2);
        let count = Touch::parse_response(frame).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_unlink_frame() {
        let cmd = Unlink::new(vec!["key1".to_string(), "key2".to_string()]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3);
                assert_eq!(args[0], Frame::BulkString(Some(Bytes::from("UNLINK"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_unlink_response() {
        let frame = Frame::Integer(1);
        let count = Unlink::parse_response(frame).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_copy_simple_frame() {
        let cmd = Copy::new("source", "dest");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3);
                assert_eq!(args[0], Frame::BulkString(Some(Bytes::from("COPY"))));
                assert_eq!(args[1], Frame::BulkString(Some(Bytes::from("source"))));
                assert_eq!(args[2], Frame::BulkString(Some(Bytes::from("dest"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_copy_with_replace_frame() {
        let cmd = Copy::new("source", "dest").replace();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 4);
                assert_eq!(args[3], Frame::BulkString(Some(Bytes::from("REPLACE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_copy_with_db_frame() {
        let cmd = Copy::new("source", "dest").db(2);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 5);
                assert_eq!(args[3], Frame::BulkString(Some(Bytes::from("DB"))));
                assert_eq!(args[4], Frame::BulkString(Some(Bytes::from("2"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_copy_response_success() {
        let frame = Frame::Integer(1);
        let result = Copy::parse_response(frame).unwrap();
        assert!(result);
    }

    #[test]
    fn test_copy_response_failure() {
        let frame = Frame::Integer(0);
        let result = Copy::parse_response(frame).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_move_frame() {
        let cmd = Move::new("mykey", 2);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3);
                assert_eq!(args[0], Frame::BulkString(Some(Bytes::from("MOVE"))));
                assert_eq!(args[1], Frame::BulkString(Some(Bytes::from("mykey"))));
                assert_eq!(args[2], Frame::BulkString(Some(Bytes::from("2"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_move_response() {
        let frame = Frame::Integer(1);
        let result = Move::parse_response(frame).unwrap();
        assert!(result);
    }

    #[test]
    fn test_expiretime_frame() {
        let cmd = ExpireTime::new("mykey");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 2);
                assert_eq!(args[0], Frame::BulkString(Some(Bytes::from("EXPIRETIME"))));
                assert_eq!(args[1], Frame::BulkString(Some(Bytes::from("mykey"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_expiretime_response() {
        let frame = Frame::Integer(1735689600); // Some future timestamp
        let timestamp = ExpireTime::parse_response(frame).unwrap();
        assert_eq!(timestamp, 1735689600);
    }

    #[test]
    fn test_expiretime_response_no_expiry() {
        let frame = Frame::Integer(-1);
        let timestamp = ExpireTime::parse_response(frame).unwrap();
        assert_eq!(timestamp, -1);
    }

    #[test]
    fn test_pexpiretime_frame() {
        let cmd = PExpireTime::new("mykey");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 2);
                assert_eq!(args[0], Frame::BulkString(Some(Bytes::from("PEXPIRETIME"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_pexpiretime_response() {
        let frame = Frame::Integer(1735689600000); // Some future timestamp in ms
        let timestamp = PExpireTime::parse_response(frame).unwrap();
        assert_eq!(timestamp, 1735689600000);
    }

    #[test]
    fn test_object_refcount_frame() {
        let cmd = ObjectRefCount::new("mykey");
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3); // OBJECT REFCOUNT key
            }
            _ => panic!("Expected array frame"),
        }
    }

    #[test]
    fn test_object_encoding_frame() {
        let cmd = ObjectEncoding::new("mykey");
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3); // OBJECT ENCODING key
            }
            _ => panic!("Expected array frame"),
        }
    }
}

/// OBJECT REFCOUNT - get object reference count
#[derive(Debug, Clone)]
pub struct ObjectRefCount {
    pub(crate) key: String,
}

impl ObjectRefCount {
    /// Create a new OBJECT REFCOUNT command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ObjectRefCount {
    type Response = Option<i64>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("OBJECT"))),
            Frame::BulkString(Some(Bytes::from("REFCOUNT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
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

/// OBJECT ENCODING - get object encoding
#[derive(Debug, Clone)]
pub struct ObjectEncoding {
    pub(crate) key: String,
}

impl ObjectEncoding {
    /// Create a new OBJECT ENCODING command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ObjectEncoding {
    type Response = Option<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("OBJECT"))),
            Frame::BulkString(Some(Bytes::from("ENCODING"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(String::from_utf8_lossy(&data).to_string())),
            Frame::Null | Frame::BulkString(None) => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// OBJECT IDLETIME - get object idle time in seconds
#[derive(Debug, Clone)]
pub struct ObjectIdleTime {
    pub(crate) key: String,
}

impl ObjectIdleTime {
    /// Create a new OBJECT IDLETIME command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ObjectIdleTime {
    type Response = Option<i64>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("OBJECT"))),
            Frame::BulkString(Some(Bytes::from("IDLETIME"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
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

/// OBJECT FREQ - get object access frequency
#[derive(Debug, Clone)]
pub struct ObjectFreq {
    pub(crate) key: String,
}

impl ObjectFreq {
    /// Create a new OBJECT FREQ command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ObjectFreq {
    type Response = Option<i64>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("OBJECT"))),
            Frame::BulkString(Some(Bytes::from("FREQ"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
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

/// Sort order for SORT command
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    /// Ascending order (default)
    Asc,
    /// Descending order
    Desc,
}

/// SORT - Sort the elements in a list, set or sorted set
///
/// Returns or stores the sorted elements of the list, set or sorted set stored at key.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::{Sort, SortOrder};
///
/// // Basic sort
/// let cmd = Sort::new("mylist");
///
/// // Sort with options
/// let cmd = Sort::new("mylist")
///     .by("weight_*")
///     .limit(0, 10)
///     .get("object_*")
///     .order(SortOrder::Desc)
///     .alpha();
///
/// // Sort and store result
/// let cmd = Sort::new("mylist").store("result");
/// ```
#[derive(Debug, Clone)]
pub struct Sort {
    key: String,
    by: Option<String>,
    limit: Option<(i64, i64)>,
    get: Vec<String>,
    order: Option<SortOrder>,
    alpha: bool,
    store: Option<String>,
}

impl Sort {
    /// Create a new SORT command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            by: None,
            limit: None,
            get: Vec::new(),
            order: None,
            alpha: false,
            store: None,
        }
    }

    /// Use external key for sorting
    pub fn by(mut self, pattern: impl Into<String>) -> Self {
        self.by = Some(pattern.into());
        self
    }

    /// Limit results to offset and count
    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.limit = Some((offset, count));
        self
    }

    /// Get external keys for the sorted elements
    pub fn get(mut self, pattern: impl Into<String>) -> Self {
        self.get.push(pattern.into());
        self
    }

    /// Set sort order
    pub fn order(mut self, order: SortOrder) -> Self {
        self.order = Some(order);
        self
    }

    /// Sort lexicographically instead of numerically
    pub fn alpha(mut self) -> Self {
        self.alpha = true;
        self
    }

    /// Store result in destination key instead of returning it
    pub fn store(mut self, destination: impl Into<String>) -> Self {
        self.store = Some(destination.into());
        self
    }
}

impl Command for Sort {
    type Response = SortResult;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![
            Frame::BulkString(Some(Bytes::from("SORT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref by) = self.by {
            parts.push(Frame::BulkString(Some(Bytes::from("BY"))));
            parts.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                by.as_bytes(),
            ))));
        }

        if let Some((offset, count)) = self.limit {
            parts.push(Frame::BulkString(Some(Bytes::from("LIMIT"))));
            parts.push(Frame::BulkString(Some(Bytes::from(offset.to_string()))));
            parts.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        for pattern in &self.get {
            parts.push(Frame::BulkString(Some(Bytes::from("GET"))));
            parts.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                pattern.as_bytes(),
            ))));
        }

        if let Some(order) = self.order {
            let order_str = match order {
                SortOrder::Asc => "ASC",
                SortOrder::Desc => "DESC",
            };
            parts.push(Frame::BulkString(Some(Bytes::from(order_str))));
        }

        if self.alpha {
            parts.push(Frame::BulkString(Some(Bytes::from("ALPHA"))));
        }

        if let Some(ref dest) = self.store {
            parts.push(Frame::BulkString(Some(Bytes::from("STORE"))));
            parts.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                dest.as_bytes(),
            ))));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let elements = items
                    .into_iter()
                    .map(|f| match f {
                        Frame::BulkString(Some(b)) => Ok(b),
                        Frame::BulkString(None) => Ok(Bytes::new()),
                        _ => Err(RedisError::UnexpectedResponse),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(SortResult::Elements(elements))
            }
            Frame::Integer(n) => Ok(SortResult::Stored(n)), // When using STORE
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Result of SORT command
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortResult {
    /// Sorted elements (when not using STORE)
    Elements(Vec<Bytes>),
    /// Number of elements stored (when using STORE)
    Stored(i64),
}

#[cfg(test)]
mod sort_tests {
    use super::*;

    #[test]
    fn test_sort_basic() {
        let cmd = Sort::new("mylist");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 2);
                assert_eq!(args[0], Frame::BulkString(Some(Bytes::from("SORT"))));
                assert_eq!(args[1], Frame::BulkString(Some(Bytes::from("mylist"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_sort_with_all_options() {
        let cmd = Sort::new("mylist")
            .by("weight_*")
            .limit(0, 10)
            .get("object_*")
            .get("value_*")
            .order(SortOrder::Desc)
            .alpha();

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert!(args.contains(&Frame::BulkString(Some(Bytes::from("BY")))));
                assert!(args.contains(&Frame::BulkString(Some(Bytes::from("weight_*")))));
                assert!(args.contains(&Frame::BulkString(Some(Bytes::from("LIMIT")))));
                assert!(args.contains(&Frame::BulkString(Some(Bytes::from("0")))));
                assert!(args.contains(&Frame::BulkString(Some(Bytes::from("10")))));
                assert!(args.contains(&Frame::BulkString(Some(Bytes::from("GET")))));
                assert!(args.contains(&Frame::BulkString(Some(Bytes::from("object_*")))));
                assert!(args.contains(&Frame::BulkString(Some(Bytes::from("value_*")))));
                assert!(args.contains(&Frame::BulkString(Some(Bytes::from("DESC")))));
                assert!(args.contains(&Frame::BulkString(Some(Bytes::from("ALPHA")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_sort_with_store() {
        let cmd = Sort::new("mylist").store("result");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert!(args.contains(&Frame::BulkString(Some(Bytes::from("STORE")))));
                assert!(args.contains(&Frame::BulkString(Some(Bytes::from("result")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_sort_order_asc() {
        let cmd = Sort::new("mylist").order(SortOrder::Asc);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert!(args.contains(&Frame::BulkString(Some(Bytes::from("ASC")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_sort_parse_elements() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::BulkString(Some(Bytes::from("b"))),
            Frame::BulkString(Some(Bytes::from("c"))),
        ]);

        let result = Sort::parse_response(frame).unwrap();
        match result {
            SortResult::Elements(elements) => {
                assert_eq!(elements.len(), 3);
                assert_eq!(elements[0], Bytes::from("a"));
                assert_eq!(elements[1], Bytes::from("b"));
                assert_eq!(elements[2], Bytes::from("c"));
            }
            _ => panic!("Expected Elements variant"),
        }
    }

    #[test]
    fn test_sort_parse_stored() {
        let frame = Frame::Integer(42);
        let result = Sort::parse_response(frame).unwrap();
        match result {
            SortResult::Stored(n) => assert_eq!(n, 42),
            _ => panic!("Expected Stored variant"),
        }
    }

    #[test]
    fn test_sort_parse_error() {
        let frame = Frame::Error(Bytes::from("ERR syntax error"));
        assert!(Sort::parse_response(frame).is_err());
    }
}
