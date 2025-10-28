//! Key management commands for Redis
//!
//! Commands for managing key lifetimes, renaming, and introspection.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// PERSIST command - Remove the expiration from a key
///
/// Remove the existing timeout on key, turning the key from volatile (a key with an expire set)
/// to persistent (a key that will never expire as no timeout is associated).
///
/// # Request
/// - `key`: The key to persist
///
/// # Response
/// Returns `bool`:
/// - `true` - The timeout was removed successfully
/// - `false` - The key does not exist or does not have an associated timeout
///
/// # Redis Version
/// Available since Redis 2.2.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::keys::Persist;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let cmd = Persist::new("mykey");
/// let removed = client.call(cmd).await?;
///
/// if removed {
///     println!("Expiration removed, key is now persistent");
/// } else {
///     println!("Key has no expiration or doesn't exist");
/// }
/// # Ok(())
/// # }
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

#[cfg(test)]
mod new_keys_tests {
    use super::*;

    #[test]
    fn test_scan_basic_frame() {
        let cmd = Scan::new(0);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("SCAN"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("0"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_scan_with_pattern_frame() {
        let cmd = Scan::new(10).pattern("user:*");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("MATCH"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("user:*"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_scan_with_count_frame() {
        let cmd = Scan::new(0).count(100);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("COUNT"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("100"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_scan_with_type_frame() {
        let cmd = Scan::new(0).key_type("string");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("TYPE"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("string"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_scan_parse_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("10"))),
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("key1"))),
                Frame::BulkString(Some(Bytes::from("key2"))),
            ]),
        ]);

        let (cursor, keys) = Scan::parse_response(frame).unwrap();
        assert_eq!(cursor, 10);
        assert_eq!(keys, vec!["key1", "key2"]);
    }

    #[test]
    fn test_migrate_single_key_frame() {
        let cmd = Migrate::new("127.0.0.1", 6380, "mykey", 0, 5000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MIGRATE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("127.0.0.1"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("6380"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("mykey"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("0"))));
                assert_eq!(parts[5], Frame::BulkString(Some(Bytes::from("5000"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_migrate_multiple_keys_frame() {
        let cmd = Migrate::multiple("127.0.0.1", 6380, 0, 5000, vec!["key1", "key2"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from(""))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("KEYS")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("key1")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("key2")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_migrate_with_copy_frame() {
        let cmd = Migrate::new("127.0.0.1", 6380, "mykey", 0, 5000).copy();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("COPY")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_migrate_with_auth_frame() {
        let cmd = Migrate::new("127.0.0.1", 6380, "mykey", 0, 5000).auth("password");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("AUTH")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("password")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_waitaof_frame() {
        let cmd = WaitAof::new(1, 2, 1000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("WAITAOF"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("1"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("2"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("1000"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_waitaof_parse_response() {
        let frame = Frame::Array(vec![Frame::Integer(1), Frame::Integer(2)]);
        let (local, replica) = WaitAof::parse_response(frame).unwrap();
        assert_eq!(local, 1);
        assert_eq!(replica, 2);
    }

    #[test]
    fn test_sort_ro_basic_frame() {
        let cmd = SortRo::new("mylist");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("SORT_RO"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("mylist"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_sort_ro_with_by_frame() {
        let cmd = SortRo::new("mylist").by("weight_*");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("BY")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("weight_*")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_sort_ro_with_limit_frame() {
        let cmd = SortRo::new("mylist").limit(0, 10);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("LIMIT")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("0")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("10")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_sort_ro_alpha_frame() {
        let cmd = SortRo::new("mylist").alpha();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("ALPHA")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_restore_asking_frame() {
        let cmd = RestoreAsking::new("mykey", 0, Bytes::from(vec![1, 2, 3]));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("RESTORE-ASKING")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("mykey"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("0"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_restore_asking_with_replace_frame() {
        let cmd = RestoreAsking::new("mykey", 0, Bytes::from(vec![1, 2, 3])).replace();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("REPLACE")))));
            }
            _ => panic!("Expected Array frame"),
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
/// Renames key to newkey. It returns an error when key does not exist. If newkey already exists
/// it is overwritten. Before Redis 3.2.0, an error was returned if source and destination names
/// are the same. Starting with Redis 3.2.0, RENAME does nothing if names are identical.
///
/// # Request
/// - `key`: The source key to rename
/// - `new_key`: The destination key name
///
/// # Response
/// Returns `()` - Command always succeeds if key exists.
///
/// # Redis Version
/// Available since Redis 1.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::keys::Rename;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let cmd = Rename::new("oldkey", "newkey");
/// client.call(cmd).await?;
/// println!("Key renamed successfully");
/// # Ok(())
/// # }
/// ```
///
/// **Note:** If newkey already exists, it is overwritten.
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

/// TYPE command - Determine the type stored at key
///
/// Returns the string representation of the type of the value stored at key. The different
/// types that can be returned are: string, list, set, zset, hash, and stream.
///
/// # Request
/// - `key`: The key to check
///
/// # Response
/// Returns `String` - The type of the key:
/// - `"string"` - String value
/// - `"list"` - List value
/// - `"set"` - Set value
/// - `"zset"` - Sorted set value
/// - `"hash"` - Hash value
/// - `"stream"` - Stream value
/// - `"none"` - Key does not exist
///
/// # Redis Version
/// Available since Redis 1.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::keys::Type;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let cmd = Type::new("mykey");
/// let key_type = client.call(cmd).await?;
///
/// match key_type.as_str() {
///     "string" => println!("It's a string"),
///     "list" => println!("It's a list"),
///     "none" => println!("Key doesn't exist"),
///     _ => println!("Type: {}", key_type),
/// }
/// # Ok(())
/// # }
/// ```
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
/// Returns all keys matching pattern. While the time complexity for this operation is O(N),
/// the constant times are fairly low. For example, Redis running on an entry level laptop can
/// scan a 1 million key database in 40 milliseconds.
///
/// **Warning:** Consider KEYS as a command that should only be used in production environments
/// with extreme care. It may ruin performance when it is executed against large databases.
/// This command is intended for debugging and special operations. Use SCAN for production.
///
/// # Request
/// - `pattern`: Glob-style pattern (* matches any characters, ? matches one character, [abc] matches a, b, or c)
///
/// # Response
/// Returns `Vec<String>` - All keys matching the pattern. Empty vector if no keys match.
///
/// # Redis Version
/// Available since Redis 1.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::keys::Keys;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Get all keys (use carefully!)
/// let cmd = Keys::new("*");
/// let all_keys = client.call(cmd).await?;
///
/// // Get keys with prefix
/// let cmd = Keys::new("user:*");
/// let user_keys = client.call(cmd).await?;
///
/// // Get keys matching pattern
/// let cmd = Keys::new("user:[0-9]*");
/// let numbered_users = client.call(cmd).await?;
/// println!("Found {} matching keys", numbered_users.len());
/// # Ok(())
/// # }
/// ```
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

/// TOUCH command - Alters the last access time of a key(s)
///
/// Returns the number of existing keys specified. A key is ignored if it does not exist.
/// This command is useful to update the LRU (Least Recently Used) information for keys,
/// which affects their eviction when maxmemory-policy is set to an LRU-based policy.
///
/// # Request
/// - `keys`: One or more keys to touch
///
/// # Response
/// Returns `i64` - The number of keys that exist and were touched.
///
/// # Redis Version
/// Available since Redis 3.2.1
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::keys::Touch;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Touch single key
/// let cmd = Touch::single("mykey");
/// let touched = client.call(cmd).await?;
///
/// // Touch multiple keys
/// let cmd = Touch::new(vec!["key1".to_string(), "key2".to_string(), "key3".to_string()]);
/// let touched = client.call(cmd).await?;
/// println!("Touched {} keys", touched);
/// # Ok(())
/// # }
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

/// UNLINK command - Delete a key asynchronously in another thread
///
/// This command is very similar to DEL: it removes the specified keys. Just like DEL a key
/// is ignored if it does not exist. However, UNLINK performs the actual memory reclamation
/// in a different thread, so it is not blocking. This is more efficient when deleting large
/// values.
///
/// # Request
/// - `keys`: One or more keys to unlink
///
/// # Response
/// Returns `i64` - The number of keys that were unlinked.
///
/// # Redis Version
/// Available since Redis 4.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::keys::Unlink;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Unlink single key
/// let cmd = Unlink::single("large_key");
/// let unlinked = client.call(cmd).await?;
///
/// // Unlink multiple keys asynchronously
/// let cmd = Unlink::new(vec!["key1".to_string(), "key2".to_string()]);
/// let unlinked = client.call(cmd).await?;
/// println!("Unlinked {} keys", unlinked);
/// # Ok(())
/// # }
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

/// COPY command - Copy the value of a key to a new key
///
/// This command copies the value stored at the source key to the destination key. By default,
/// the destination key is created in the logical database used by the connection. The DB option
/// allows specifying an alternative logical database index for the destination key.
///
/// # Request
/// - `source`: The source key to copy from
/// - `destination`: The destination key to copy to
/// - `db` (optional): Database index to copy to
/// - `replace` (optional): Overwrite destination if it exists
///
/// # Response
/// Returns `bool`:
/// - `true` - The source key was copied successfully
/// - `false` - The source key does not exist, or destination exists and REPLACE was not used
///
/// # Redis Version
/// Available since Redis 6.2.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::keys::Copy;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Simple copy
/// let cmd = Copy::new("source", "dest");
/// let copied = client.call(cmd).await?;
///
/// // Copy with REPLACE option (overwrite if dest exists)
/// let cmd = Copy::new("source", "dest").replace();
/// let copied = client.call(cmd).await?;
///
/// // Copy to different database
/// let cmd = Copy::new("source", "dest").db(2);
/// let copied = client.call(cmd).await?;
/// println!("Copy successful: {}", copied);
/// # Ok(())
/// # }
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

/// DUMP command - Return a serialized version of the value stored at a key
///
/// Serialize the value stored at key in a Redis-specific format and return it to the user.
/// The returned value can be synthesized back into a Redis key using the RESTORE command.
/// The serialization format is opaque and non-standard, however it has a few semantic characteristics.
///
/// # Request
/// - `key`: The key to serialize
///
/// # Response
/// Returns `Option<Bytes>`:
/// - `Some(data)` - The serialized value
/// - `None` - The key does not exist
///
/// # Redis Version
/// Available since Redis 2.6.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::keys::Dump;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let cmd = Dump::new("mykey");
/// let serialized = client.call(cmd).await?;
///
/// if let Some(data) = serialized {
///     println!("Serialized {} bytes", data.len());
///     // Can be restored with RESTORE command
/// } else {
///     println!("Key does not exist");
/// }
/// # Ok(())
/// # }
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

/// RESTORE command - Create a key using the provided serialized value
///
/// Create a key associated with a value that is obtained by deserializing the provided
/// serialized value (obtained via DUMP). If ttl is 0 the key is created without any expire,
/// otherwise the specified expire time (in milliseconds) is set.
///
/// # Request
/// - `key`: The key to create
/// - `ttl`: Time to live in milliseconds (0 for no expiry)
/// - `serialized_value`: Serialized value from DUMP command
/// - `replace` (optional): Replace existing key if it exists
/// - `absttl` (optional): TTL represents absolute Unix timestamp in milliseconds
/// - `idletime` (optional): Set the idle time for LRU eviction (seconds)
/// - `freq` (optional): Set the frequency counter for LFU eviction
///
/// # Response
/// Returns `String` - "OK" on success
///
/// # Redis Version
/// Available since Redis 2.6.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::keys::Restore;
/// use redis_tower::RedisClient;
/// use bytes::Bytes;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// # let serialized_data = Bytes::from(vec![1, 2, 3]);
/// // Restore without TTL
/// let cmd = Restore::new("mykey", 0, serialized_data.clone());
/// client.call(cmd).await?;
///
/// // Restore with 10 second TTL
/// let cmd = Restore::new("mykey2", 10000, serialized_data.clone());
/// client.call(cmd).await?;
///
/// // Restore with REPLACE option (overwrite if exists)
/// let cmd = Restore::new("mykey", 0, serialized_data.clone()).replace();
/// client.call(cmd).await?;
/// # Ok(())
/// # }
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

/// SCAN command - Incrementally iterate over the keys space
///
/// SCAN is a cursor-based iterator that allows incrementally iterating over the entire key space
/// without blocking the server. Unlike KEYS, SCAN doesn't block the server and is safe to use in
/// production environments. SCAN guarantees to return all the elements that are present from the
/// start to the end of the iteration (assuming no modifications).
///
/// # Request
/// - `cursor`: Cursor position (0 to start, use returned cursor for next iteration)
/// - `pattern` (optional): Glob-style pattern to filter keys
/// - `count` (optional): Hint for number of elements to return per call
/// - `key_type` (optional): Filter by key type (string, list, set, zset, hash, stream)
///
/// # Response
/// Returns `(u64, Vec<String>)`:
/// - First element: Next cursor position (0 means iteration complete)
/// - Second element: Vector of keys found in this iteration
///
/// # Redis Version
/// Available since Redis 2.8.0. TYPE filter available since Redis 6.0.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::keys::Scan;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Iterate over all keys
/// let mut cursor = 0;
/// loop {
///     let cmd = Scan::new(cursor);
///     let (next_cursor, keys) = client.call(cmd).await?;
///
///     for key in keys {
///         println!("Key: {}", key);
///     }
///
///     if next_cursor == 0 {
///         break; // Iteration complete
///     }
///     cursor = next_cursor;
/// }
///
/// // Scan with pattern matching
/// let cmd = Scan::new(0).pattern("user:*");
/// let (cursor, user_keys) = client.call(cmd).await?;
///
/// // Scan with count hint and type filter
/// let cmd = Scan::new(0).count(100).key_type("string");
/// let (cursor, string_keys) = client.call(cmd).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Scan {
    cursor: u64,
    pattern: Option<String>,
    count: Option<i64>,
    key_type: Option<String>,
}

impl Scan {
    /// Create a new SCAN command with cursor position
    pub fn new(cursor: u64) -> Self {
        Self {
            cursor,
            pattern: None,
            count: None,
            key_type: None,
        }
    }

    /// Set pattern to match keys against
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Set count hint for number of elements to return per iteration
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Filter by key type (Redis 6.0+)
    pub fn key_type(mut self, key_type: impl Into<String>) -> Self {
        self.key_type = Some(key_type.into());
        self
    }
}

impl Command for Scan {
    type Response = (u64, Vec<String>); // (next_cursor, keys)

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("SCAN"))),
            Frame::BulkString(Some(Bytes::from(self.cursor.to_string()))),
        ];

        if let Some(pattern) = &self.pattern {
            frames.push(Frame::BulkString(Some(Bytes::from("MATCH"))));
            frames.push(Frame::BulkString(Some(Bytes::from(pattern.clone()))));
        }

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        if let Some(key_type) = &self.key_type {
            frames.push(Frame::BulkString(Some(Bytes::from("TYPE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(key_type.clone()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(mut parts) if parts.len() == 2 => {
                let keys_frame = parts.pop().unwrap();
                let cursor_frame = parts.pop().unwrap();

                let cursor = match cursor_frame {
                    Frame::BulkString(Some(data)) => String::from_utf8_lossy(&data)
                        .parse::<u64>()
                        .map_err(|_| RedisError::UnexpectedResponse)?,
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                let keys = match keys_frame {
                    Frame::Array(key_frames) => {
                        let mut keys = Vec::new();
                        for key_frame in key_frames {
                            match key_frame {
                                Frame::BulkString(Some(data)) => {
                                    keys.push(String::from_utf8_lossy(&data).into_owned());
                                }
                                _ => return Err(RedisError::UnexpectedResponse),
                            }
                        }
                        keys
                    }
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                Ok((cursor, keys))
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// MIGRATE command - Atomically transfer key(s) to another Redis instance
///
/// Transfers one or more keys from the source instance to the destination
/// instance. On success, keys are deleted from the source.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Migrate;
///
/// // Migrate single key
/// let cmd = Migrate::new("127.0.0.1", 6380, "mykey", 0, 5000);
///
/// // Migrate with COPY (don't delete from source)
/// let cmd = Migrate::new("127.0.0.1", 6380, "mykey", 0, 5000).copy();
///
/// // Migrate with REPLACE (replace existing key)
/// let cmd = Migrate::new("127.0.0.1", 6380, "mykey", 0, 5000).replace();
///
/// // Migrate multiple keys (Redis 3.0.6+)
/// let cmd = Migrate::multiple("127.0.0.1", 6380, 0, 5000, vec!["key1", "key2"]);
///
/// // Migrate with authentication
/// let cmd = Migrate::new("127.0.0.1", 6380, "mykey", 0, 5000).auth("password");
/// ```
#[derive(Debug, Clone)]
pub struct Migrate {
    host: String,
    port: u16,
    key: Option<String>,
    keys: Vec<String>,
    destination_db: i64,
    timeout: i64,
    copy: bool,
    replace: bool,
    auth: Option<String>,
    auth2: Option<(String, String)>,
}

impl Migrate {
    /// Create a new MIGRATE command for a single key
    pub fn new(
        host: impl Into<String>,
        port: u16,
        key: impl Into<String>,
        destination_db: i64,
        timeout: i64,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            key: Some(key.into()),
            keys: Vec::new(),
            destination_db,
            timeout,
            copy: false,
            replace: false,
            auth: None,
            auth2: None,
        }
    }

    /// Create a MIGRATE command for multiple keys (Redis 3.0.6+)
    pub fn multiple(
        host: impl Into<String>,
        port: u16,
        destination_db: i64,
        timeout: i64,
        keys: Vec<impl Into<String>>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            key: None,
            keys: keys.into_iter().map(|k| k.into()).collect(),
            destination_db,
            timeout,
            copy: false,
            replace: false,
            auth: None,
            auth2: None,
        }
    }

    /// Don't remove the key from the source instance (COPY option)
    pub fn copy(mut self) -> Self {
        self.copy = true;
        self
    }

    /// Replace existing key on destination (REPLACE option)
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }

    /// Authenticate with password
    pub fn auth(mut self, password: impl Into<String>) -> Self {
        self.auth = Some(password.into());
        self
    }

    /// Authenticate with username and password (Redis 6.0+)
    pub fn auth2(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth2 = Some((username.into(), password.into()));
        self
    }
}

impl Command for Migrate {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("MIGRATE"))),
            Frame::BulkString(Some(Bytes::from(self.host.clone()))),
            Frame::BulkString(Some(Bytes::from(self.port.to_string()))),
        ];

        // Add key or empty string for multi-key mode
        if let Some(key) = &self.key {
            frames.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
        } else {
            frames.push(Frame::BulkString(Some(Bytes::from(""))));
        }

        frames.push(Frame::BulkString(Some(Bytes::from(
            self.destination_db.to_string(),
        ))));
        frames.push(Frame::BulkString(Some(Bytes::from(
            self.timeout.to_string(),
        ))));

        if self.copy {
            frames.push(Frame::BulkString(Some(Bytes::from("COPY"))));
        }

        if self.replace {
            frames.push(Frame::BulkString(Some(Bytes::from("REPLACE"))));
        }

        if let Some(password) = &self.auth {
            frames.push(Frame::BulkString(Some(Bytes::from("AUTH"))));
            frames.push(Frame::BulkString(Some(Bytes::from(password.clone()))));
        }

        if let Some((username, password)) = &self.auth2 {
            frames.push(Frame::BulkString(Some(Bytes::from("AUTH2"))));
            frames.push(Frame::BulkString(Some(Bytes::from(username.clone()))));
            frames.push(Frame::BulkString(Some(Bytes::from(password.clone()))));
        }

        if !self.keys.is_empty() {
            frames.push(Frame::BulkString(Some(Bytes::from("KEYS"))));
            for key in &self.keys {
                frames.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
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

/// WAITAOF command - Wait for AOF fsync acknowledgment
///
/// Blocks until all previous write commands are fsynced to the AOF
/// of the local Redis and/or at least the specified number of replicas.
///
/// Available since Redis 7.2.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::WaitAof;
///
/// // Wait for local fsync only
/// let cmd = WaitAof::new(1, 0, 1000);
///
/// // Wait for 2 replicas, no local fsync required
/// let cmd = WaitAof::new(0, 2, 1000);
///
/// // Wait for both local and 1 replica
/// let cmd = WaitAof::new(1, 1, 5000);
/// ```
#[derive(Debug, Clone)]
pub struct WaitAof {
    numlocal: i64,
    numreplicas: i64,
    timeout: i64,
}

impl WaitAof {
    /// Create a new WAITAOF command
    ///
    /// # Arguments
    /// * `numlocal` - Number of local fsyncs (0 or 1)
    /// * `numreplicas` - Minimum number of replicas to reach
    /// * `timeout` - Timeout in milliseconds
    pub fn new(numlocal: i64, numreplicas: i64, timeout: i64) -> Self {
        Self {
            numlocal,
            numreplicas,
            timeout,
        }
    }
}

impl Command for WaitAof {
    type Response = (i64, i64); // (local_acks, replica_acks)

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("WAITAOF"))),
            Frame::BulkString(Some(Bytes::from(self.numlocal.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.numreplicas.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.timeout.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(mut parts) if parts.len() == 2 => {
                let replica_acks = parts.pop().unwrap();
                let local_acks = parts.pop().unwrap();

                match (local_acks, replica_acks) {
                    (Frame::Integer(l), Frame::Integer(r)) => Ok((l, r)),
                    _ => Err(RedisError::UnexpectedResponse),
                }
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SORT_RO command - Read-only variant of SORT
///
/// Returns sorted elements from a list, set, or sorted set.
/// This is exactly like SORT but refuses the STORE option and can
/// safely be used in read-only replicas.
///
/// Available since Redis 7.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::{SortRo, SortOrder};
///
/// // Basic sort
/// let cmd = SortRo::new("mylist");
///
/// // Sort with pattern
/// let cmd = SortRo::new("mylist").by("weight_*");
///
/// // Sort with limit
/// let cmd = SortRo::new("mylist").limit(0, 10);
///
/// // Sort with GET pattern
/// let cmd = SortRo::new("mylist").get("object_*");
///
/// // Sort descending
/// let cmd = SortRo::new("mylist").order(SortOrder::Desc);
///
/// // Sort alphabetically
/// let cmd = SortRo::new("mylist").alpha();
/// ```
#[derive(Debug, Clone)]
pub struct SortRo {
    key: String,
    by_pattern: Option<String>,
    limit: Option<(i64, i64)>,
    get_patterns: Vec<String>,
    order: Option<SortOrder>,
    alpha: bool,
}

impl SortRo {
    /// Create a new SORT_RO command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            by_pattern: None,
            limit: None,
            get_patterns: Vec::new(),
            order: None,
            alpha: false,
        }
    }

    /// Sort by external key pattern
    pub fn by(mut self, pattern: impl Into<String>) -> Self {
        self.by_pattern = Some(pattern.into());
        self
    }

    /// Limit results with offset and count
    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.limit = Some((offset, count));
        self
    }

    /// Get external keys using pattern
    pub fn get(mut self, pattern: impl Into<String>) -> Self {
        self.get_patterns.push(pattern.into());
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
}

impl Command for SortRo {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("SORT_RO"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(by_pattern) = &self.by_pattern {
            frames.push(Frame::BulkString(Some(Bytes::from("BY"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                by_pattern.as_bytes(),
            ))));
        }

        if let Some((offset, count)) = self.limit {
            frames.push(Frame::BulkString(Some(Bytes::from("LIMIT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(offset.to_string()))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        for pattern in &self.get_patterns {
            frames.push(Frame::BulkString(Some(Bytes::from("GET"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                pattern.as_bytes(),
            ))));
        }

        if let Some(order) = &self.order {
            let order_str = match order {
                SortOrder::Asc => "ASC",
                SortOrder::Desc => "DESC",
            };
            frames.push(Frame::BulkString(Some(Bytes::from(order_str))));
        }

        if self.alpha {
            frames.push(Frame::BulkString(Some(Bytes::from("ALPHA"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(elements) => {
                let mut result = Vec::new();
                for element in elements {
                    match element {
                        Frame::BulkString(Some(data)) => result.push(data),
                        Frame::BulkString(None) => result.push(Bytes::new()),
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// RESTORE-ASKING command - Internal command for cluster key migration
///
/// Like RESTORE but used during cluster resharding. This is an internal
/// command used by Redis Cluster and should not be used directly by clients.
///
/// Available since Redis 3.0.0
#[derive(Debug, Clone)]
pub struct RestoreAsking {
    key: String,
    ttl: i64,
    serialized_value: Bytes,
    replace: bool,
}

impl RestoreAsking {
    /// Create a new RESTORE-ASKING command
    pub fn new(key: impl Into<String>, ttl: i64, serialized_value: Bytes) -> Self {
        Self {
            key: key.into(),
            ttl,
            serialized_value,
            replace: false,
        }
    }

    /// Replace existing key if it exists
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }
}

impl Command for RestoreAsking {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("RESTORE-ASKING"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.ttl.to_string()))),
            Frame::BulkString(Some(self.serialized_value.clone())),
        ];

        if self.replace {
            frames.push(Frame::BulkString(Some(Bytes::from("REPLACE"))));
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
