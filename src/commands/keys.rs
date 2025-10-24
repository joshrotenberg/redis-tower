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
}
