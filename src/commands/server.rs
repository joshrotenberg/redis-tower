//! Server and administrative commands
//!
//! Commands for server management, information, and database operations.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// DBSIZE - Return the number of keys in the current database
///
/// # Example
/// ```no_run
/// use redis_tower::commands::DbSize;
///
/// let cmd = DbSize::new();
/// // Response: number of keys (i64)
/// ```
#[derive(Debug, Clone)]
pub struct DbSize;

impl DbSize {
    /// Create a new DBSIZE command
    pub fn new() -> Self {
        Self
    }
}

impl Default for DbSize {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for DbSize {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("DBSIZE")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// FLUSHDB - Delete all keys in the current database
///
/// # Warning
/// This is a destructive operation that cannot be undone!
///
/// # Example
/// ```no_run
/// use redis_tower::commands::FlushDb;
///
/// let cmd = FlushDb::new();
/// // Optionally use async mode
/// let cmd = FlushDb::new().async_mode();
/// ```
#[derive(Debug, Clone)]
pub struct FlushDb {
    async_mode: bool,
}

impl FlushDb {
    /// Create a new FLUSHDB command
    pub fn new() -> Self {
        Self { async_mode: false }
    }

    /// Use async mode (non-blocking)
    pub fn async_mode(mut self) -> Self {
        self.async_mode = true;
        self
    }
}

impl Default for FlushDb {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for FlushDb {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("FLUSHDB")))];

        if self.async_mode {
            frames.push(Frame::BulkString(Some(Bytes::from("ASYNC"))));
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

/// FLUSHALL - Delete all keys in all databases
///
/// # Warning
/// This is an extremely destructive operation that affects ALL databases!
///
/// # Example
/// ```no_run
/// use redis_tower::commands::FlushAll;
///
/// let cmd = FlushAll::new();
/// // Optionally use async mode
/// let cmd = FlushAll::new().async_mode();
/// ```
#[derive(Debug, Clone)]
pub struct FlushAll {
    async_mode: bool,
}

impl FlushAll {
    /// Create a new FLUSHALL command
    pub fn new() -> Self {
        Self { async_mode: false }
    }

    /// Use async mode (non-blocking)
    pub fn async_mode(mut self) -> Self {
        self.async_mode = true;
        self
    }
}

impl Default for FlushAll {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for FlushAll {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("FLUSHALL")))];

        if self.async_mode {
            frames.push(Frame::BulkString(Some(Bytes::from("ASYNC"))));
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

/// RANDOMKEY - Return a random key from the current database
///
/// Returns None if the database is empty.
///
/// # Example
/// ```no_run
/// use redis_tower::commands::RandomKey;
///
/// let cmd = RandomKey::new();
/// // Response: Option<String> - random key or None if database is empty
/// ```
#[derive(Debug, Clone)]
pub struct RandomKey;

impl RandomKey {
    /// Create a new RANDOMKEY command
    pub fn new() -> Self {
        Self
    }
}

impl Default for RandomKey {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for RandomKey {
    type Response = Option<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("RANDOMKEY")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(String::from_utf8_lossy(&data).to_string())),
            Frame::BulkString(None) | Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// TIME - Return the server time
///
/// Returns a tuple of (unix_timestamp_seconds, microseconds).
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Time;
///
/// let cmd = Time::new();
/// // Response: (i64, i64) - (seconds, microseconds)
/// ```
#[derive(Debug, Clone)]
pub struct Time;

impl Time {
    /// Create a new TIME command
    pub fn new() -> Self {
        Self
    }
}

impl Default for Time {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Time {
    type Response = (i64, i64);

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("TIME")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(mut items) if items.len() == 2 => {
                let micros = items.pop().unwrap();
                let secs = items.pop().unwrap();

                match (secs, micros) {
                    (Frame::BulkString(Some(s)), Frame::BulkString(Some(m))) => {
                        let seconds = String::from_utf8_lossy(&s)
                            .parse::<i64>()
                            .map_err(|_| RedisError::UnexpectedResponse)?;
                        let microseconds = String::from_utf8_lossy(&m)
                            .parse::<i64>()
                            .map_err(|_| RedisError::UnexpectedResponse)?;
                        Ok((seconds, microseconds))
                    }
                    _ => Err(RedisError::UnexpectedResponse),
                }
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// LASTSAVE - Get UNIX timestamp of last successful save to disk
///
/// Returns the UNIX timestamp of the last DB save executed with success.
///
/// # Example
/// ```no_run
/// use redis_tower::commands::LastSave;
///
/// let cmd = LastSave::new();
/// // Response: i64 - UNIX timestamp
/// ```
#[derive(Debug, Clone)]
pub struct LastSave;

impl LastSave {
    /// Create a new LASTSAVE command
    pub fn new() -> Self {
        Self
    }
}

impl Default for LastSave {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for LastSave {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("LASTSAVE")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// Read-only trait implementations
use crate::read_preference::ReadOnly;

impl ReadOnly for DbSize {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for RandomKey {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Time {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for LastSave {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands (destructive operations)
impl ReadOnly for FlushDb {}
impl ReadOnly for FlushAll {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dbsize_frame() {
        let cmd = DbSize::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("DBSIZE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_dbsize_response() {
        let frame = Frame::Integer(42);
        let result = DbSize::parse_response(frame).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_flushdb_frame() {
        let cmd = FlushDb::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("FLUSHDB"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_flushdb_async_frame() {
        let cmd = FlushDb::new().async_mode();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("ASYNC"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_flushall_frame() {
        let cmd = FlushAll::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("FLUSHALL"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_randomkey_frame() {
        let cmd = RandomKey::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("RANDOMKEY"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_randomkey_response_some() {
        let frame = Frame::BulkString(Some(Bytes::from("mykey")));
        let result = RandomKey::parse_response(frame).unwrap();
        assert_eq!(result, Some("mykey".to_string()));
    }

    #[test]
    fn test_randomkey_response_none() {
        let frame = Frame::Null;
        let result = RandomKey::parse_response(frame).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_time_frame() {
        let cmd = Time::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("TIME"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_time_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("1609459200"))),
            Frame::BulkString(Some(Bytes::from("123456"))),
        ]);

        let result = Time::parse_response(frame).unwrap();
        assert_eq!(result, (1609459200, 123456));
    }

    #[test]
    fn test_lastsave_frame() {
        let cmd = LastSave::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("LASTSAVE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_lastsave_response() {
        let frame = Frame::Integer(1609459200);
        let result = LastSave::parse_response(frame).unwrap();
        assert_eq!(result, 1609459200);
    }
}
