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

/// SAVE command - Synchronously save the dataset to disk
///
/// Performs a synchronous save of the dataset, creating a snapshot.
/// This blocks the server until the save is complete.
///
/// **Warning**: SAVE is a blocking operation. Use BGSAVE in production
/// to perform saves in the background.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Save;
///
/// let cmd = Save;
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Save;

impl Command for Save {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("SAVE")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// BGSAVE command - Asynchronously save the dataset to disk
///
/// Creates a background save operation. Redis forks a child process
/// that writes the dataset to disk while the parent continues serving requests.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::BgSave;
///
/// let cmd = BgSave;
/// ```
#[derive(Debug, Clone, Copy)]
pub struct BgSave;

impl Command for BgSave {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("BGSAVE")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// INFO command - Get information and statistics about the server
///
/// Returns information and statistics about the Redis server in a
/// format that is both human-readable and easily parsable by computers.
///
/// You can optionally specify a section to limit the output:
/// - server, clients, memory, persistence, stats, replication,
///   cpu, commandstats, cluster, keyspace, modules, errorstats
/// - all: Return all sections
/// - default: Return default sections
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Info;
///
/// // Get all info
/// let cmd = Info::all();
///
/// // Get specific section
/// let cmd = Info::section("memory");
/// ```
#[derive(Debug, Clone)]
pub struct Info {
    section: Option<String>,
}

impl Info {
    /// Get all server information
    pub fn all() -> Self {
        Self {
            section: Some("all".to_string()),
        }
    }

    /// Get default server information
    pub fn default_info() -> Self {
        Self {
            section: Some("default".to_string()),
        }
    }

    /// Get specific section of server information
    pub fn section(section: impl Into<String>) -> Self {
        Self {
            section: Some(section.into()),
        }
    }

    /// Get all information with no section filter
    pub fn new() -> Self {
        Self { section: None }
    }
}

impl Default for Info {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Info {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("INFO")))];

        if let Some(section) = &self.section {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                section.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            Frame::SimpleString(data) => Ok(String::from_utf8_lossy(&data).into_owned()),
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

/// WAIT - Wait for the synchronous replication of all write commands
///
/// This command blocks the current client until all the previous write commands
/// are successfully transferred and acknowledged by at least the specified number
/// of replicas. If the timeout (specified in milliseconds) is reached, the command
/// returns even if the specified number of replicas were not yet reached.
///
/// # Returns
/// The number of replicas that acknowledged the write commands
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Wait;
///
/// // Wait for 2 replicas with 1 second timeout
/// let cmd = Wait::new(2, 1000);
///
/// // Wait for 1 replica with no timeout (0 means wait forever)
/// let cmd = Wait::new(1, 0);
/// ```
#[derive(Debug, Clone)]
pub struct Wait {
    numreplicas: i64,
    timeout: i64,
}

impl Wait {
    /// Create a new WAIT command
    ///
    /// # Arguments
    /// * `numreplicas` - Minimum number of replicas to reach
    /// * `timeout` - Timeout in milliseconds (0 means wait forever)
    pub fn new(numreplicas: i64, timeout: i64) -> Self {
        Self {
            numreplicas,
            timeout,
        }
    }
}

impl Command for Wait {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("WAIT"))),
            Frame::BulkString(Some(Bytes::from(self.numreplicas.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.timeout.to_string()))),
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

#[cfg(test)]
mod wait_tests {
    use super::*;

    #[test]
    fn test_wait_to_frame() {
        let cmd = Wait::new(2, 1000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3);
                assert_eq!(args[0], Frame::BulkString(Some(Bytes::from("WAIT"))));
                assert_eq!(args[1], Frame::BulkString(Some(Bytes::from("2"))));
                assert_eq!(args[2], Frame::BulkString(Some(Bytes::from("1000"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_wait_no_timeout() {
        let cmd = Wait::new(1, 0);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args[2], Frame::BulkString(Some(Bytes::from("0"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_wait_parse_response() {
        let frame = Frame::Integer(2);
        let result = Wait::parse_response(frame).unwrap();
        assert_eq!(result, 2);
    }

    #[test]
    fn test_wait_parse_zero() {
        let frame = Frame::Integer(0);
        let result = Wait::parse_response(frame).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn test_wait_parse_error() {
        let frame = Frame::Error(Bytes::from("ERR invalid arguments"));
        assert!(Wait::parse_response(frame).is_err());
    }
}
