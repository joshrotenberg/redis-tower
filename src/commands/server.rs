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

    // Tests for new SERVER/ADMIN commands

    #[test]
    fn test_bgrewriteaof_frame() {
        let cmd = BgRewriteAof::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("BGREWRITEAOF")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_config_get_frame() {
        let cmd = ConfigGet::new("maxmemory");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CONFIG"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("GET"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("maxmemory"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_config_get_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("maxmemory"))),
            Frame::BulkString(Some(Bytes::from("2gb"))),
        ]);
        let result = ConfigGet::parse_response(frame).unwrap();
        assert_eq!(result, vec![("maxmemory".to_string(), "2gb".to_string())]);
    }

    #[test]
    fn test_config_set_frame() {
        let cmd = ConfigSet::new("maxmemory", "2gb");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CONFIG"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("SET"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("maxmemory"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("2gb"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_config_rewrite_frame() {
        let cmd = ConfigRewrite::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CONFIG"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("REWRITE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_config_resetstat_frame() {
        let cmd = ConfigResetStat::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CONFIG"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("RESETSTAT"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_command_cmd_frame() {
        let cmd = CommandCmd::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("COMMAND"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_command_count_frame() {
        let cmd = CommandCount::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("COMMAND"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("COUNT"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_command_count_response() {
        let frame = Frame::Integer(238);
        let result = CommandCount::parse_response(frame).unwrap();
        assert_eq!(result, 238);
    }

    #[test]
    fn test_command_info_frame() {
        let cmd = CommandInfo::new(vec!["GET", "SET"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("COMMAND"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("INFO"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("GET"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("SET"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_slowlog_get_frame() {
        let cmd = SlowlogGet::new(10);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("SLOWLOG"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("GET"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("10"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_slowlog_get_all_frame() {
        let cmd = SlowlogGet::all();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("SLOWLOG"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("GET"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_slowlog_len_frame() {
        let cmd = SlowlogLen::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("SLOWLOG"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("LEN"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_slowlog_len_response() {
        let frame = Frame::Integer(42);
        let result = SlowlogLen::parse_response(frame).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_slowlog_get_response() {
        // Simulate a slowlog entry response
        let frame = Frame::Array(vec![Frame::Array(vec![
            Frame::Integer(1),          // id
            Frame::Integer(1609459200), // timestamp
            Frame::Integer(15000),      // duration in microseconds
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("GET"))),
                Frame::BulkString(Some(Bytes::from("key123"))),
            ]),
            Frame::BulkString(Some(Bytes::from("127.0.0.1:54321"))), // client address
            Frame::BulkString(Some(Bytes::from("myclient"))),        // client name
        ])]);

        let result = SlowlogGet::parse_response(frame).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, 1);
        assert_eq!(result[0].timestamp, 1609459200);
        assert_eq!(result[0].duration_micros, 15000);
        assert_eq!(result[0].command, vec!["GET", "key123"]);
        assert_eq!(
            result[0].client_address,
            Some("127.0.0.1:54321".to_string())
        );
        assert_eq!(result[0].client_name, Some("myclient".to_string()));
    }

    #[test]
    fn test_slowlog_get_response_minimal() {
        // Test with minimal fields (pre-Redis 4.0)
        let frame = Frame::Array(vec![Frame::Array(vec![
            Frame::Integer(2),
            Frame::Integer(1609459300),
            Frame::Integer(25000),
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("SET"))),
                Frame::BulkString(Some(Bytes::from("mykey"))),
                Frame::BulkString(Some(Bytes::from("myvalue"))),
            ]),
        ])]);

        let result = SlowlogGet::parse_response(frame).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, 2);
        assert_eq!(result[0].client_address, None);
        assert_eq!(result[0].client_name, None);
    }

    #[test]
    fn test_slowlog_reset_frame() {
        let cmd = SlowlogReset::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("SLOWLOG"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("RESET"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_memory_usage_frame() {
        let cmd = MemoryUsage::new("mykey");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MEMORY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("USAGE"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("mykey"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_memory_usage_with_samples_frame() {
        let cmd = MemoryUsage::new("mykey").samples(5);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MEMORY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("USAGE"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("mykey"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("SAMPLES"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("5"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_memory_usage_response() {
        let frame = Frame::Integer(1024);
        let result = MemoryUsage::parse_response(frame).unwrap();
        assert_eq!(result, Some(1024));
    }

    #[test]
    fn test_memory_usage_response_none() {
        let frame = Frame::Null;
        let result = MemoryUsage::parse_response(frame).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_memory_stats_frame() {
        let cmd = MemoryStats::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MEMORY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("STATS"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_shutdown_frame() {
        let cmd = Shutdown::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("SHUTDOWN"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_shutdown_save_frame() {
        let cmd = Shutdown::new().save();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("SHUTDOWN"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("SAVE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_shutdown_nosave_frame() {
        let cmd = Shutdown::new().nosave();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("SHUTDOWN"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("NOSAVE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_replicaof_frame() {
        let cmd = ReplicaOf::new("127.0.0.1", 6379);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("REPLICAOF"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("127.0.0.1"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("6379"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_replicaof_no_one_frame() {
        let cmd = ReplicaOf::no_one();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("REPLICAOF"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("NO"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("ONE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_role_frame() {
        let cmd = Role::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("ROLE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_command_docs_all() {
        let cmd = CommandDocs::all();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("COMMAND"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("DOCS"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_command_docs_specific() {
        let cmd = CommandDocs::new(vec!["GET", "SET"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("COMMAND"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("DOCS"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("GET"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("SET"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_command_getkeys_frame() {
        let cmd = CommandGetKeys::new("MSET", vec!["a", "b", "c", "d"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 7);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("COMMAND"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("GETKEYS"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("MSET"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("a"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("b"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_command_getkeys_parse() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::BulkString(Some(Bytes::from("c"))),
        ]);

        let result = CommandGetKeys::parse_response(frame).unwrap();
        assert_eq!(result, vec!["a", "c"]);
    }

    #[test]
    fn test_command_getkeysandflags_frame() {
        let cmd = CommandGetKeysAndFlags::new("LMOVE", vec!["mylist1", "mylist2", "left", "left"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 7);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("COMMAND"))));
                assert_eq!(
                    parts[1],
                    Frame::BulkString(Some(Bytes::from("GETKEYSANDFLAGS")))
                );
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("LMOVE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_command_getkeysandflags_parse() {
        let frame = Frame::Array(vec![
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("mylist1"))),
                Frame::Array(vec![
                    Frame::BulkString(Some(Bytes::from("RW"))),
                    Frame::BulkString(Some(Bytes::from("access"))),
                ]),
            ]),
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("mylist2"))),
                Frame::Array(vec![
                    Frame::BulkString(Some(Bytes::from("RW"))),
                    Frame::BulkString(Some(Bytes::from("insert"))),
                ]),
            ]),
        ]);

        let result = CommandGetKeysAndFlags::parse_response(frame).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].key, "mylist1");
        assert_eq!(result[0].flags, vec!["RW", "access"]);
        assert_eq!(result[1].key, "mylist2");
        assert_eq!(result[1].flags, vec!["RW", "insert"]);
    }

    #[test]
    fn test_command_list_all() {
        let cmd = CommandList::all();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("COMMAND"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("LIST"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_command_list_filter_module() {
        let cmd = CommandList::new(CommandListFilter::Module("mymodule".to_string()));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("FILTERBY"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("MODULE"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("mymodule"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_command_list_filter_aclcat() {
        let cmd = CommandList::new(CommandListFilter::AclCategory("dangerous".to_string()));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("ACLCAT"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("dangerous"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_command_list_filter_pattern() {
        let cmd = CommandList::new(CommandListFilter::Pattern("z*".to_string()));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("PATTERN"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("z*"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_command_list_parse() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("get"))),
            Frame::BulkString(Some(Bytes::from("set"))),
            Frame::BulkString(Some(Bytes::from("del"))),
        ]);

        let result = CommandList::parse_response(frame).unwrap();
        assert_eq!(result, vec!["get", "set", "del"]);
    }

    #[test]
    fn test_memory_doctor_frame() {
        let cmd = MemoryDoctor::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MEMORY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("DOCTOR"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_memory_doctor_parse() {
        let frame = Frame::BulkString(Some(Bytes::from(
            "Sam, I have a few observations about your memory...",
        )));
        let result = MemoryDoctor::parse_response(frame).unwrap();
        assert_eq!(
            result,
            "Sam, I have a few observations about your memory..."
        );
    }

    #[test]
    fn test_memory_malloc_stats_frame() {
        let cmd = MemoryMallocStats::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MEMORY"))));
                assert_eq!(
                    parts[1],
                    Frame::BulkString(Some(Bytes::from("MALLOC-STATS")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_memory_malloc_stats_parse() {
        let frame = Frame::BulkString(Some(Bytes::from("___ Begin jemalloc statistics ___\n...")));
        let result = MemoryMallocStats::parse_response(frame).unwrap();
        assert!(result.starts_with("___ Begin jemalloc"));
    }

    #[test]
    fn test_memory_purge_frame() {
        let cmd = MemoryPurge::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MEMORY"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("PURGE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_memory_purge_parse() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let result = MemoryPurge::parse_response(frame);
        assert!(result.is_ok());
    }
}

/// BGREWRITEAOF command - Asynchronously rewrite the append-only file
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::BgRewriteAof;
///
/// let cmd = BgRewriteAof::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct BgRewriteAof;

impl BgRewriteAof {
    /// Create a new BGREWRITEAOF command
    pub fn new() -> Self {
        Self
    }
}

impl Default for BgRewriteAof {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for BgRewriteAof {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("BGREWRITEAOF")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CONFIG GET command - Get configuration parameters
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ConfigGet;
///
/// let cmd = ConfigGet::new("maxmemory");
/// let cmd = ConfigGet::new("save");
/// ```
#[derive(Debug, Clone)]
pub struct ConfigGet {
    pub(crate) parameter: String,
}

impl ConfigGet {
    /// Create a new CONFIG GET command
    pub fn new(parameter: impl Into<String>) -> Self {
        Self {
            parameter: parameter.into(),
        }
    }
}

impl Command for ConfigGet {
    type Response = Vec<(String, String)>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CONFIG"))),
            Frame::BulkString(Some(Bytes::from("GET"))),
            Frame::BulkString(Some(Bytes::from(self.parameter.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                let mut i = 0;
                while i + 1 < items.len() {
                    let key = match &items[i] {
                        Frame::BulkString(Some(k)) => String::from_utf8_lossy(k).into_owned(),
                        _ => return Err(RedisError::UnexpectedResponse),
                    };
                    let value = match &items[i + 1] {
                        Frame::BulkString(Some(v)) => String::from_utf8_lossy(v).into_owned(),
                        Frame::BulkString(None) => String::new(),
                        _ => return Err(RedisError::UnexpectedResponse),
                    };
                    result.push((key, value));
                    i += 2;
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CONFIG SET command - Set configuration parameters
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ConfigSet;
///
/// let cmd = ConfigSet::new("maxmemory", "2gb");
/// let cmd = ConfigSet::new("timeout", "300");
/// ```
#[derive(Debug, Clone)]
pub struct ConfigSet {
    pub(crate) parameter: String,
    pub(crate) value: String,
}

impl ConfigSet {
    /// Create a new CONFIG SET command
    pub fn new(parameter: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            parameter: parameter.into(),
            value: value.into(),
        }
    }
}

impl Command for ConfigSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CONFIG"))),
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from(self.parameter.clone()))),
            Frame::BulkString(Some(Bytes::from(self.value.clone()))),
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

/// CONFIG REWRITE command - Rewrite the configuration file
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ConfigRewrite;
///
/// let cmd = ConfigRewrite::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ConfigRewrite;

impl ConfigRewrite {
    /// Create a new CONFIG REWRITE command
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConfigRewrite {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ConfigRewrite {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CONFIG"))),
            Frame::BulkString(Some(Bytes::from("REWRITE"))),
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

/// CONFIG RESETSTAT command - Reset INFO statistics
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ConfigResetStat;
///
/// let cmd = ConfigResetStat::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ConfigResetStat;

impl ConfigResetStat {
    /// Create a new CONFIG RESETSTAT command
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConfigResetStat {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ConfigResetStat {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CONFIG"))),
            Frame::BulkString(Some(Bytes::from("RESETSTAT"))),
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

/// COMMAND command - Get array of Redis command details
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::CommandCmd;
///
/// let cmd = CommandCmd::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct CommandCmd;

impl CommandCmd {
    /// Create a new COMMAND command
    pub fn new() -> Self {
        Self
    }
}

impl Default for CommandCmd {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for CommandCmd {
    type Response = String; // Simplified - returns complex array

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("COMMAND")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
    }
}

/// COMMAND COUNT command - Get total number of Redis commands
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::CommandCount;
///
/// let cmd = CommandCount::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct CommandCount;

impl CommandCount {
    /// Create a new COMMAND COUNT command
    pub fn new() -> Self {
        Self
    }
}

impl Default for CommandCount {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for CommandCount {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("COMMAND"))),
            Frame::BulkString(Some(Bytes::from("COUNT"))),
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

/// COMMAND INFO command - Get specific command info
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::CommandInfo;
///
/// let cmd = CommandInfo::new(vec!["GET", "SET"]);
/// ```
#[derive(Debug, Clone)]
pub struct CommandInfo {
    pub(crate) commands: Vec<String>,
}

impl CommandInfo {
    /// Create a new COMMAND INFO command
    pub fn new(commands: Vec<impl Into<String>>) -> Self {
        Self {
            commands: commands.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl Command for CommandInfo {
    type Response = String; // Simplified

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("COMMAND"))),
            Frame::BulkString(Some(Bytes::from("INFO"))),
        ];

        for cmd in &self.commands {
            args.push(Frame::BulkString(Some(Bytes::from(cmd.clone()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
    }
}

/// COMMAND DOCS command - Get command documentation
///
/// Returns documentary information about one, multiple or all commands.
///
/// Available since Redis 7.0.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::CommandDocs;
///
/// // Get all command documentation
/// let cmd = CommandDocs::all();
///
/// // Get documentation for specific commands
/// let cmd = CommandDocs::new(vec!["GET", "SET"]);
/// ```
#[derive(Debug, Clone)]
pub struct CommandDocs {
    commands: Vec<String>,
}

impl CommandDocs {
    /// Create a new COMMAND DOCS command for specific commands
    pub fn new(commands: Vec<impl Into<String>>) -> Self {
        Self {
            commands: commands.into_iter().map(|c| c.into()).collect(),
        }
    }

    /// Get documentation for all commands
    pub fn all() -> Self {
        Self {
            commands: Vec::new(),
        }
    }
}

impl Command for CommandDocs {
    type Response = String; // Simplified - returns complex nested map

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("COMMAND"))),
            Frame::BulkString(Some(Bytes::from("DOCS"))),
        ];

        for cmd in &self.commands {
            args.push(Frame::BulkString(Some(Bytes::from(cmd.clone()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Ok(format!("{:?}", frame)),
        }
    }
}

/// COMMAND GETKEYS command - Extract key names from a command
///
/// Returns the key names from a full Redis command.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::CommandGetKeys;
///
/// // Extract keys from MSET command
/// let cmd = CommandGetKeys::new("MSET", vec!["a", "b", "c", "d"]);
///
/// // Extract keys from EVAL command
/// let cmd = CommandGetKeys::new("EVAL", vec!["script", "3", "key1", "key2", "key3", "arg1"]);
/// ```
#[derive(Debug, Clone)]
pub struct CommandGetKeys {
    command: String,
    args: Vec<String>,
}

impl CommandGetKeys {
    /// Create a new COMMAND GETKEYS command
    pub fn new(command: impl Into<String>, args: Vec<impl Into<String>>) -> Self {
        Self {
            command: command.into(),
            args: args.into_iter().map(|a| a.into()).collect(),
        }
    }
}

impl Command for CommandGetKeys {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("COMMAND"))),
            Frame::BulkString(Some(Bytes::from("GETKEYS"))),
            Frame::BulkString(Some(Bytes::from(self.command.clone()))),
        ];

        for arg in &self.args {
            frames.push(Frame::BulkString(Some(Bytes::from(arg.clone()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut keys = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            keys.push(String::from_utf8_lossy(&data).to_string());
                        }
                        Frame::BulkString(None) => {}
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

/// Key with access flags from COMMAND GETKEYSANDFLAGS
#[derive(Debug, Clone)]
pub struct KeyWithFlags {
    /// The key name
    pub key: String,
    /// Access flags for this key (e.g., "RW", "RO", "OW", "RM", etc.)
    pub flags: Vec<String>,
}

/// COMMAND GETKEYSANDFLAGS command - Extract key names and access flags
///
/// Returns the key names and their access flags from a full Redis command.
///
/// Available since Redis 7.0.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::CommandGetKeysAndFlags;
///
/// // Extract keys and flags from LMOVE command
/// let cmd = CommandGetKeysAndFlags::new("LMOVE", vec!["mylist1", "mylist2", "left", "left"]);
/// ```
#[derive(Debug, Clone)]
pub struct CommandGetKeysAndFlags {
    command: String,
    args: Vec<String>,
}

impl CommandGetKeysAndFlags {
    /// Create a new COMMAND GETKEYSANDFLAGS command
    pub fn new(command: impl Into<String>, args: Vec<impl Into<String>>) -> Self {
        Self {
            command: command.into(),
            args: args.into_iter().map(|a| a.into()).collect(),
        }
    }
}

impl Command for CommandGetKeysAndFlags {
    type Response = Vec<KeyWithFlags>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("COMMAND"))),
            Frame::BulkString(Some(Bytes::from("GETKEYSANDFLAGS"))),
            Frame::BulkString(Some(Bytes::from(self.command.clone()))),
        ];

        for arg in &self.args {
            frames.push(Frame::BulkString(Some(Bytes::from(arg.clone()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::Array(pair) if pair.len() == 2 => {
                            // Each item is [key, [flags]]
                            let key = match &pair[0] {
                                Frame::BulkString(Some(data)) => {
                                    String::from_utf8_lossy(data).to_string()
                                }
                                _ => return Err(RedisError::UnexpectedResponse),
                            };

                            let flags = match &pair[1] {
                                Frame::Array(flag_items) => {
                                    let mut f = Vec::new();
                                    for flag in flag_items {
                                        match flag {
                                            Frame::BulkString(Some(data)) => {
                                                f.push(String::from_utf8_lossy(data).to_string());
                                            }
                                            _ => return Err(RedisError::UnexpectedResponse),
                                        }
                                    }
                                    f
                                }
                                _ => return Err(RedisError::UnexpectedResponse),
                            };

                            result.push(KeyWithFlags { key, flags });
                        }
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

/// Filter for COMMAND LIST command
#[derive(Debug, Clone)]
pub enum CommandListFilter {
    /// Filter by module name
    Module(String),
    /// Filter by ACL category
    AclCategory(String),
    /// Filter by pattern (glob-like)
    Pattern(String),
}

/// COMMAND LIST command - Get list of command names
///
/// Returns an array of the server's command names.
///
/// Available since Redis 7.0.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::{CommandList, CommandListFilter};
///
/// // Get all command names
/// let cmd = CommandList::all();
///
/// // Filter by ACL category
/// let cmd = CommandList::new(CommandListFilter::AclCategory("dangerous".to_string()));
///
/// // Filter by pattern
/// let cmd = CommandList::new(CommandListFilter::Pattern("z*".to_string()));
/// ```
#[derive(Debug, Clone)]
pub struct CommandList {
    filter: Option<CommandListFilter>,
}

impl CommandList {
    /// Create a new COMMAND LIST command with a filter
    pub fn new(filter: CommandListFilter) -> Self {
        Self {
            filter: Some(filter),
        }
    }

    /// Get all command names (no filter)
    pub fn all() -> Self {
        Self { filter: None }
    }
}

impl Command for CommandList {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("COMMAND"))),
            Frame::BulkString(Some(Bytes::from("LIST"))),
        ];

        if let Some(filter) = &self.filter {
            frames.push(Frame::BulkString(Some(Bytes::from("FILTERBY"))));
            match filter {
                CommandListFilter::Module(name) => {
                    frames.push(Frame::BulkString(Some(Bytes::from("MODULE"))));
                    frames.push(Frame::BulkString(Some(Bytes::from(name.clone()))));
                }
                CommandListFilter::AclCategory(cat) => {
                    frames.push(Frame::BulkString(Some(Bytes::from("ACLCAT"))));
                    frames.push(Frame::BulkString(Some(Bytes::from(cat.clone()))));
                }
                CommandListFilter::Pattern(pat) => {
                    frames.push(Frame::BulkString(Some(Bytes::from("PATTERN"))));
                    frames.push(Frame::BulkString(Some(Bytes::from(pat.clone()))));
                }
            }
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut commands = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            commands.push(String::from_utf8_lossy(&data).to_string());
                        }
                        Frame::BulkString(None) => {}
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(commands)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// COMMAND HELP command - Get help text for COMMAND subcommands
///
/// Returns helpful text describing the different COMMAND subcommands.
///
/// Available since Redis 5.0.0.
#[derive(Debug, Clone, Copy)]
pub struct CommandHelp;

impl CommandHelp {
    /// Create a new COMMAND HELP command
    pub fn new() -> Self {
        Self
    }
}

impl Default for CommandHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for CommandHelp {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("COMMAND"))),
            Frame::BulkString(Some(Bytes::from("HELP"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut help = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            help.push(String::from_utf8_lossy(&data).to_string());
                        }
                        _ => {}
                    }
                }
                Ok(help)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Slowlog entry - represents a single slow query log entry
///
/// Each entry contains information about a slow command execution.
#[derive(Debug, Clone)]
pub struct SlowlogEntry {
    /// Unique progressive identifier for every slow log entry
    pub id: i64,
    /// Unix timestamp at which the logged command was processed
    pub timestamp: i64,
    /// Amount of time needed for execution, in microseconds
    pub duration_micros: i64,
    /// Array of command arguments (first element is the command name)
    pub command: Vec<String>,
    /// Client IP address and port (Redis 4.0+)
    pub client_address: Option<String>,
    /// Client name (Redis 4.0+)
    pub client_name: Option<String>,
}

/// SLOWLOG GET command - Get the Redis slow queries log
///
/// Returns a list of slow query log entries.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::SlowlogGet;
///
/// // Get last 10 entries
/// let cmd = SlowlogGet::new(10);
///
/// // Get all entries
/// let cmd = SlowlogGet::all();
/// ```
#[derive(Debug, Clone)]
pub struct SlowlogGet {
    pub(crate) count: Option<i64>,
}

impl SlowlogGet {
    /// Create a new SLOWLOG GET command with count
    pub fn new(count: i64) -> Self {
        Self { count: Some(count) }
    }

    /// Get all slowlog entries
    pub fn all() -> Self {
        Self { count: None }
    }
}

impl Command for SlowlogGet {
    type Response = Vec<SlowlogEntry>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("SLOWLOG"))),
            Frame::BulkString(Some(Bytes::from("GET"))),
        ];

        if let Some(count) = self.count {
            args.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(entries) => {
                let mut result = Vec::new();

                for entry in entries {
                    match entry {
                        Frame::Array(fields) if fields.len() >= 4 => {
                            // Parse ID (field 0)
                            let id = match &fields[0] {
                                Frame::Integer(n) => *n,
                                _ => return Err(RedisError::UnexpectedResponse),
                            };

                            // Parse timestamp (field 1)
                            let timestamp = match &fields[1] {
                                Frame::Integer(n) => *n,
                                _ => return Err(RedisError::UnexpectedResponse),
                            };

                            // Parse duration (field 2)
                            let duration_micros = match &fields[2] {
                                Frame::Integer(n) => *n,
                                _ => return Err(RedisError::UnexpectedResponse),
                            };

                            // Parse command array (field 3)
                            let command = match &fields[3] {
                                Frame::Array(cmd_parts) => {
                                    let mut cmd = Vec::new();
                                    for part in cmd_parts {
                                        match part {
                                            Frame::BulkString(Some(data)) => {
                                                cmd.push(
                                                    String::from_utf8_lossy(data).into_owned(),
                                                );
                                            }
                                            Frame::SimpleString(data) => {
                                                cmd.push(
                                                    String::from_utf8_lossy(data).into_owned(),
                                                );
                                            }
                                            _ => return Err(RedisError::UnexpectedResponse),
                                        }
                                    }
                                    cmd
                                }
                                _ => return Err(RedisError::UnexpectedResponse),
                            };

                            // Parse optional client address (field 4, Redis 4.0+)
                            let client_address = if fields.len() > 4 {
                                match &fields[4] {
                                    Frame::BulkString(Some(data)) => {
                                        Some(String::from_utf8_lossy(data).into_owned())
                                    }
                                    Frame::SimpleString(data) => {
                                        Some(String::from_utf8_lossy(data).into_owned())
                                    }
                                    _ => None,
                                }
                            } else {
                                None
                            };

                            // Parse optional client name (field 5, Redis 4.0+)
                            let client_name = if fields.len() > 5 {
                                match &fields[5] {
                                    Frame::BulkString(Some(data)) => {
                                        Some(String::from_utf8_lossy(data).into_owned())
                                    }
                                    Frame::SimpleString(data) => {
                                        Some(String::from_utf8_lossy(data).into_owned())
                                    }
                                    Frame::BulkString(None) | Frame::Null => None,
                                    _ => None,
                                }
                            } else {
                                None
                            };

                            result.push(SlowlogEntry {
                                id,
                                timestamp,
                                duration_micros,
                                command,
                                client_address,
                                client_name,
                            });
                        }
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

/// SLOWLOG LEN command - Get the length of the slowlog
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::SlowlogLen;
///
/// let cmd = SlowlogLen::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct SlowlogLen;

impl SlowlogLen {
    /// Create a new SLOWLOG LEN command
    pub fn new() -> Self {
        Self
    }
}

impl Default for SlowlogLen {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for SlowlogLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SLOWLOG"))),
            Frame::BulkString(Some(Bytes::from("LEN"))),
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

/// SLOWLOG RESET command - Clear the slowlog
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::SlowlogReset;
///
/// let cmd = SlowlogReset::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct SlowlogReset;

impl SlowlogReset {
    /// Create a new SLOWLOG RESET command
    pub fn new() -> Self {
        Self
    }
}

impl Default for SlowlogReset {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for SlowlogReset {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SLOWLOG"))),
            Frame::BulkString(Some(Bytes::from("RESET"))),
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

/// MEMORY USAGE command - Estimate memory usage of a key
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::MemoryUsage;
///
/// let cmd = MemoryUsage::new("mykey");
/// let cmd = MemoryUsage::new("mykey").samples(5);
/// ```
#[derive(Debug, Clone)]
pub struct MemoryUsage {
    pub(crate) key: String,
    pub(crate) samples: Option<i64>,
}

impl MemoryUsage {
    /// Create a new MEMORY USAGE command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            samples: None,
        }
    }

    /// Set number of sampled nested values
    pub fn samples(mut self, count: i64) -> Self {
        self.samples = Some(count);
        self
    }
}

impl Command for MemoryUsage {
    type Response = Option<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("MEMORY"))),
            Frame::BulkString(Some(Bytes::from("USAGE"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ];

        if let Some(samples) = self.samples {
            args.push(Frame::BulkString(Some(Bytes::from("SAMPLES"))));
            args.push(Frame::BulkString(Some(Bytes::from(samples.to_string()))));
        }

        Frame::Array(args)
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

/// MEMORY STATS command - Get memory statistics
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::MemoryStats;
///
/// let cmd = MemoryStats::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct MemoryStats;

impl MemoryStats {
    /// Create a new MEMORY STATS command
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemoryStats {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for MemoryStats {
    type Response = String; // Simplified

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("MEMORY"))),
            Frame::BulkString(Some(Bytes::from("STATS"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
    }
}

/// MEMORY DOCTOR command - Get memory problems report
///
/// Reports about different memory-related issues that the Redis server
/// experiences, and advises about possible remedies.
///
/// Available since Redis 4.0.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::MemoryDoctor;
///
/// let cmd = MemoryDoctor::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct MemoryDoctor;

impl MemoryDoctor {
    /// Create a new MEMORY DOCTOR command
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemoryDoctor {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for MemoryDoctor {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("MEMORY"))),
            Frame::BulkString(Some(Bytes::from("DOCTOR"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// MEMORY MALLOC-STATS command - Get allocator statistics
///
/// Provides an internal statistics report from the memory allocator.
///
/// This command is currently implemented only when using jemalloc as an
/// allocator, and evaluates to a benign NOOP for all others.
///
/// Available since Redis 4.0.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::MemoryMallocStats;
///
/// let cmd = MemoryMallocStats::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct MemoryMallocStats;

impl MemoryMallocStats {
    /// Create a new MEMORY MALLOC-STATS command
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemoryMallocStats {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for MemoryMallocStats {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("MEMORY"))),
            Frame::BulkString(Some(Bytes::from("MALLOC-STATS"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// MEMORY PURGE command - Ask allocator to release memory
///
/// Attempts to purge dirty pages so these can be reclaimed by the allocator.
///
/// This command is currently implemented only when using jemalloc as an
/// allocator, and evaluates to a benign NOOP for all others.
///
/// Available since Redis 4.0.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::MemoryPurge;
///
/// let cmd = MemoryPurge::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct MemoryPurge;

impl MemoryPurge {
    /// Create a new MEMORY PURGE command
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemoryPurge {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for MemoryPurge {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("MEMORY"))),
            Frame::BulkString(Some(Bytes::from("PURGE"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// MEMORY HELP command - Get help text for MEMORY subcommands
///
/// Available since Redis 4.0.0.
#[derive(Debug, Clone, Copy)]
pub struct MemoryHelp;

impl MemoryHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemoryHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for MemoryHelp {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("MEMORY"))),
            Frame::BulkString(Some(Bytes::from("HELP"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => Ok(items
                .into_iter()
                .filter_map(|item| match item {
                    Frame::BulkString(Some(data)) => {
                        Some(String::from_utf8_lossy(&data).to_string())
                    }
                    _ => None,
                })
                .collect()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CONFIG HELP command - Get help text for CONFIG subcommands
///
/// Available since Redis 5.0.0.
#[derive(Debug, Clone, Copy)]
pub struct ConfigHelp;

impl ConfigHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConfigHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ConfigHelp {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CONFIG"))),
            Frame::BulkString(Some(Bytes::from("HELP"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => Ok(items
                .into_iter()
                .filter_map(|item| match item {
                    Frame::BulkString(Some(data)) => {
                        Some(String::from_utf8_lossy(&data).to_string())
                    }
                    _ => None,
                })
                .collect()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SLOWLOG HELP command - Get help text for SLOWLOG subcommands
///
/// Available since Redis 6.2.0.
#[derive(Debug, Clone, Copy)]
pub struct SlowlogHelp;

impl SlowlogHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SlowlogHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for SlowlogHelp {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SLOWLOG"))),
            Frame::BulkString(Some(Bytes::from("HELP"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => Ok(items
                .into_iter()
                .filter_map(|item| match item {
                    Frame::BulkString(Some(data)) => {
                        Some(String::from_utf8_lossy(&data).to_string())
                    }
                    _ => None,
                })
                .collect()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// MODULE HELP command - Get help text for MODULE subcommands
///
/// Available since Redis 5.0.0.
#[derive(Debug, Clone, Copy)]
pub struct ModuleHelp;

impl ModuleHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ModuleHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ModuleHelp {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("MODULE"))),
            Frame::BulkString(Some(Bytes::from("HELP"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => Ok(items
                .into_iter()
                .filter_map(|item| match item {
                    Frame::BulkString(Some(data)) => {
                        Some(String::from_utf8_lossy(&data).to_string())
                    }
                    _ => None,
                })
                .collect()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// OBJECT HELP command - Get help text for OBJECT subcommands
///
/// Available since Redis 6.2.0.
#[derive(Debug, Clone, Copy)]
pub struct ObjectHelp;

impl ObjectHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ObjectHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ObjectHelp {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("OBJECT"))),
            Frame::BulkString(Some(Bytes::from("HELP"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => Ok(items
                .into_iter()
                .filter_map(|item| match item {
                    Frame::BulkString(Some(data)) => {
                        Some(String::from_utf8_lossy(&data).to_string())
                    }
                    _ => None,
                })
                .collect()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SHUTDOWN command - Synchronously save and shut down the server
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Shutdown;
///
/// let cmd = Shutdown::new();
/// let cmd = Shutdown::new().save();
/// let cmd = Shutdown::new().nosave();
/// ```
#[derive(Debug, Clone)]
pub struct Shutdown {
    pub(crate) save_option: Option<bool>,
}

impl Shutdown {
    /// Create a new SHUTDOWN command
    pub fn new() -> Self {
        Self { save_option: None }
    }

    /// Force save before shutdown
    pub fn save(mut self) -> Self {
        self.save_option = Some(true);
        self
    }

    /// Don't save before shutdown
    pub fn nosave(mut self) -> Self {
        self.save_option = Some(false);
        self
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Shutdown {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![Frame::BulkString(Some(Bytes::from("SHUTDOWN")))];

        if let Some(save) = self.save_option {
            args.push(Frame::BulkString(Some(Bytes::from(if save {
                "SAVE"
            } else {
                "NOSAVE"
            }))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// REPLICAOF command - Make server a replica of another instance
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ReplicaOf;
///
/// // Make this server a replica
/// let cmd = ReplicaOf::new("127.0.0.1", 6379);
///
/// // Stop replication (promote to master)
/// let cmd = ReplicaOf::no_one();
/// ```
#[derive(Debug, Clone)]
pub struct ReplicaOf {
    pub(crate) host: Option<String>,
    pub(crate) port: Option<u16>,
}

impl ReplicaOf {
    /// Create a new REPLICAOF command
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: Some(host.into()),
            port: Some(port),
        }
    }

    /// Stop replication and become a master
    pub fn no_one() -> Self {
        Self {
            host: None,
            port: None,
        }
    }
}

impl Command for ReplicaOf {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let args = if let (Some(host), Some(port)) = (&self.host, &self.port) {
            vec![
                Frame::BulkString(Some(Bytes::from("REPLICAOF"))),
                Frame::BulkString(Some(Bytes::from(host.clone()))),
                Frame::BulkString(Some(Bytes::from(port.to_string()))),
            ]
        } else {
            vec![
                Frame::BulkString(Some(Bytes::from("REPLICAOF"))),
                Frame::BulkString(Some(Bytes::from("NO"))),
                Frame::BulkString(Some(Bytes::from("ONE"))),
            ]
        };

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ROLE command - Return the role of the instance
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Role;
///
/// let cmd = Role::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Role;

impl Role {
    /// Create a new ROLE command
    pub fn new() -> Self {
        Self
    }
}

impl Default for Role {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Role {
    type Response = String; // Simplified

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("ROLE")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
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

/// FAILOVER command - Start coordinated failover to replica
///
/// Starts a coordinated failover between the master and one of its replicas.
/// This command allows manually triggering a failover with various options.
///
/// Available since Redis 6.2.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Failover;
///
/// // Start normal failover
/// let cmd = Failover::new();
///
/// // Failover to specific replica
/// let cmd = Failover::to("127.0.0.1", 6380);
///
/// // Force failover to specific replica
/// let cmd = Failover::to("127.0.0.1", 6380).force();
///
/// // Abort ongoing failover
/// let cmd = Failover::abort();
///
/// // Failover with timeout
/// let cmd = Failover::new().timeout(10000);
/// ```
#[derive(Debug, Clone)]
pub struct Failover {
    to_host: Option<String>,
    to_port: Option<u16>,
    force: bool,
    abort: bool,
    timeout: Option<i64>,
}

impl Failover {
    /// Create a new FAILOVER command
    pub fn new() -> Self {
        Self {
            to_host: None,
            to_port: None,
            force: false,
            abort: false,
            timeout: None,
        }
    }

    /// Failover to a specific replica
    pub fn to(host: impl Into<String>, port: u16) -> Self {
        Self {
            to_host: Some(host.into()),
            to_port: Some(port),
            force: false,
            abort: false,
            timeout: None,
        }
    }

    /// Force the failover (skip offset check)
    pub fn force(mut self) -> Self {
        self.force = true;
        self
    }

    /// Abort an ongoing failover
    pub fn abort() -> Self {
        Self {
            to_host: None,
            to_port: None,
            force: false,
            abort: true,
            timeout: None,
        }
    }

    /// Set failover timeout in milliseconds
    pub fn timeout(mut self, milliseconds: i64) -> Self {
        self.timeout = Some(milliseconds);
        self
    }
}

impl Default for Failover {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Failover {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("FAILOVER")))];

        if let (Some(host), Some(port)) = (&self.to_host, &self.to_port) {
            frames.push(Frame::BulkString(Some(Bytes::from("TO"))));
            frames.push(Frame::BulkString(Some(Bytes::from(host.clone()))));
            frames.push(Frame::BulkString(Some(Bytes::from(port.to_string()))));

            if self.force {
                frames.push(Frame::BulkString(Some(Bytes::from("FORCE"))));
            }
        }

        if self.abort {
            frames.push(Frame::BulkString(Some(Bytes::from("ABORT"))));
        }

        if let Some(timeout) = self.timeout {
            frames.push(Frame::BulkString(Some(Bytes::from("TIMEOUT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(timeout.to_string()))));
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

/// DEBUG command subcommands
///
/// Available since Redis 1.0.0
#[derive(Debug, Clone)]
pub enum DebugSubcommand {
    /// DEBUG OBJECT key - Get debugging information about a key
    Object(String),
    /// DEBUG SEGFAULT - Crash the server (for testing)
    Segfault,
    /// DEBUG SLEEP seconds - Sleep for N seconds
    Sleep(f64),
    /// DEBUG RELOAD - Reload the server configuration
    Reload,
    /// DEBUG RESTART - Restart the server
    Restart,
    /// DEBUG DIGEST - Get digest of the dataset
    Digest,
    /// DEBUG DIGEST-VALUE key [key ...] - Get digest of specific keys
    DigestValue(Vec<String>),
    /// DEBUG POPULATE count [prefix] [size] - Create test keys
    Populate {
        /// Number of keys to create
        count: i64,
        /// Optional key prefix
        prefix: Option<String>,
        /// Optional value size in bytes
        size: Option<i64>,
    },
    /// DEBUG PROTOCOL - Get information about the protocol
    Protocol(String),
    /// DEBUG SDSLEN key - Get SDS string length
    SdsLen(String),
    /// Other DEBUG subcommands
    Other(String, Vec<String>),
}

/// DEBUG command - Internal debugging command
///
/// This is an internal command used for developing and testing Redis.
/// It provides various debugging subcommands.
///
/// **Warning**: This command is for internal use and testing only.
///
/// Available since Redis 1.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::{Debug, DebugSubcommand};
///
/// // Debug object information
/// let cmd = Debug::new(DebugSubcommand::Object("mykey".into()));
///
/// // Crash server (testing only!)
/// let cmd = Debug::new(DebugSubcommand::Segfault);
///
/// // Sleep for 2.5 seconds
/// let cmd = Debug::new(DebugSubcommand::Sleep(2.5));
/// ```
#[derive(Debug, Clone)]
pub struct Debug {
    subcommand: DebugSubcommand,
}

impl Debug {
    /// Create a new DEBUG command
    pub fn new(subcommand: DebugSubcommand) -> Self {
        Self { subcommand }
    }
}

impl Command for Debug {
    type Response = String; // Response varies by subcommand

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("DEBUG")))];

        match &self.subcommand {
            DebugSubcommand::Object(key) => {
                frames.push(Frame::BulkString(Some(Bytes::from("OBJECT"))));
                frames.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
            }
            DebugSubcommand::Segfault => {
                frames.push(Frame::BulkString(Some(Bytes::from("SEGFAULT"))));
            }
            DebugSubcommand::Sleep(seconds) => {
                frames.push(Frame::BulkString(Some(Bytes::from("SLEEP"))));
                frames.push(Frame::BulkString(Some(Bytes::from(seconds.to_string()))));
            }
            DebugSubcommand::Reload => {
                frames.push(Frame::BulkString(Some(Bytes::from("RELOAD"))));
            }
            DebugSubcommand::Restart => {
                frames.push(Frame::BulkString(Some(Bytes::from("RESTART"))));
            }
            DebugSubcommand::Digest => {
                frames.push(Frame::BulkString(Some(Bytes::from("DIGEST"))));
            }
            DebugSubcommand::DigestValue(keys) => {
                frames.push(Frame::BulkString(Some(Bytes::from("DIGEST-VALUE"))));
                for key in keys {
                    frames.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
                }
            }
            DebugSubcommand::Populate {
                count,
                prefix,
                size,
            } => {
                frames.push(Frame::BulkString(Some(Bytes::from("POPULATE"))));
                frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
                if let Some(p) = prefix {
                    frames.push(Frame::BulkString(Some(Bytes::from(p.clone()))));
                }
                if let Some(s) = size {
                    frames.push(Frame::BulkString(Some(Bytes::from(s.to_string()))));
                }
            }
            DebugSubcommand::Protocol(arg) => {
                frames.push(Frame::BulkString(Some(Bytes::from("PROTOCOL"))));
                frames.push(Frame::BulkString(Some(Bytes::from(arg.clone()))));
            }
            DebugSubcommand::SdsLen(key) => {
                frames.push(Frame::BulkString(Some(Bytes::from("SDSLEN"))));
                frames.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
            }
            DebugSubcommand::Other(subcmd, args) => {
                frames.push(Frame::BulkString(Some(Bytes::from(subcmd.clone()))));
                for arg in args {
                    frames.push(Frame::BulkString(Some(Bytes::from(arg.clone()))));
                }
            }
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(data) | Frame::BulkString(Some(data)) => {
                Ok(String::from_utf8_lossy(&data).into_owned())
            }
            Frame::Integer(n) => Ok(n.to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Ok(format!("{:?}", frame)),
        }
    }
}

#[cfg(test)]
mod debug_tests {
    use super::*;

    #[test]
    fn test_debug_object_frame() {
        let cmd = Debug::new(DebugSubcommand::Object("mykey".into()));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3);
                assert_eq!(args[0], Frame::BulkString(Some(Bytes::from("DEBUG"))));
                assert_eq!(args[1], Frame::BulkString(Some(Bytes::from("OBJECT"))));
                assert_eq!(args[2], Frame::BulkString(Some(Bytes::from("mykey"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_debug_segfault_frame() {
        let cmd = Debug::new(DebugSubcommand::Segfault);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 2);
                assert_eq!(args[0], Frame::BulkString(Some(Bytes::from("DEBUG"))));
                assert_eq!(args[1], Frame::BulkString(Some(Bytes::from("SEGFAULT"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_debug_sleep_frame() {
        let cmd = Debug::new(DebugSubcommand::Sleep(2.5));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3);
                assert_eq!(args[0], Frame::BulkString(Some(Bytes::from("DEBUG"))));
                assert_eq!(args[1], Frame::BulkString(Some(Bytes::from("SLEEP"))));
                assert_eq!(args[2], Frame::BulkString(Some(Bytes::from("2.5"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_debug_populate_frame() {
        let cmd = Debug::new(DebugSubcommand::Populate {
            count: 1000,
            prefix: Some("test".into()),
            size: Some(64),
        });
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 5);
                assert_eq!(args[0], Frame::BulkString(Some(Bytes::from("DEBUG"))));
                assert_eq!(args[1], Frame::BulkString(Some(Bytes::from("POPULATE"))));
                assert_eq!(args[2], Frame::BulkString(Some(Bytes::from("1000"))));
                assert_eq!(args[3], Frame::BulkString(Some(Bytes::from("test"))));
                assert_eq!(args[4], Frame::BulkString(Some(Bytes::from("64"))));
            }
            _ => panic!("Expected Array frame"),
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

    #[test]
    fn test_failover_basic_frame() {
        let cmd = Failover::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("FAILOVER"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_failover_to_frame() {
        let cmd = Failover::to("127.0.0.1", 6380);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("FAILOVER"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("TO"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("127.0.0.1"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("6380"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_failover_force_frame() {
        let cmd = Failover::to("127.0.0.1", 6380).force();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("FORCE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_failover_abort_frame() {
        let cmd = Failover::abort();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("ABORT")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_failover_timeout_frame() {
        let cmd = Failover::new().timeout(10000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("TIMEOUT")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("10000")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }
}

/// SWAPDB command - Swap two Redis databases (Redis 4.0+)
///
/// Atomically swaps two databases so that clients connected to a given database
/// will see the data of the other database.
///
/// Available since Redis 4.0.0.
#[derive(Debug, Clone, Copy)]
pub struct SwapDb {
    index1: i64,
    index2: i64,
}

impl SwapDb {
    /// Create a new SWAPDB command
    pub fn new(index1: i64, index2: i64) -> Self {
        Self { index1, index2 }
    }
}

impl Command for SwapDb {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SWAPDB"))),
            Frame::BulkString(Some(Bytes::from(self.index1.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.index2.to_string()))),
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

/// SYNC command - Internal command for replication
///
/// Used by Redis replicas for replicating data from master.
/// This is an internal command that should not be used by clients.
#[derive(Debug, Clone, Copy)]
pub struct Sync;

impl Sync {
    /// Create a new SYNC command
    pub fn new() -> Self {
        Self
    }
}

impl Default for Sync {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Sync {
    type Response = Bytes;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("SYNC")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(data),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// PSYNC command - Partial resynchronization for replication (Redis 2.8+)
///
/// Used by Redis replicas to request partial resynchronization from master.
///
/// Available since Redis 2.8.0.
#[derive(Debug, Clone)]
pub struct PSync {
    replication_id: String,
    offset: i64,
}

impl PSync {
    /// Create a new PSYNC command
    pub fn new(replication_id: impl Into<String>, offset: i64) -> Self {
        Self {
            replication_id: replication_id.into(),
            offset,
        }
    }

    /// PSYNC with ? ? for full sync
    pub fn full_sync() -> Self {
        Self {
            replication_id: "?".to_string(),
            offset: -1,
        }
    }
}

impl Command for PSync {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("PSYNC"))),
            Frame::BulkString(Some(Bytes::from(self.replication_id.clone()))),
            Frame::BulkString(Some(Bytes::from(self.offset.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// REPLCONF command - Configure replication connection
///
/// Used by replicas to configure the replication stream.
///
/// Available since Redis 2.8.0.
#[derive(Debug, Clone)]
pub struct ReplConf {
    args: Vec<String>,
}

impl ReplConf {
    /// Create a new REPLCONF command with custom arguments
    pub fn new(args: Vec<impl Into<String>>) -> Self {
        Self {
            args: args.into_iter().map(|a| a.into()).collect(),
        }
    }

    /// REPLCONF listening-port
    pub fn listening_port(port: u16) -> Self {
        Self {
            args: vec!["listening-port".to_string(), port.to_string()],
        }
    }

    /// REPLCONF capa (capabilities)
    pub fn capa(capability: impl Into<String>) -> Self {
        Self {
            args: vec!["capa".to_string(), capability.into()],
        }
    }

    /// REPLCONF getack
    pub fn getack() -> Self {
        Self {
            args: vec!["GETACK".to_string(), "*".to_string()],
        }
    }
}

impl Command for ReplConf {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("REPLCONF")))];
        for arg in &self.args {
            frames.push(Frame::BulkString(Some(Bytes::from(arg.clone()))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).to_string()),
            Frame::Array(_) => Ok("OK".to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SLAVEOF command - Make server a replica (deprecated, use REPLICAOF)
///
/// Configure a server to become a replica of another instance.
/// This command is deprecated, use REPLICAOF instead.
///
/// Available since Redis 1.0.0.
#[derive(Debug, Clone)]
pub struct SlaveOf {
    host: String,
    port: u16,
}

impl SlaveOf {
    /// Create a new SLAVEOF command
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// SLAVEOF NO ONE - stop replication
    pub fn no_one() -> Self {
        Self {
            host: "NO".to_string(),
            port: 0, // Will be replaced with "ONE"
        }
    }
}

impl Command for SlaveOf {
    type Response = ();

    fn to_frame(&self) -> Frame {
        if self.host == "NO" {
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("SLAVEOF"))),
                Frame::BulkString(Some(Bytes::from("NO"))),
                Frame::BulkString(Some(Bytes::from("ONE"))),
            ])
        } else {
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("SLAVEOF"))),
                Frame::BulkString(Some(Bytes::from(self.host.clone()))),
                Frame::BulkString(Some(Bytes::from(self.port.to_string()))),
            ])
        }
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// LOLWUT command - Display Redis version-specific artwork
///
/// A fun command that displays computer art and the Redis version.
/// The output changes with each Redis version.
///
/// Available since Redis 5.0.0.
#[derive(Debug, Clone, Copy)]
pub struct Lolwut {
    version: Option<i64>,
}

impl Lolwut {
    /// Create a new LOLWUT command
    pub fn new() -> Self {
        Self { version: None }
    }

    /// Specify version to display
    pub fn version(mut self, version: i64) -> Self {
        self.version = Some(version);
        self
    }
}

impl Default for Lolwut {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Lolwut {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("LOLWUT")))];
        if let Some(v) = self.version {
            frames.push(Frame::BulkString(Some(Bytes::from("VERSION"))));
            frames.push(Frame::BulkString(Some(Bytes::from(v.to_string()))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).to_string()),
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}
