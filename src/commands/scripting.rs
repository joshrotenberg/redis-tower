//! Redis scripting commands (Level 5 complexity)
//!
//! These commands allow executing Lua scripts on the Redis server.
//! They support dynamic return types via the RedisValue enum.

use crate::codec::Frame;
use crate::commands::Command;
use crate::read_preference::ReadOnly;
use crate::types::value::FromFrame;
use crate::types::{RedisError, RedisValue};
use bytes::Bytes;
use sha1::{Digest, Sha1};

/// EVAL command - Execute a Lua script
///
/// Executes a Lua script on the Redis server. The script can access keys and arguments
/// passed to it, and can call Redis commands. Scripts are atomic and executed as a single
/// operation. The return type is dynamic (RedisValue) as scripts can return any Redis type.
///
/// # Request
/// - `script`: The Lua script source code
/// - `keys` (optional): Keys that the script will access (available as KEYS in Lua)
/// - `args` (optional): Additional arguments (available as ARGV in Lua)
///
/// # Response
/// Returns `RedisValue` - The script's return value, which can be any Redis type:
/// - Integer, String, Array, Null, etc. depending on what the script returns
///
/// # Redis Version
/// Available since Redis 2.6.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::scripting::Eval;
/// use redis_tower::types::RedisValue;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let script = r#"
///     local key = KEYS[1]
///     local value = ARGV[1]
///     redis.call('SET', key, value)
///     return redis.call('GET', key)
/// "#;
///
/// let cmd = Eval::new(script)
///     .key("mykey")
///     .arg("myvalue");
/// let result: RedisValue = client.call(cmd).await?;
/// # Ok(())
/// # }
/// ```
pub struct Eval {
    pub(crate) script: String,
    pub(crate) keys: Vec<String>,
    pub(crate) args: Vec<Bytes>,
}

impl Eval {
    /// Create a new EVAL command with a Lua script
    pub fn new(script: impl Into<String>) -> Self {
        Self {
            script: script.into(),
            keys: Vec::new(),
            args: Vec::new(),
        }
    }

    /// Add a key that the script will access
    ///
    /// Keys are available in the Lua script via the KEYS table.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Add multiple keys that the script will access
    pub fn keys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.keys.extend(keys.into_iter().map(Into::into));
        self
    }

    /// Add an argument to pass to the script
    ///
    /// Arguments are available in the Lua script via the ARGV table.
    pub fn arg(mut self, arg: impl Into<Bytes>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Add multiple arguments to pass to the script
    pub fn args<I, B>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = B>,
        B: Into<Bytes>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Calculate the SHA1 hash of the script
    ///
    /// This can be used for EVALSHA to avoid sending the script repeatedly.
    pub fn sha1(&self) -> String {
        let mut hasher = Sha1::new();
        hasher.update(self.script.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

impl Command for Eval {
    type Response = RedisValue;

    fn to_frame(&self) -> Frame {
        let mut parts = Vec::new();
        parts.push(Frame::BulkString(Some(Bytes::from("EVAL"))));
        parts.push(Frame::BulkString(Some(Bytes::from(self.script.clone()))));
        parts.push(Frame::BulkString(Some(Bytes::from(
            self.keys.len().to_string(),
        ))));

        for key in &self.keys {
            parts.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
        }

        for arg in &self.args {
            parts.push(Frame::BulkString(Some(arg.clone())));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            other => RedisValue::from_frame(other),
        }
    }
}

/// EVALSHA command - Execute a Lua script by its SHA1 hash
///
/// Executes a previously cached Lua script by its SHA1 hash. This is more efficient than EVAL
/// when the same script is executed repeatedly, as it avoids sending the script source code.
/// If the script is not in the cache, returns a NOSCRIPT error.
///
/// # Request
/// - `sha1`: The SHA1 hash of the script (40-character hex string)
/// - `keys` (optional): Keys that the script will access (available as KEYS in Lua)
/// - `args` (optional): Additional arguments (available as ARGV in Lua)
///
/// # Response
/// Returns `RedisValue` - The script's return value (same as EVAL)
///
/// # Redis Version
/// Available since Redis 2.6.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::scripting::{Eval, EvalSha};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let script = r#"return redis.call('GET', KEYS[1])"#;
/// let eval = Eval::new(script);
/// let sha = eval.sha1();
///
/// // First time: use EVAL (or SCRIPT LOAD) to cache the script
/// client.call(eval).await?;
///
/// // Subsequent times: use EVALSHA with the cached SHA1
/// let cmd = EvalSha::new(&sha).key("mykey");
/// let result = client.call(cmd).await?;
/// # Ok(())
/// # }
/// ```
pub struct EvalSha {
    pub(crate) sha1: String,
    pub(crate) keys: Vec<String>,
    pub(crate) args: Vec<Bytes>,
}

impl EvalSha {
    /// Create a new EVALSHA command with a script SHA1 hash
    pub fn new(sha1: impl Into<String>) -> Self {
        Self {
            sha1: sha1.into(),
            keys: Vec::new(),
            args: Vec::new(),
        }
    }

    /// Add a key that the script will access
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Add multiple keys that the script will access
    pub fn keys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.keys.extend(keys.into_iter().map(Into::into));
        self
    }

    /// Add an argument to pass to the script
    pub fn arg(mut self, arg: impl Into<Bytes>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Add multiple arguments to pass to the script
    pub fn args<I, B>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = B>,
        B: Into<Bytes>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }
}

impl Command for EvalSha {
    type Response = RedisValue;

    fn to_frame(&self) -> Frame {
        let mut parts = Vec::new();
        parts.push(Frame::BulkString(Some(Bytes::from("EVALSHA"))));
        parts.push(Frame::BulkString(Some(Bytes::from(self.sha1.clone()))));
        parts.push(Frame::BulkString(Some(Bytes::from(
            self.keys.len().to_string(),
        ))));

        for key in &self.keys {
            parts.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
        }

        for arg in &self.args {
            parts.push(Frame::BulkString(Some(arg.clone())));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Error(e) => {
                let err_str = String::from_utf8_lossy(&e).to_string();
                // Check for NOSCRIPT error
                if err_str.starts_with("NOSCRIPT") {
                    Err(RedisError::Protocol(format!(
                        "Script not found. Use EVAL or SCRIPT LOAD first: {}",
                        err_str
                    )))
                } else {
                    Err(RedisError::Redis(err_str))
                }
            }
            other => RedisValue::from_frame(other),
        }
    }
}

/// SCRIPT LOAD command - Load a script into the script cache
///
/// Loads a Lua script into the script cache without executing it. Returns the SHA1 hash of
/// the script, which can be used with EVALSHA. The script remains cached until SCRIPT FLUSH
/// is called or the server restarts.
///
/// # Request
/// - `script`: The Lua script source code to cache
///
/// # Response
/// Returns `String` - The SHA1 hash of the script (40-character hex string)
///
/// # Redis Version
/// Available since Redis 2.6.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::scripting::ScriptLoad;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let script = r#"return redis.call('GET', KEYS[1])"#;
/// let cmd = ScriptLoad::new(script);
/// let sha = client.call(cmd).await?;
/// println!("Script cached with SHA1: {}", sha);
/// // Now use EVALSHA with this SHA1
/// # Ok(())
/// # }
/// ```
pub struct ScriptLoad {
    pub(crate) script: String,
}

impl ScriptLoad {
    /// Create a new SCRIPT LOAD command
    pub fn new(script: impl Into<String>) -> Self {
        Self {
            script: script.into(),
        }
    }
}

impl Command for ScriptLoad {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SCRIPT"))),
            Frame::BulkString(Some(Bytes::from("LOAD"))),
            Frame::BulkString(Some(Bytes::from(self.script.clone()))),
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

/// SCRIPT EXISTS command - Check if scripts exist in the cache
///
/// Checks if one or more scripts exist in the script cache by their SHA1 hashes.
/// Returns a boolean for each SHA1 provided, in the same order.
///
/// # Request
/// - `sha1s`: One or more SHA1 hashes to check (40-character hex strings)
///
/// # Response
/// Returns `Vec<bool>` - Boolean for each SHA1:
/// - `true` - Script exists in cache
/// - `false` - Script does not exist in cache
///
/// # Redis Version
/// Available since Redis 2.6.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::scripting::ScriptExists;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let cmd = ScriptExists::new()
///     .sha1("abc123...")
///     .sha1("def456...");
/// let exists = client.call(cmd).await?;
/// println!("Script 1 exists: {}", exists[0]);
/// println!("Script 2 exists: {}", exists[1]);
/// # Ok(())
/// # }
/// ```
pub struct ScriptExists {
    pub(crate) sha1s: Vec<String>,
}

impl ScriptExists {
    /// Create a new SCRIPT EXISTS command
    pub fn new() -> Self {
        Self { sha1s: Vec::new() }
    }

    /// Add a SHA1 hash to check
    pub fn sha1(mut self, sha1: impl Into<String>) -> Self {
        self.sha1s.push(sha1.into());
        self
    }

    /// Add multiple SHA1 hashes to check
    pub fn sha1s<I, S>(mut self, sha1s: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.sha1s.extend(sha1s.into_iter().map(Into::into));
        self
    }
}

impl Default for ScriptExists {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ScriptExists {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![
            Frame::BulkString(Some(Bytes::from("SCRIPT"))),
            Frame::BulkString(Some(Bytes::from("EXISTS"))),
        ];

        for sha1 in &self.sha1s {
            parts.push(Frame::BulkString(Some(Bytes::from(sha1.clone()))));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::new();
                for item in items {
                    match item {
                        Frame::Integer(i) => results.push(i != 0),
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

/// SCRIPT FLUSH command - Remove all scripts from the script cache
///
/// Removes all Lua scripts from the script cache. Can be executed synchronously (default)
/// or asynchronously (Redis 6.2+). After flushing, all cached scripts must be reloaded
/// before they can be used with EVALSHA.
///
/// # Request
/// - `async_mode` (optional): If true, flush asynchronously without blocking (Redis 6.2+)
///
/// # Response
/// Returns `String` - "OK" on success
///
/// # Redis Version
/// Available since Redis 2.6.0. ASYNC option available since Redis 6.2.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::scripting::ScriptFlush;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Synchronous flush (blocks until complete)
/// let cmd = ScriptFlush::new();
/// client.call(cmd).await?;
///
/// // Asynchronous flush (Redis 6.2+)
/// let cmd = ScriptFlush::new().async_mode();
/// client.call(cmd).await?;
/// # Ok(())
/// # }
/// ```
pub struct ScriptFlush {
    pub(crate) async_mode: bool,
}

impl ScriptFlush {
    /// Create a new SCRIPT FLUSH command
    pub fn new() -> Self {
        Self { async_mode: false }
    }

    /// Enable ASYNC mode (Redis 6.2+)
    ///
    /// The flush will happen asynchronously without blocking.
    pub fn async_mode(mut self) -> Self {
        self.async_mode = true;
        self
    }
}

impl Default for ScriptFlush {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ScriptFlush {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![
            Frame::BulkString(Some(Bytes::from("SCRIPT"))),
            Frame::BulkString(Some(Bytes::from("FLUSH"))),
        ];

        if self.async_mode {
            parts.push(Frame::BulkString(Some(Bytes::from("ASYNC"))));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eval_sha1() {
        let script = "return redis.call('GET', KEYS[1])";
        let eval = Eval::new(script);
        let sha1 = eval.sha1();

        // SHA1 should be 40 characters hex
        assert_eq!(sha1.len(), 40);
        assert!(sha1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_eval_frame() {
        let eval = Eval::new("return 1").key("mykey").arg(b"myvalue".to_vec());

        let frame = eval.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5); // EVAL, script, numkeys, key, arg
            }
            _ => panic!("Expected array frame"),
        }
    }

    #[test]
    fn test_evalsha_frame() {
        let evalsha = EvalSha::new("abc123").key("mykey").arg(b"myvalue".to_vec());

        let frame = evalsha.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5); // EVALSHA, sha1, numkeys, key, arg
            }
            _ => panic!("Expected array frame"),
        }
    }

    #[test]
    fn test_script_load_frame() {
        let load = ScriptLoad::new("return 1");
        let frame = load.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3); // SCRIPT, LOAD, script
            }
            _ => panic!("Expected array frame"),
        }
    }

    #[test]
    fn test_script_exists_frame() {
        let exists = ScriptExists::new().sha1("abc123").sha1("def456");

        let frame = exists.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4); // SCRIPT, EXISTS, sha1, sha1
            }
            _ => panic!("Expected array frame"),
        }
    }

    #[test]
    fn test_script_flush_frame() {
        let flush = ScriptFlush::new();
        let frame = flush.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2); // SCRIPT, FLUSH
            }
            _ => panic!("Expected array frame"),
        }
    }

    #[test]
    fn test_script_flush_async_frame() {
        let flush = ScriptFlush::new().async_mode();
        let frame = flush.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3); // SCRIPT, FLUSH, ASYNC
            }
            _ => panic!("Expected array frame"),
        }
    }
}

/// SCRIPT DEBUG command - Set script debugging mode
///
/// Sets the debugging mode for subsequent EVAL/EVALSHA commands. In debug mode, Redis will
/// provide step-by-step execution information for Lua scripts. Use YES for synchronous debugging
/// (blocking), SYNC for asynchronous debugging, or NO to disable.
///
/// # Request
/// - `mode`: The debugging mode (Yes, Sync, or No)
///
/// # Response
/// Returns `()` - Always returns OK
///
/// # Redis Version
/// Available since Redis 3.2.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::scripting::{ScriptDebug, ScriptDebugMode};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Enable synchronous debugging (blocking)
/// let cmd = ScriptDebug::new(ScriptDebugMode::Yes);
/// client.call(cmd).await?;
///
/// // Enable asynchronous debugging
/// let cmd = ScriptDebug::new(ScriptDebugMode::Sync);
/// client.call(cmd).await?;
///
/// // Disable debugging
/// let cmd = ScriptDebug::new(ScriptDebugMode::No);
/// client.call(cmd).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ScriptDebug {
    mode: ScriptDebugMode,
}

/// Script debugging modes
#[derive(Debug, Clone, Copy)]
pub enum ScriptDebugMode {
    /// Enable synchronous debugging
    Yes,
    /// Enable asynchronous debugging
    Sync,
    /// Disable debugging
    No,
}

impl ScriptDebug {
    /// Create a new SCRIPT DEBUG command
    pub fn new(mode: ScriptDebugMode) -> Self {
        Self { mode }
    }
}

impl Command for ScriptDebug {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mode_str = match self.mode {
            ScriptDebugMode::Yes => "YES",
            ScriptDebugMode::Sync => "SYNC",
            ScriptDebugMode::No => "NO",
        };

        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SCRIPT"))),
            Frame::BulkString(Some(Bytes::from("DEBUG"))),
            Frame::BulkString(Some(Bytes::from(mode_str))),
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

/// SCRIPT KILL command - Kill currently executing script
///
/// Kills the currently executing Lua script, assuming no write operations were performed
/// by the script. If the script has already performed write operations, this command will
/// fail and you must use SHUTDOWN NOSAVE to stop the server.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `()` - Always returns OK if successful
///
/// # Redis Version
/// Available since Redis 2.6.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::scripting::ScriptKill;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Kill a long-running script
/// let cmd = ScriptKill::new();
/// client.call(cmd).await?;
/// println!("Script killed successfully");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ScriptKill;

impl ScriptKill {
    /// Create a new SCRIPT KILL command
    pub fn new() -> Self {
        Self
    }
}

impl Default for ScriptKill {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ScriptKill {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SCRIPT"))),
            Frame::BulkString(Some(Bytes::from("KILL"))),
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

/// SCRIPT HELP command - Get help text for SCRIPT subcommands
///
/// Available since Redis 5.0.0.
#[derive(Debug, Clone, Copy)]
pub struct ScriptHelp;

impl ScriptHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ScriptHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::commands::Command for ScriptHelp {
    type Response = Vec<String>;

    fn to_frame(&self) -> crate::codec::Frame {
        crate::codec::Frame::Array(vec![
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from("SCRIPT"))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from("HELP"))),
        ])
    }

    fn parse_response(
        frame: crate::codec::Frame,
    ) -> Result<Self::Response, crate::types::RedisError> {
        match frame {
            crate::codec::Frame::Array(items) => Ok(items
                .into_iter()
                .filter_map(|item| match item {
                    crate::codec::Frame::BulkString(Some(data)) => {
                        Some(String::from_utf8_lossy(&data).to_string())
                    }
                    _ => None,
                })
                .collect()),
            crate::codec::Frame::Error(e) => Err(crate::types::RedisError::from_redis_error(
                &String::from_utf8_lossy(&e),
            )),
            _ => Err(crate::types::RedisError::UnexpectedResponse),
        }
    }
}

/// EVAL_RO command - Execute a read-only Lua script (Redis 7.0+)
///
/// Identical to EVAL but marked as read-only for cluster replica routing.
///
/// Available since Redis 7.0.0.
#[derive(Debug, Clone)]
pub struct EvalReadOnly {
    script: String,
    keys: Vec<String>,
    args: Vec<String>,
}

impl EvalReadOnly {
    pub fn new(
        script: impl Into<String>,
        keys: Vec<impl Into<String>>,
        args: Vec<impl Into<String>>,
    ) -> Self {
        Self {
            script: script.into(),
            keys: keys.into_iter().map(|k| k.into()).collect(),
            args: args.into_iter().map(|a| a.into()).collect(),
        }
    }
}

impl crate::commands::Command for EvalReadOnly {
    type Response = crate::types::RedisValue;

    fn to_frame(&self) -> crate::codec::Frame {
        let mut frames = vec![
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from("EVAL_RO"))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from(self.script.clone()))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            frames.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                key.clone(),
            ))));
        }

        for arg in &self.args {
            frames.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                arg.clone(),
            ))));
        }

        crate::codec::Frame::Array(frames)
    }

    fn parse_response(
        frame: crate::codec::Frame,
    ) -> Result<Self::Response, crate::types::RedisError> {
        crate::types::RedisValue::from_frame(frame)
    }
}

impl ReadOnly for EvalReadOnly {
    fn is_read_only(&self) -> bool {
        true
    }
}

/// EVALSHA_RO command - Execute a read-only cached Lua script (Redis 7.0+)
///
/// Identical to EVALSHA but marked as read-only for cluster replica routing.
///
/// Available since Redis 7.0.0.
#[derive(Debug, Clone)]
pub struct EvalShaReadOnly {
    sha1: String,
    keys: Vec<String>,
    args: Vec<String>,
}

impl EvalShaReadOnly {
    pub fn new(
        sha1: impl Into<String>,
        keys: Vec<impl Into<String>>,
        args: Vec<impl Into<String>>,
    ) -> Self {
        Self {
            sha1: sha1.into(),
            keys: keys.into_iter().map(|k| k.into()).collect(),
            args: args.into_iter().map(|a| a.into()).collect(),
        }
    }
}

impl crate::commands::Command for EvalShaReadOnly {
    type Response = crate::types::RedisValue;

    fn to_frame(&self) -> crate::codec::Frame {
        let mut frames = vec![
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from("EVALSHA_RO"))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from(self.sha1.clone()))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            frames.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                key.clone(),
            ))));
        }

        for arg in &self.args {
            frames.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                arg.clone(),
            ))));
        }

        crate::codec::Frame::Array(frames)
    }

    fn parse_response(
        frame: crate::codec::Frame,
    ) -> Result<Self::Response, crate::types::RedisError> {
        crate::types::RedisValue::from_frame(frame)
    }
}

impl ReadOnly for EvalShaReadOnly {
    fn is_read_only(&self) -> bool {
        true
    }
}
