//! Redis scripting commands (Level 5 complexity)
//!
//! These commands allow executing Lua scripts on the Redis server.
//! They support dynamic return types via the RedisValue enum.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::value::FromFrame;
use crate::types::{RedisError, RedisValue};
use bytes::Bytes;
use sha1::{Digest, Sha1};

/// EVAL command - Execute a Lua script
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Eval;
/// use redis_tower::types::RedisValue;
///
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
/// This is more efficient than EVAL when the script has already been
/// loaded via SCRIPT LOAD or a previous EVAL.
///
/// # Example
/// ```no_run
/// use redis_tower::commands::{Eval, EvalSha};
///
/// let script = r#"return redis.call('GET', KEYS[1])"#;
/// let eval = Eval::new(script);
/// let sha = eval.sha1();
///
/// // First time: use EVAL (or SCRIPT LOAD)
/// // Subsequent times: use EVALSHA
/// let cmd = EvalSha::new(&sha).key("mykey");
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
/// Returns the SHA1 hash of the script.
///
/// # Example
/// ```no_run
/// use redis_tower::commands::ScriptLoad;
///
/// let script = r#"return redis.call('GET', KEYS[1])"#;
/// let cmd = ScriptLoad::new(script);
/// // Response will be the SHA1 hash as a String
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
/// Returns a vector of booleans indicating which scripts exist.
///
/// # Example
/// ```no_run
/// use redis_tower::commands::ScriptExists;
///
/// let cmd = ScriptExists::new()
///     .sha1("abc123")
///     .sha1("def456");
/// // Response will be Vec<bool>
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
/// # Example
/// ```no_run
/// use redis_tower::commands::ScriptFlush;
///
/// let cmd = ScriptFlush::new();
/// // Response will be "OK"
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
