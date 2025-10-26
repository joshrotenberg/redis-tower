//! Redis Functions commands (Redis 7.0+)
//!
//! Functions provide a way to load server-side Lua scripts that persist across
//! server restarts, with better organization and versioning than EVAL.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// FUNCTION LOAD command - Load a library of functions
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::functions::FunctionLoad;
/// let code = r#"
/// #!lua name=mylib
/// redis.register_function('myfunc', function(keys, args)
///     return 'Hello'
/// end)
/// "#;
/// let cmd = FunctionLoad::new(code);
///
/// // Replace existing library
/// let cmd = FunctionLoad::new(code).replace();
/// ```
#[derive(Debug, Clone)]
pub struct FunctionLoad {
    pub(crate) code: String,
    pub(crate) replace: bool,
}

impl FunctionLoad {
    /// Create a new FUNCTION LOAD command
    pub fn new(code: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            replace: false,
        }
    }

    /// Replace existing library if it exists
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }
}

impl Command for FunctionLoad {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("FUNCTION"))),
            Frame::BulkString(Some(Bytes::from("LOAD"))),
        ];

        if self.replace {
            args.push(Frame::BulkString(Some(Bytes::from("REPLACE"))));
        }

        args.push(Frame::BulkString(Some(Bytes::from(self.code.clone()))));

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// FUNCTION DELETE command - Delete a library
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::functions::FunctionDelete;
/// let cmd = FunctionDelete::new("mylib");
/// ```
#[derive(Debug, Clone)]
pub struct FunctionDelete {
    pub(crate) library_name: String,
}

impl FunctionDelete {
    /// Create a new FUNCTION DELETE command
    pub fn new(library_name: impl Into<String>) -> Self {
        Self {
            library_name: library_name.into(),
        }
    }
}

impl Command for FunctionDelete {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("FUNCTION"))),
            Frame::BulkString(Some(Bytes::from("DELETE"))),
            Frame::BulkString(Some(Bytes::from(self.library_name.clone()))),
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

/// FCALL command - Call a function
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::functions::FCall;
/// // Call without keys or args
/// let cmd = FCall::new("myfunc");
///
/// // Call with keys and arguments
/// let cmd = FCall::new("process")
///     .key("user:1")
///     .key("user:2")
///     .arg("increment")
///     .arg("5");
/// ```
#[derive(Debug, Clone)]
pub struct FCall {
    pub(crate) function: String,
    pub(crate) keys: Vec<String>,
    pub(crate) args: Vec<String>,
}

impl FCall {
    /// Create a new FCALL command
    pub fn new(function: impl Into<String>) -> Self {
        Self {
            function: function.into(),
            keys: Vec::new(),
            args: Vec::new(),
        }
    }

    /// Add a key
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Add an argument
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }
}

impl Command for FCall {
    type Response = String; // Simplified - actual response depends on function

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("FCALL"))),
            Frame::BulkString(Some(Bytes::from(self.function.clone()))),
            Frame::BulkString(Some(Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            args.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
        }

        for arg in &self.args {
            args.push(Frame::BulkString(Some(Bytes::from(arg.clone()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        // Simplified - real implementation would handle various types
        Ok(format!("{:?}", frame))
    }
}

/// FCALL_RO command - Call a read-only function
///
/// Same as FCALL but for functions that only read data (can be routed to replicas)
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::functions::FCallReadOnly;
/// let cmd = FCallReadOnly::new("get_stats")
///     .key("stats:daily");
/// ```
#[derive(Debug, Clone)]
pub struct FCallReadOnly {
    pub(crate) function: String,
    pub(crate) keys: Vec<String>,
    pub(crate) args: Vec<String>,
}

impl FCallReadOnly {
    /// Create a new FCALL_RO command
    pub fn new(function: impl Into<String>) -> Self {
        Self {
            function: function.into(),
            keys: Vec::new(),
            args: Vec::new(),
        }
    }

    /// Add a key
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Add an argument
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }
}

impl Command for FCallReadOnly {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("FCALL_RO"))),
            Frame::BulkString(Some(Bytes::from(self.function.clone()))),
            Frame::BulkString(Some(Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            args.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
        }

        for arg in &self.args {
            args.push(Frame::BulkString(Some(Bytes::from(arg.clone()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
    }
}

/// FUNCTION LIST command - List loaded libraries
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::functions::FunctionList;
/// // List all libraries
/// let cmd = FunctionList::new();
///
/// // Filter by library name pattern
/// let cmd = FunctionList::new().library_name("mylib");
///
/// // Include function code
/// let cmd = FunctionList::new().withcode();
/// ```
#[derive(Debug, Clone)]
pub struct FunctionList {
    pub(crate) library_name: Option<String>,
    pub(crate) withcode: bool,
}

impl FunctionList {
    /// Create a new FUNCTION LIST command
    pub fn new() -> Self {
        Self {
            library_name: None,
            withcode: false,
        }
    }

    /// Filter by library name
    pub fn library_name(mut self, name: impl Into<String>) -> Self {
        self.library_name = Some(name.into());
        self
    }

    /// Include function source code in response
    pub fn withcode(mut self) -> Self {
        self.withcode = true;
        self
    }
}

impl Default for FunctionList {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for FunctionList {
    type Response = String; // Simplified

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("FUNCTION"))),
            Frame::BulkString(Some(Bytes::from("LIST"))),
        ];

        if let Some(name) = &self.library_name {
            args.push(Frame::BulkString(Some(Bytes::from("LIBRARYNAME"))));
            args.push(Frame::BulkString(Some(Bytes::from(name.clone()))));
        }

        if self.withcode {
            args.push(Frame::BulkString(Some(Bytes::from("WITHCODE"))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
    }
}

/// FUNCTION FLUSH command - Delete all libraries
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::functions::FunctionFlush;
/// // Synchronous flush
/// let cmd = FunctionFlush::new();
///
/// // Asynchronous flush
/// let cmd = FunctionFlush::new().async_flush();
/// ```
#[derive(Debug, Clone)]
pub struct FunctionFlush {
    pub(crate) async_flush: bool,
}

impl FunctionFlush {
    /// Create a new FUNCTION FLUSH command
    pub fn new() -> Self {
        Self { async_flush: false }
    }

    /// Flush asynchronously
    pub fn async_flush(mut self) -> Self {
        self.async_flush = true;
        self
    }
}

impl Default for FunctionFlush {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for FunctionFlush {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("FUNCTION"))),
            Frame::BulkString(Some(Bytes::from("FLUSH"))),
        ];

        if self.async_flush {
            args.push(Frame::BulkString(Some(Bytes::from("ASYNC"))));
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

/// FUNCTION DUMP command - Dump all libraries to serialized binary
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::functions::FunctionDump;
/// let cmd = FunctionDump::new();
/// ```
#[derive(Debug, Clone)]
pub struct FunctionDump;

impl FunctionDump {
    /// Create a new FUNCTION DUMP command
    pub fn new() -> Self {
        Self
    }
}

impl Default for FunctionDump {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for FunctionDump {
    type Response = Bytes;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("FUNCTION"))),
            Frame::BulkString(Some(Bytes::from("DUMP"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(data),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// FUNCTION RESTORE command - Restore libraries from dump
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::functions::FunctionRestore;
/// # use bytes::Bytes;
/// let dump = Bytes::from(vec![/* serialized data */]);
/// let cmd = FunctionRestore::new(dump);
///
/// // Replace existing libraries
/// let cmd = FunctionRestore::new(dump).replace();
/// ```
#[derive(Debug, Clone)]
pub struct FunctionRestore {
    pub(crate) serialized_payload: Bytes,
    pub(crate) replace: bool,
    pub(crate) flush: bool,
}

impl FunctionRestore {
    /// Create a new FUNCTION RESTORE command
    pub fn new(serialized_payload: Bytes) -> Self {
        Self {
            serialized_payload,
            replace: false,
            flush: false,
        }
    }

    /// Replace existing libraries
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }

    /// Flush all existing libraries before restoring
    pub fn flush(mut self) -> Self {
        self.flush = true;
        self
    }
}

impl Command for FunctionRestore {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("FUNCTION"))),
            Frame::BulkString(Some(Bytes::from("RESTORE"))),
            Frame::BulkString(Some(self.serialized_payload.clone())),
        ];

        if self.replace {
            args.push(Frame::BulkString(Some(Bytes::from("REPLACE"))));
        }

        if self.flush {
            args.push(Frame::BulkString(Some(Bytes::from("FLUSH"))));
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

/// FUNCTION KILL command - Kill currently executing function
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::functions::FunctionKill;
/// let cmd = FunctionKill::new();
/// ```
#[derive(Debug, Clone)]
pub struct FunctionKill;

impl FunctionKill {
    /// Create a new FUNCTION KILL command
    pub fn new() -> Self {
        Self
    }
}

impl Default for FunctionKill {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for FunctionKill {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("FUNCTION"))),
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

/// FUNCTION STATS command - Get function execution statistics
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::functions::FunctionStats;
/// let cmd = FunctionStats::new();
/// ```
#[derive(Debug, Clone)]
pub struct FunctionStats;

impl FunctionStats {
    /// Create a new FUNCTION STATS command
    pub fn new() -> Self {
        Self
    }
}

impl Default for FunctionStats {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for FunctionStats {
    type Response = String; // Simplified

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("FUNCTION"))),
            Frame::BulkString(Some(Bytes::from("STATS"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_load_frame() {
        let cmd =
            FunctionLoad::new("#!lua name=mylib\nredis.register_function('f', function() end)");

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert_eq!(items[0], Frame::BulkString(Some(Bytes::from("FUNCTION"))));
            assert_eq!(items[1], Frame::BulkString(Some(Bytes::from("LOAD"))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_function_load_replace_frame() {
        let cmd = FunctionLoad::new("code").replace();

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert!(items.contains(&Frame::BulkString(Some(Bytes::from("REPLACE")))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_function_delete_frame() {
        let cmd = FunctionDelete::new("mylib");

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 3);
            assert_eq!(items[1], Frame::BulkString(Some(Bytes::from("DELETE"))));
            assert_eq!(items[2], Frame::BulkString(Some(Bytes::from("mylib"))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_fcall_frame() {
        let cmd = FCall::new("myfunc").key("key1").arg("arg1");

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert_eq!(items[0], Frame::BulkString(Some(Bytes::from("FCALL"))));
            assert_eq!(items[1], Frame::BulkString(Some(Bytes::from("myfunc"))));
            assert_eq!(items[2], Frame::BulkString(Some(Bytes::from("1")))); // numkeys
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_fcall_ro_frame() {
        let cmd = FCallReadOnly::new("readonly_func");

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert_eq!(items[0], Frame::BulkString(Some(Bytes::from("FCALL_RO"))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_function_list_frame() {
        let cmd = FunctionList::new().library_name("mylib").withcode();

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert!(items.contains(&Frame::BulkString(Some(Bytes::from("LIBRARYNAME")))));
            assert!(items.contains(&Frame::BulkString(Some(Bytes::from("WITHCODE")))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_function_flush_frame() {
        let cmd = FunctionFlush::new().async_flush();

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert!(items.contains(&Frame::BulkString(Some(Bytes::from("ASYNC")))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_function_dump_frame() {
        let cmd = FunctionDump::new();

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 2);
            assert_eq!(items[1], Frame::BulkString(Some(Bytes::from("DUMP"))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_function_restore_frame() {
        let cmd = FunctionRestore::new(Bytes::from("data")).replace();

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert!(items.contains(&Frame::BulkString(Some(Bytes::from("REPLACE")))));
        } else {
            panic!("Expected Array frame");
        }
    }
}

/// FUNCTION HELP command - Get help text for FUNCTION subcommands
///
/// Available since Redis 7.0.0.
#[derive(Debug, Clone, Copy)]
pub struct FunctionHelp;

impl FunctionHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FunctionHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::commands::Command for FunctionHelp {
    type Response = Vec<String>;

    fn to_frame(&self) -> crate::codec::Frame {
        crate::codec::Frame::Array(vec![
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from("FUNCTION"))),
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
