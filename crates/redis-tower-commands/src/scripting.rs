use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

use crate::server::FlushMode;

// ---------------------------------------------------------------------------
// EVAL
// ---------------------------------------------------------------------------

/// EVAL script numkeys [key ...] [arg ...]
///
/// Evaluates a Lua script server-side. Returns `Frame` directly because Lua
/// scripts can produce any response type.
pub struct Eval {
    script: String,
    keys: Vec<String>,
    args: Vec<String>,
}

impl Eval {
    pub fn new(script: impl Into<String>) -> Self {
        Self {
            script: script.into(),
            keys: Vec::new(),
            args: Vec::new(),
        }
    }

    /// Add a key argument (populates KEYS table in Lua).
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Add a regular argument (populates ARGV table in Lua).
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }
}

impl Command for Eval {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![
            bulk("EVAL"),
            bulk(self.script.as_str()),
            bulk(self.keys.len().to_string()),
        ];
        for k in &self.keys {
            parts.push(bulk(k.as_str()));
        }
        for a in &self.args {
            parts.push(bulk(a.as_str()));
        }
        array(parts)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "EVAL"
    }
}

// ---------------------------------------------------------------------------
// EVALSHA
// ---------------------------------------------------------------------------

/// EVALSHA sha1 numkeys [key ...] [arg ...]
///
/// Evaluates a cached Lua script by its SHA1 digest. Returns `Frame` directly.
pub struct EvalSha {
    sha1: String,
    keys: Vec<String>,
    args: Vec<String>,
}

impl EvalSha {
    pub fn new(sha1: impl Into<String>) -> Self {
        Self {
            sha1: sha1.into(),
            keys: Vec::new(),
            args: Vec::new(),
        }
    }

    /// Add a key argument (populates KEYS table in Lua).
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Add a regular argument (populates ARGV table in Lua).
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }
}

impl Command for EvalSha {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![
            bulk("EVALSHA"),
            bulk(self.sha1.as_str()),
            bulk(self.keys.len().to_string()),
        ];
        for k in &self.keys {
            parts.push(bulk(k.as_str()));
        }
        for a in &self.args {
            parts.push(bulk(a.as_str()));
        }
        array(parts)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "EVALSHA"
    }
}

// ---------------------------------------------------------------------------
// SCRIPT LOAD
// ---------------------------------------------------------------------------

/// SCRIPT LOAD script
///
/// Loads a Lua script into the script cache without executing it. Returns the
/// SHA1 digest of the script.
pub struct ScriptLoad {
    script: String,
}

impl ScriptLoad {
    pub fn new(script: impl Into<String>) -> Self {
        Self {
            script: script.into(),
        }
    }
}

impl Command for ScriptLoad {
    type Response = String;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("SCRIPT"),
            bulk("LOAD"),
            bulk(self.script.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SCRIPT LOAD"
    }
}

// ---------------------------------------------------------------------------
// SCRIPT EXISTS
// ---------------------------------------------------------------------------

/// SCRIPT EXISTS sha1 [sha1 ...]
///
/// Returns a list of booleans indicating whether each script SHA1 exists in
/// the cache.
pub struct ScriptExists {
    sha1s: Vec<String>,
}

impl ScriptExists {
    pub fn new(sha1: impl Into<String>) -> Self {
        Self {
            sha1s: vec![sha1.into()],
        }
    }

    /// Add another SHA1 digest to check.
    pub fn sha1(mut self, sha1: impl Into<String>) -> Self {
        self.sha1s.push(sha1.into());
        self
    }
}

impl Command for ScriptExists {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![bulk("SCRIPT"), bulk("EXISTS")];
        for s in &self.sha1s {
            parts.push(bulk(s.as_str()));
        }
        array(parts)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::Integer(n) => Ok(n == 1),
                    Frame::Boolean(b) => Ok(b),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "integer or boolean",
                        actual: format!("{other:?}"),
                    }),
                })
                .collect(),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SCRIPT EXISTS"
    }
}

// ---------------------------------------------------------------------------
// SCRIPT FLUSH
// ---------------------------------------------------------------------------

/// SCRIPT FLUSH [ASYNC | SYNC]
///
/// Flushes the Lua script cache.
pub struct ScriptFlush {
    mode: Option<FlushMode>,
}

impl ScriptFlush {
    pub fn new() -> Self {
        Self { mode: None }
    }

    /// Set the flush mode (ASYNC or SYNC).
    pub fn mode(mut self, mode: FlushMode) -> Self {
        self.mode = Some(mode);
        self
    }
}

impl Default for ScriptFlush {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ScriptFlush {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut parts = vec![bulk("SCRIPT"), bulk("FLUSH")];
        match &self.mode {
            Some(FlushMode::Async) => parts.push(bulk("ASYNC")),
            Some(FlushMode::Sync) => parts.push(bulk("SYNC")),
            None => {}
        }
        array(parts)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SCRIPT FLUSH"
    }
}

// ---------------------------------------------------------------------------
// SCRIPT KILL
// ---------------------------------------------------------------------------

/// SCRIPT KILL
///
/// Kills the currently executing Lua script.
pub struct ScriptKill;

impl ScriptKill {
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
        array(vec![bulk("SCRIPT"), bulk("KILL")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SCRIPT KILL"
    }
}

// ---------------------------------------------------------------------------
// FCALL
// ---------------------------------------------------------------------------

/// FCALL function numkeys [key ...] [arg ...]
///
/// Calls a Redis function. Returns `Frame` directly because functions can
/// produce any response type.
pub struct FCall {
    function: String,
    keys: Vec<String>,
    args: Vec<String>,
}

impl FCall {
    pub fn new(function: impl Into<String>) -> Self {
        Self {
            function: function.into(),
            keys: Vec::new(),
            args: Vec::new(),
        }
    }

    /// Add a key argument (populates KEYS table).
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Add a regular argument (populates ARGV table).
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }
}

impl Command for FCall {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![
            bulk("FCALL"),
            bulk(self.function.as_str()),
            bulk(self.keys.len().to_string()),
        ];
        for k in &self.keys {
            parts.push(bulk(k.as_str()));
        }
        for a in &self.args {
            parts.push(bulk(a.as_str()));
        }
        array(parts)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "FCALL"
    }
}

// ---------------------------------------------------------------------------
// FCALL_RO
// ---------------------------------------------------------------------------

/// FCALL_RO function numkeys [key ...] [arg ...]
///
/// Read-only variant of FCALL. Calls a Redis function without write
/// permissions. Returns `Frame` directly.
pub struct FCallRo {
    function: String,
    keys: Vec<String>,
    args: Vec<String>,
}

impl FCallRo {
    pub fn new(function: impl Into<String>) -> Self {
        Self {
            function: function.into(),
            keys: Vec::new(),
            args: Vec::new(),
        }
    }

    /// Add a key argument (populates KEYS table).
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Add a regular argument (populates ARGV table).
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }
}

impl Command for FCallRo {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![
            bulk("FCALL_RO"),
            bulk(self.function.as_str()),
            bulk(self.keys.len().to_string()),
        ];
        for k in &self.keys {
            parts.push(bulk(k.as_str()));
        }
        for a in &self.args {
            parts.push(bulk(a.as_str()));
        }
        array(parts)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "FCALL_RO"
    }
}

// ---------------------------------------------------------------------------
// FUNCTION LOAD
// ---------------------------------------------------------------------------

/// FUNCTION LOAD \[REPLACE\] function-code
///
/// Loads a library to Redis. Returns the library name.
pub struct FunctionLoad {
    code: String,
    replace: bool,
}

impl FunctionLoad {
    pub fn new(code: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            replace: false,
        }
    }

    /// Replace the existing library if it already exists.
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }
}

impl Command for FunctionLoad {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![bulk("FUNCTION"), bulk("LOAD")];
        if self.replace {
            parts.push(bulk("REPLACE"));
        }
        parts.push(bulk(self.code.as_str()));
        array(parts)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FUNCTION LOAD"
    }
}

// ---------------------------------------------------------------------------
// FUNCTION DELETE
// ---------------------------------------------------------------------------

/// FUNCTION DELETE library-name
///
/// Deletes a library and all its functions.
pub struct FunctionDelete {
    library: String,
}

impl FunctionDelete {
    pub fn new(library: impl Into<String>) -> Self {
        Self {
            library: library.into(),
        }
    }
}

impl Command for FunctionDelete {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("FUNCTION"),
            bulk("DELETE"),
            bulk(self.library.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FUNCTION DELETE"
    }
}

// ---------------------------------------------------------------------------
// FUNCTION LIST
// ---------------------------------------------------------------------------

/// FUNCTION LIST \[LIBRARYNAME library-name-pattern\] \[WITHCODE\]
///
/// Returns information about the libraries. Returns the raw `Frame` array
/// because the response is a complex nested structure.
pub struct FunctionList {
    library_pattern: Option<String>,
    withcode: bool,
}

impl FunctionList {
    pub fn new() -> Self {
        Self {
            library_pattern: None,
            withcode: false,
        }
    }

    /// Filter by library name pattern.
    pub fn library(mut self, pattern: impl Into<String>) -> Self {
        self.library_pattern = Some(pattern.into());
        self
    }

    /// Include the library source code in the response.
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
    type Response = Vec<Frame>;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![bulk("FUNCTION"), bulk("LIST")];
        if let Some(ref pattern) = self.library_pattern {
            parts.push(bulk("LIBRARYNAME"));
            parts.push(bulk(pattern.as_str()));
        }
        if self.withcode {
            parts.push(bulk("WITHCODE"));
        }
        array(parts)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => Ok(frames),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FUNCTION LIST"
    }
}

// ---------------------------------------------------------------------------
// FUNCTION DUMP
// ---------------------------------------------------------------------------

/// FUNCTION DUMP
///
/// Returns a serialized payload of all libraries. The payload can be restored
/// with FUNCTION RESTORE.
pub struct FunctionDump;

impl FunctionDump {
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
        array(vec![bulk("FUNCTION"), bulk("DUMP")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(data),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FUNCTION DUMP"
    }
}

// ---------------------------------------------------------------------------
// FUNCTION RESTORE
// ---------------------------------------------------------------------------

/// FUNCTION RESTORE serialized-value [FLUSH | APPEND | REPLACE]
///
/// Restores libraries from a serialized payload produced by FUNCTION DUMP.
pub struct FunctionRestore {
    payload: Bytes,
    policy: Option<RestorePolicy>,
}

/// Restore policy for FUNCTION RESTORE.
pub enum RestorePolicy {
    /// Delete all existing libraries before restoring.
    Flush,
    /// Append new libraries; fail if a library already exists.
    Append,
    /// Replace existing libraries with the restored ones.
    Replace,
}

impl FunctionRestore {
    pub fn new(payload: Bytes) -> Self {
        Self {
            payload,
            policy: None,
        }
    }

    /// Set the restore policy.
    pub fn policy(mut self, policy: RestorePolicy) -> Self {
        self.policy = Some(policy);
        self
    }
}

impl Command for FunctionRestore {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut parts = vec![
            bulk("FUNCTION"),
            bulk("RESTORE"),
            Frame::BulkString(Some(self.payload.clone())),
        ];
        match &self.policy {
            Some(RestorePolicy::Flush) => parts.push(bulk("FLUSH")),
            Some(RestorePolicy::Append) => parts.push(bulk("APPEND")),
            Some(RestorePolicy::Replace) => parts.push(bulk("REPLACE")),
            None => {}
        }
        array(parts)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FUNCTION RESTORE"
    }
}

// ---------------------------------------------------------------------------
// FUNCTION FLUSH
// ---------------------------------------------------------------------------

/// FUNCTION FLUSH [ASYNC | SYNC]
///
/// Deletes all libraries and functions.
pub struct FunctionFlush {
    mode: Option<FlushMode>,
}

impl FunctionFlush {
    pub fn new() -> Self {
        Self { mode: None }
    }

    /// Set the flush mode (ASYNC or SYNC).
    pub fn mode(mut self, mode: FlushMode) -> Self {
        self.mode = Some(mode);
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
        let mut parts = vec![bulk("FUNCTION"), bulk("FLUSH")];
        match &self.mode {
            Some(FlushMode::Async) => parts.push(bulk("ASYNC")),
            Some(FlushMode::Sync) => parts.push(bulk("SYNC")),
            None => {}
        }
        array(parts)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FUNCTION FLUSH"
    }
}

// ---------------------------------------------------------------------------
// FUNCTION STATS
// ---------------------------------------------------------------------------

/// FUNCTION STATS
///
/// Returns information about the function currently running and the available
/// execution engines. Returns the raw `Frame` array because the response is a
/// complex nested structure.
pub struct FunctionStats;

impl FunctionStats {
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
    type Response = Vec<Frame>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("FUNCTION"), bulk("STATS")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => Ok(frames),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FUNCTION STATS"
    }
}
