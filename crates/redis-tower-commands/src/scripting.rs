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
#[derive(Clone)]
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
#[derive(Clone)]
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
// EVAL_RO
// ---------------------------------------------------------------------------

/// EVAL_RO script numkeys [key ...] [arg ...]
///
/// Read-only variant of EVAL. Evaluates a Lua script server-side, rejecting any
/// write commands. Returns `Frame` directly.
#[derive(Clone)]
pub struct EvalRo {
    script: String,
    keys: Vec<String>,
    args: Vec<String>,
}

impl EvalRo {
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

impl Command for EvalRo {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![
            bulk("EVAL_RO"),
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
        "EVAL_RO"
    }
}

// ---------------------------------------------------------------------------
// EVALSHA_RO
// ---------------------------------------------------------------------------

/// EVALSHA_RO sha1 numkeys [key ...] [arg ...]
///
/// Read-only variant of EVALSHA. Evaluates a cached Lua script by its SHA1
/// digest, rejecting any write commands. Returns `Frame` directly.
#[derive(Clone)]
pub struct EvalShaRo {
    sha1: String,
    keys: Vec<String>,
    args: Vec<String>,
}

impl EvalShaRo {
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

impl Command for EvalShaRo {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![
            bulk("EVALSHA_RO"),
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
        "EVALSHA_RO"
    }
}

// ---------------------------------------------------------------------------
// SCRIPT LOAD
// ---------------------------------------------------------------------------

/// SCRIPT LOAD script
///
/// Loads a Lua script into the script cache without executing it. Returns the
/// SHA1 digest of the script.
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
pub struct FunctionRestore {
    payload: Bytes,
    policy: Option<RestorePolicy>,
}

/// Restore policy for FUNCTION RESTORE.
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    // -- Eval --

    #[test]
    fn eval_simple_to_frame() {
        let cmd = Eval::new("return 1");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("EVAL"), bulk("return 1"), bulk("0")])
        );
    }

    #[test]
    fn eval_with_keys_and_args_to_frame() {
        let cmd = Eval::new("return redis.call('GET', KEYS[1])")
            .key("mykey")
            .arg("extra");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("EVAL"),
                bulk("return redis.call('GET', KEYS[1])"),
                bulk("1"),
                bulk("mykey"),
                bulk("extra"),
            ])
        );
    }

    #[test]
    fn eval_parse_response_passthrough() {
        let cmd = Eval::new("return 42");
        let frame = Frame::Integer(42);
        assert_eq!(cmd.parse_response(frame).unwrap(), Frame::Integer(42));
    }

    // -- EvalSha --

    #[test]
    fn evalsha_to_frame() {
        let cmd = EvalSha::new("abc123").key("k1").key("k2").arg("a1");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("EVALSHA"),
                bulk("abc123"),
                bulk("2"),
                bulk("k1"),
                bulk("k2"),
                bulk("a1"),
            ])
        );
    }

    #[test]
    fn evalsha_parse_response() {
        let cmd = EvalSha::new("abc123");
        let frame = Frame::SimpleString(Bytes::from("OK"));
        assert_eq!(
            cmd.parse_response(frame).unwrap(),
            Frame::SimpleString(Bytes::from("OK"))
        );
    }

    // -- FCall --

    #[test]
    fn fcall_to_frame() {
        let cmd = FCall::new("myfunc").key("k1").arg("a1").arg("a2");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FCALL"),
                bulk("myfunc"),
                bulk("1"),
                bulk("k1"),
                bulk("a1"),
                bulk("a2"),
            ])
        );
    }

    #[test]
    fn fcall_no_keys_to_frame() {
        let cmd = FCall::new("noop");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("FCALL"), bulk("noop"), bulk("0")])
        );
    }

    #[test]
    fn fcall_parse_response() {
        let cmd = FCall::new("myfunc");
        let frame = Frame::BulkString(Some(Bytes::from("result")));
        assert_eq!(
            cmd.parse_response(frame).unwrap(),
            Frame::BulkString(Some(Bytes::from("result")))
        );
    }

    // -- FCallRo --

    #[test]
    fn fcall_ro_to_frame() {
        let cmd = FCallRo::new("readonly_fn").key("k1");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FCALL_RO"),
                bulk("readonly_fn"),
                bulk("1"),
                bulk("k1")
            ])
        );
    }

    // -- EvalRo --

    #[test]
    fn eval_ro_to_frame() {
        let cmd = EvalRo::new("return redis.call('GET', KEYS[1])").key("mykey");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("EVAL_RO"),
                bulk("return redis.call('GET', KEYS[1])"),
                bulk("1"),
                bulk("mykey"),
            ])
        );
    }

    #[test]
    fn eval_ro_parse_response() {
        let cmd = EvalRo::new("return 1");
        assert_eq!(
            cmd.parse_response(Frame::Integer(1)).unwrap(),
            Frame::Integer(1)
        );
    }

    // -- EvalShaRo --

    #[test]
    fn evalsha_ro_to_frame() {
        let cmd = EvalShaRo::new("abc123").key("k1").arg("a1");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("EVALSHA_RO"),
                bulk("abc123"),
                bulk("1"),
                bulk("k1"),
                bulk("a1"),
            ])
        );
    }

    // -- ScriptLoad --

    #[test]
    fn script_load_to_frame() {
        let cmd = ScriptLoad::new("return 1");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("SCRIPT"), bulk("LOAD"), bulk("return 1")])
        );
    }

    #[test]
    fn script_load_parse_response() {
        let cmd = ScriptLoad::new("return 1");
        let frame = Frame::BulkString(Some(Bytes::from(
            "e0e1f9fabfc9d4800c877a703b823ac0578ff831",
        )));
        assert_eq!(
            cmd.parse_response(frame).unwrap(),
            "e0e1f9fabfc9d4800c877a703b823ac0578ff831"
        );
    }

    // -- ScriptExists --

    #[test]
    fn script_exists_to_frame() {
        let cmd = ScriptExists::new("sha1").sha1("sha2");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("SCRIPT"),
                bulk("EXISTS"),
                bulk("sha1"),
                bulk("sha2")
            ])
        );
    }

    #[test]
    fn script_exists_parse_response() {
        let cmd = ScriptExists::new("sha1").sha1("sha2");
        let frame = Frame::Array(Some(vec![Frame::Integer(1), Frame::Integer(0)]));
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![true, false]);
    }

    // -- ScriptFlush --

    #[test]
    fn script_flush_to_frame() {
        let cmd = ScriptFlush::new().mode(FlushMode::Async);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("SCRIPT"), bulk("FLUSH"), bulk("ASYNC")])
        );
    }

    // -- FunctionLoad --

    #[test]
    fn function_load_to_frame() {
        let cmd = FunctionLoad::new(
            "#!lua name=mylib\nredis.register_function('myfunc', function() return 1 end)",
        )
        .replace();
        let frame = cmd.to_frame();
        if let Frame::Array(Some(parts)) = &frame {
            assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("FUNCTION"))));
            assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("LOAD"))));
            assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("REPLACE"))));
            assert_eq!(parts.len(), 4);
        } else {
            panic!("expected array frame");
        }
    }

    // -- FunctionDelete --

    #[test]
    fn function_delete_to_frame() {
        let cmd = FunctionDelete::new("mylib");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("FUNCTION"), bulk("DELETE"), bulk("mylib")])
        );
    }

    #[test]
    fn function_delete_parse_ok() {
        let cmd = FunctionDelete::new("mylib");
        let frame = Frame::SimpleString(Bytes::from("OK"));
        cmd.parse_response(frame).unwrap();
    }
}
