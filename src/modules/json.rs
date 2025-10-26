//! RedisJSON module - JSON document storage and manipulation
//!
//! This module provides two layers of interaction with RedisJSON:
//!
//! # Layer 1: Low-Level Commands
//! Direct 1:1 mapping to RedisJSON commands with full control over JSONPath.
//!
//! # Layer 2: Ergonomic Sugar
//! High-level Rust-native API with automatic serde serialization.
//!
//! # Examples
//!
//! ## Low-Level Usage
//! ```no_run
//! use redis_tower::modules::json::{JsonSet, JsonGet};
//! use redis_tower::RedisClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("localhost:6379").await?;
//!
//! // Set JSON at root
//! client.call(JsonSet::new("user:1", "$", r#"{"name":"Alice","age":30}"#)).await?;
//!
//! // Get specific path
//! let name: String = client.call(JsonGet::new("user:1").path("$.name")).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## High-Level Usage (with serde)
//! ```no_run
//! use redis_tower::modules::json::JsonDocument;
//! use redis_tower::RedisClient;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct User {
//!     name: String,
//!     age: u32,
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("localhost:6379").await?;
//!
//! let user = User { name: "Alice".into(), age: 30 };
//!
//! // Automatic serialization
//! JsonDocument::new(&client, "user:1")
//!     .set(&user)
//!     .await?;
//!
//! // Automatic deserialization
//! let user: User = JsonDocument::new(&client, "user:1")
//!     .get()
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

// ============================================================================
// LAYER 1: LOW-LEVEL COMMANDS
// ============================================================================

/// JSON.SET - Set JSON value at path
///
/// Sets or updates the JSON value at the specified path.
/// The path must exist except for the root path ($).
///
/// # Arguments
/// * `key` - Redis key
/// * `path` - JSONPath (use "$" for root)
/// * `value` - JSON string value
///
/// # Optional
/// * `nx` - Only set if key doesn't exist
/// * `xx` - Only set if key exists
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonSet;
///
/// // Set entire document
/// let cmd = JsonSet::new("user:1", "$", r#"{"name":"Alice","age":30}"#);
///
/// // Set specific field
/// let cmd = JsonSet::new("user:1", "$.name", r#""Bob""#);
///
/// // Conditional set
/// let cmd = JsonSet::new("user:1", "$", r#"{"name":"Alice"}"#).nx();
/// ```
#[derive(Debug, Clone)]
pub struct JsonSet {
    key: String,
    path: String,
    value: String,
    nx: bool,
    xx: bool,
}

impl JsonSet {
    /// Create a new JSON.SET command
    pub fn new(key: impl Into<String>, path: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            value: value.into(),
            nx: false,
            xx: false,
        }
    }

    /// Only set if key doesn't exist
    pub fn nx(mut self) -> Self {
        self.nx = true;
        self.xx = false;
        self
    }

    /// Only set if key exists
    pub fn xx(mut self) -> Self {
        self.xx = true;
        self.nx = false;
        self
    }
}

impl Command for JsonSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.SET"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.path.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.value.as_bytes()))),
        ];

        if self.nx {
            frames.push(Frame::BulkString(Some(Bytes::from("NX"))));
        } else if self.xx {
            frames.push(Frame::BulkString(Some(Bytes::from("XX"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::BulkString(Some(_)) => Ok(()), // "OK"
            Frame::BulkString(None) => Ok(()),    // NULL (NX/XX condition not met)
            Frame::Null => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// JSON.GET - Get JSON value at path(s)
///
/// Returns the JSON value at one or more paths in serialized form.
///
/// # Arguments
/// * `key` - Redis key
///
/// # Optional
/// * `paths` - One or more JSONPath expressions (defaults to root "$")
/// * `indent` - Pretty-print with indentation
/// * `newline` - Add newlines
/// * `space` - Add spaces around separators
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonGet;
///
/// // Get entire document
/// let cmd = JsonGet::new("user:1");
///
/// // Get specific path
/// let cmd = JsonGet::new("user:1").path("$.name");
///
/// // Get multiple paths
/// let cmd = JsonGet::new("user:1")
///     .path("$.name")
///     .path("$.age");
///
/// // Pretty print
/// let cmd = JsonGet::new("user:1").indent("  ").newline("\n");
/// ```
#[derive(Debug, Clone)]
pub struct JsonGet {
    key: String,
    paths: Vec<String>,
    indent: Option<String>,
    newline: Option<String>,
    space: Option<String>,
}

impl JsonGet {
    /// Create a new JSON.GET command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            paths: Vec::new(),
            indent: None,
            newline: None,
            space: None,
        }
    }

    /// Add a JSONPath to retrieve
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.paths.push(path.into());
        self
    }

    /// Set indentation for pretty printing
    pub fn indent(mut self, indent: impl Into<String>) -> Self {
        self.indent = Some(indent.into());
        self
    }

    /// Set newline character for pretty printing
    pub fn newline(mut self, newline: impl Into<String>) -> Self {
        self.newline = Some(newline.into());
        self
    }

    /// Set space character for pretty printing
    pub fn space(mut self, space: impl Into<String>) -> Self {
        self.space = Some(space.into());
        self
    }
}

impl Command for JsonGet {
    type Response = Option<String>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.GET"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref indent) = self.indent {
            frames.push(Frame::BulkString(Some(Bytes::from("INDENT"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                indent.as_bytes(),
            ))));
        }

        if let Some(ref newline) = self.newline {
            frames.push(Frame::BulkString(Some(Bytes::from("NEWLINE"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                newline.as_bytes(),
            ))));
        }

        if let Some(ref space) = self.space {
            frames.push(Frame::BulkString(Some(Bytes::from("SPACE"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                space.as_bytes(),
            ))));
        }

        // Add paths (default to root if none specified)
        if self.paths.is_empty() {
            frames.push(Frame::BulkString(Some(Bytes::from("$"))));
        } else {
            for path in &self.paths {
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    path.as_bytes(),
                ))));
            }
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(String::from_utf8_lossy(&data).to_string())),
            Frame::BulkString(None) => Ok(None),
            Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// JSON.DEL - Delete value at path
///
/// Deletes the JSON value at the specified path.
/// Returns the number of paths deleted.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonDel;
///
/// // Delete entire document
/// let cmd = JsonDel::new("user:1");
///
/// // Delete specific field
/// let cmd = JsonDel::new("user:1").path("$.age");
/// ```
#[derive(Debug, Clone)]
pub struct JsonDel {
    key: String,
    path: Option<String>,
}

impl JsonDel {
    /// Create a new JSON.DEL command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set path to delete (defaults to root)
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl Command for JsonDel {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.DEL"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref path) = self.path {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                path.as_bytes(),
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

/// JSON.MGET - Get JSON values from multiple keys
///
/// Returns the values at a path from multiple keys.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonMGet;
///
/// let cmd = JsonMGet::new(vec!["user:1".into(), "user:2".into()], "$.name");
/// // Response: vec![Some("\"Alice\""), Some("\"Bob\"")]
/// ```
#[derive(Debug, Clone)]
pub struct JsonMGet {
    keys: Vec<String>,
    path: String,
}

impl JsonMGet {
    /// Create a new JSON.MGET command
    pub fn new(keys: Vec<String>, path: impl Into<String>) -> Self {
        Self {
            keys,
            path: path.into(),
        }
    }
}

impl Command for JsonMGet {
    type Response = Vec<Option<String>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("JSON.MGET")))];

        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
            self.path.as_bytes(),
        ))));

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            results.push(Some(String::from_utf8_lossy(&data).to_string()));
                        }
                        Frame::BulkString(None) => results.push(None),
                        Frame::Null => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
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

/// JSON.MSET - Set JSON values for multiple keys
///
/// Sets or updates JSON values for multiple keys atomically.
/// Available in Redis 7.1+
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonMSet;
///
/// let cmd = JsonMSet::new()
///     .set("user:1", "$", r#"{"name":"Alice"}"#)
///     .set("user:2", "$", r#"{"name":"Bob"}"#);
/// ```
#[derive(Debug, Clone)]
pub struct JsonMSet {
    operations: Vec<(String, String, String)>, // (key, path, value)
}

impl JsonMSet {
    /// Create a new JSON.MSET command
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Add a set operation
    pub fn set(
        mut self,
        key: impl Into<String>,
        path: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.operations
            .push((key.into(), path.into(), value.into()));
        self
    }
}

impl Default for JsonMSet {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for JsonMSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("JSON.MSET")))];

        for (key, path, value) in &self.operations {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                path.as_bytes(),
            ))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                value.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::BulkString(Some(_)) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// ARRAY COMMANDS
// ============================================================================

/// JSON.ARRAPPEND - Append values to JSON array
///
/// Appends one or more JSON values to the array at path.
/// Returns the new array length.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonArrAppend;
///
/// let cmd = JsonArrAppend::new("doc", "$.tags")
///     .value(r#""rust""#)
///     .value(r#""redis""#);
/// // Response: vec![3] - new array length
/// ```
#[derive(Debug, Clone)]
pub struct JsonArrAppend {
    key: String,
    path: String,
    values: Vec<String>,
}

impl JsonArrAppend {
    /// Create a new JSON.ARRAPPEND command
    pub fn new(key: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            values: Vec::new(),
        }
    }

    /// Add a value to append (as JSON string)
    pub fn value(mut self, value: impl Into<String>) -> Self {
        self.values.push(value.into());
        self
    }
}

impl Command for JsonArrAppend {
    type Response = Vec<Option<i64>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.ARRAPPEND"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.path.as_bytes()))),
        ];

        for value in &self.values {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                value.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::Integer(n) => results.push(Some(n)),
                        Frame::Null => results.push(None),
                        Frame::BulkString(None) => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Integer(n) => Ok(vec![Some(n)]), // Single path result
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// Read-only trait implementations
use crate::read_preference::ReadOnly;

impl ReadOnly for JsonGet {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for JsonMGet {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands
impl ReadOnly for JsonSet {}
impl ReadOnly for JsonDel {}
impl ReadOnly for JsonMSet {}
impl ReadOnly for JsonArrAppend {}

/// JSON.ARRINDEX - Find index of value in array
///
/// Searches for the first occurrence of a JSON value in the array at path.
/// Returns -1 if not found.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonArrIndex;
///
/// let cmd = JsonArrIndex::new("doc", "$.tags", r#""rust""#);
/// // Response: vec![Some(0)] if found at index 0
///
/// // With start/stop range
/// let cmd = JsonArrIndex::new("doc", "$.tags", r#""redis""#)
///     .start(1)
///     .stop(5);
/// ```
#[derive(Debug, Clone)]
pub struct JsonArrIndex {
    key: String,
    path: String,
    value: String,
    start: Option<i64>,
    stop: Option<i64>,
}

impl JsonArrIndex {
    /// Create a new JSON.ARRINDEX command
    pub fn new(key: impl Into<String>, path: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            value: value.into(),
            start: None,
            stop: None,
        }
    }

    /// Set start index for search
    pub fn start(mut self, start: i64) -> Self {
        self.start = Some(start);
        self
    }

    /// Set stop index for search
    pub fn stop(mut self, stop: i64) -> Self {
        self.stop = Some(stop);
        self
    }
}

impl Command for JsonArrIndex {
    type Response = Vec<Option<i64>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.ARRINDEX"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.path.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.value.as_bytes()))),
        ];

        if let Some(start) = self.start {
            frames.push(Frame::BulkString(Some(Bytes::from(start.to_string()))));
        }

        if let Some(stop) = self.stop {
            frames.push(Frame::BulkString(Some(Bytes::from(stop.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::Integer(n) => results.push(Some(n)),
                        Frame::Null => results.push(None),
                        Frame::BulkString(None) => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Integer(n) => Ok(vec![Some(n)]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// JSON.ARRINSERT - Insert values into array
///
/// Inserts JSON values at the specified index in the array at path.
/// Returns the new array length.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonArrInsert;
///
/// let cmd = JsonArrInsert::new("doc", "$.tags", 1)
///     .value(r#""tower""#)
///     .value(r#""async""#);
/// // Inserts at index 1, shifting existing elements
/// ```
#[derive(Debug, Clone)]
pub struct JsonArrInsert {
    key: String,
    path: String,
    index: i64,
    values: Vec<String>,
}

impl JsonArrInsert {
    /// Create a new JSON.ARRINSERT command
    pub fn new(key: impl Into<String>, path: impl Into<String>, index: i64) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            index,
            values: Vec::new(),
        }
    }

    /// Add a value to insert
    pub fn value(mut self, value: impl Into<String>) -> Self {
        self.values.push(value.into());
        self
    }
}

impl Command for JsonArrInsert {
    type Response = Vec<Option<i64>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.ARRINSERT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.path.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.index.to_string()))),
        ];

        for value in &self.values {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                value.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::Integer(n) => results.push(Some(n)),
                        Frame::Null => results.push(None),
                        Frame::BulkString(None) => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Integer(n) => Ok(vec![Some(n)]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// JSON.ARRLEN - Get array length
///
/// Returns the length of the JSON array at path.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonArrLen;
///
/// let cmd = JsonArrLen::new("doc", "$.tags");
/// // Response: vec![Some(3)] if array has 3 elements
/// ```
#[derive(Debug, Clone)]
pub struct JsonArrLen {
    key: String,
    path: Option<String>,
}

impl JsonArrLen {
    /// Create a new JSON.ARRLEN command
    pub fn new(key: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: Some(path.into()),
        }
    }
}

impl Command for JsonArrLen {
    type Response = Vec<Option<i64>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.ARRLEN"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref path) = self.path {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                path.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::Integer(n) => results.push(Some(n)),
                        Frame::Null => results.push(None),
                        Frame::BulkString(None) => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Integer(n) => Ok(vec![Some(n)]),
            Frame::Null => Ok(vec![None]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// JSON.ARRPOP - Pop element from array
///
/// Removes and returns an element from the array at path.
/// By default pops the last element (index -1).
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonArrPop;
///
/// // Pop last element
/// let cmd = JsonArrPop::new("doc", "$.tags");
///
/// // Pop first element
/// let cmd = JsonArrPop::new("doc", "$.tags").index(0);
///
/// // Pop specific index
/// let cmd = JsonArrPop::new("doc", "$.tags").index(2);
/// ```
#[derive(Debug, Clone)]
pub struct JsonArrPop {
    key: String,
    path: Option<String>,
    index: Option<i64>,
}

impl JsonArrPop {
    /// Create a new JSON.ARRPOP command
    pub fn new(key: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: Some(path.into()),
            index: None,
        }
    }

    /// Set index to pop (default -1 for last element)
    pub fn index(mut self, index: i64) -> Self {
        self.index = Some(index);
        self
    }
}

impl Command for JsonArrPop {
    type Response = Vec<Option<String>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.ARRPOP"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref path) = self.path {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                path.as_bytes(),
            ))));
        }

        if let Some(index) = self.index {
            frames.push(Frame::BulkString(Some(Bytes::from(index.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            results.push(Some(String::from_utf8_lossy(&data).to_string()));
                        }
                        Frame::BulkString(None) => results.push(None),
                        Frame::Null => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::BulkString(Some(data)) => {
                Ok(vec![Some(String::from_utf8_lossy(&data).to_string())])
            }
            Frame::BulkString(None) => Ok(vec![None]),
            Frame::Null => Ok(vec![None]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// JSON.ARRTRIM - Trim array to range
///
/// Trims an array to contain only the specified inclusive range of elements.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonArrTrim;
///
/// // Keep only elements 0-4
/// let cmd = JsonArrTrim::new("doc", "$.tags", 0, 4);
///
/// // Keep last 10 elements
/// let cmd = JsonArrTrim::new("doc", "$.tags", -10, -1);
/// ```
#[derive(Debug, Clone)]
pub struct JsonArrTrim {
    key: String,
    path: String,
    start: i64,
    stop: i64,
}

impl JsonArrTrim {
    /// Create a new JSON.ARRTRIM command
    pub fn new(key: impl Into<String>, path: impl Into<String>, start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            start,
            stop,
        }
    }
}

impl Command for JsonArrTrim {
    type Response = Vec<Option<i64>>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("JSON.ARRTRIM"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.path.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.start.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.stop.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::Integer(n) => results.push(Some(n)),
                        Frame::Null => results.push(None),
                        Frame::BulkString(None) => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Integer(n) => Ok(vec![Some(n)]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// OBJECT COMMANDS
// ============================================================================

/// JSON.OBJKEYS - Get object keys
///
/// Returns the keys in the JSON object at path.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonObjKeys;
///
/// let cmd = JsonObjKeys::new("user:1", "$");
/// // Response: vec![Some(vec!["name", "age", "email"])]
/// ```
#[derive(Debug, Clone)]
pub struct JsonObjKeys {
    key: String,
    path: Option<String>,
}

impl JsonObjKeys {
    /// Create a new JSON.OBJKEYS command
    pub fn new(key: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: Some(path.into()),
        }
    }
}

impl Command for JsonObjKeys {
    type Response = Vec<Option<Vec<String>>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.OBJKEYS"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref path) = self.path {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                path.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(outer) => {
                let mut results = Vec::with_capacity(outer.len());
                for item in outer {
                    match item {
                        Frame::Array(inner) => {
                            let keys: Result<Vec<String>, _> = inner
                                .into_iter()
                                .map(|f| match f {
                                    Frame::BulkString(Some(data)) => {
                                        Ok(String::from_utf8_lossy(&data).to_string())
                                    }
                                    _ => Err(RedisError::UnexpectedResponse),
                                })
                                .collect();
                            results.push(Some(keys?));
                        }
                        Frame::Null => results.push(None),
                        Frame::BulkString(None) => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
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

/// JSON.OBJLEN - Get object length
///
/// Returns the number of keys in the JSON object at path.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonObjLen;
///
/// let cmd = JsonObjLen::new("user:1", "$");
/// // Response: vec![Some(3)] if object has 3 keys
/// ```
#[derive(Debug, Clone)]
pub struct JsonObjLen {
    key: String,
    path: Option<String>,
}

impl JsonObjLen {
    /// Create a new JSON.OBJLEN command
    pub fn new(key: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: Some(path.into()),
        }
    }
}

impl Command for JsonObjLen {
    type Response = Vec<Option<i64>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.OBJLEN"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref path) = self.path {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                path.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::Integer(n) => results.push(Some(n)),
                        Frame::Null => results.push(None),
                        Frame::BulkString(None) => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Integer(n) => Ok(vec![Some(n)]),
            Frame::Null => Ok(vec![None]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// NUMERIC COMMANDS
// ============================================================================

/// JSON.NUMINCRBY - Increment numeric value
///
/// Increments the numeric value at path by the given increment.
/// Returns the new value after increment.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonNumIncrBy;
///
/// let cmd = JsonNumIncrBy::new("stats", "$.count", 5.0);
/// // Response: vec![Some("10")] if previous value was 5
/// ```
#[derive(Debug, Clone)]
pub struct JsonNumIncrBy {
    key: String,
    path: String,
    value: f64,
}

impl JsonNumIncrBy {
    /// Create a new JSON.NUMINCRBY command
    pub fn new(key: impl Into<String>, path: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            value,
        }
    }
}

impl Command for JsonNumIncrBy {
    type Response = Vec<Option<String>>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("JSON.NUMINCRBY"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.path.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.value.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            results.push(Some(String::from_utf8_lossy(&data).to_string()));
                        }
                        Frame::BulkString(None) => results.push(None),
                        Frame::Null => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::BulkString(Some(data)) => {
                Ok(vec![Some(String::from_utf8_lossy(&data).to_string())])
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// JSON.NUMMULTBY - Multiply numeric value
///
/// Multiplies the numeric value at path by the given multiplier.
/// Returns the new value after multiplication.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonNumMultBy;
///
/// let cmd = JsonNumMultBy::new("stats", "$.score", 2.0);
/// // Response: vec![Some("20")] if previous value was 10
/// ```
#[derive(Debug, Clone)]
pub struct JsonNumMultBy {
    key: String,
    path: String,
    value: f64,
}

impl JsonNumMultBy {
    /// Create a new JSON.NUMMULTBY command
    pub fn new(key: impl Into<String>, path: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            value,
        }
    }
}

impl Command for JsonNumMultBy {
    type Response = Vec<Option<String>>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("JSON.NUMMULTBY"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.path.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.value.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            results.push(Some(String::from_utf8_lossy(&data).to_string()));
                        }
                        Frame::BulkString(None) => results.push(None),
                        Frame::Null => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::BulkString(Some(data)) => {
                Ok(vec![Some(String::from_utf8_lossy(&data).to_string())])
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// STRING COMMANDS
// ============================================================================

/// JSON.STRAPPEND - Append to string value
///
/// Appends a string to the string value at path.
/// Returns the new string length.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonStrAppend;
///
/// let cmd = JsonStrAppend::new("doc", "$.name", r#"" Jr.""#);
/// // Appends " Jr." to existing string
/// ```
#[derive(Debug, Clone)]
pub struct JsonStrAppend {
    key: String,
    path: Option<String>,
    value: String,
}

impl JsonStrAppend {
    /// Create a new JSON.STRAPPEND command
    pub fn new(key: impl Into<String>, path: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: Some(path.into()),
            value: value.into(),
        }
    }
}

impl Command for JsonStrAppend {
    type Response = Vec<Option<i64>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.STRAPPEND"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref path) = self.path {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                path.as_bytes(),
            ))));
        }

        frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
            self.value.as_bytes(),
        ))));

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::Integer(n) => results.push(Some(n)),
                        Frame::Null => results.push(None),
                        Frame::BulkString(None) => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Integer(n) => Ok(vec![Some(n)]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// JSON.STRLEN - Get string length
///
/// Returns the length of the JSON string at path.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonStrLen;
///
/// let cmd = JsonStrLen::new("doc", "$.name");
/// // Response: vec![Some(5)] if string is "Alice"
/// ```
#[derive(Debug, Clone)]
pub struct JsonStrLen {
    key: String,
    path: Option<String>,
}

impl JsonStrLen {
    /// Create a new JSON.STRLEN command
    pub fn new(key: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: Some(path.into()),
        }
    }
}

impl Command for JsonStrLen {
    type Response = Vec<Option<i64>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.STRLEN"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref path) = self.path {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                path.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::Integer(n) => results.push(Some(n)),
                        Frame::Null => results.push(None),
                        Frame::BulkString(None) => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Integer(n) => Ok(vec![Some(n)]),
            Frame::Null => Ok(vec![None]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// UTILITY COMMANDS
// ============================================================================

/// JSON.CLEAR - Clear container values
///
/// Clears container values (arrays/objects) or zeros numeric values at path.
/// Returns the number of paths cleared.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonClear;
///
/// let cmd = JsonClear::new("doc", "$.tags");
/// // Clears array to [] or object to {}
/// ```
#[derive(Debug, Clone)]
pub struct JsonClear {
    key: String,
    path: Option<String>,
}

impl JsonClear {
    /// Create a new JSON.CLEAR command
    pub fn new(key: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: Some(path.into()),
        }
    }
}

impl Command for JsonClear {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.CLEAR"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref path) = self.path {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                path.as_bytes(),
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

/// JSON.TOGGLE - Toggle boolean value
///
/// Toggles a boolean value (true becomes false, false becomes true).
/// Returns the new boolean value.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonToggle;
///
/// let cmd = JsonToggle::new("doc", "$.active");
/// // Response: vec![Some(true)] if was false before
/// ```
#[derive(Debug, Clone)]
pub struct JsonToggle {
    key: String,
    path: String,
}

impl JsonToggle {
    /// Create a new JSON.TOGGLE command
    pub fn new(key: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
        }
    }
}

impl Command for JsonToggle {
    type Response = Vec<Option<bool>>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("JSON.TOGGLE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.path.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::Integer(1) => results.push(Some(true)),
                        Frame::Integer(0) => results.push(Some(false)),
                        Frame::Null => results.push(None),
                        Frame::BulkString(None) => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Integer(1) => Ok(vec![Some(true)]),
            Frame::Integer(0) => Ok(vec![Some(false)]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// JSON.TYPE - Get value type
///
/// Returns the type of the JSON value at path.
/// Types: null, boolean, integer, number, string, object, array
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonType;
///
/// let cmd = JsonType::new("doc", "$.name");
/// // Response: vec![Some("string")]
/// ```
#[derive(Debug, Clone)]
pub struct JsonType {
    key: String,
    path: Option<String>,
}

impl JsonType {
    /// Create a new JSON.TYPE command
    pub fn new(key: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: Some(path.into()),
        }
    }
}

impl Command for JsonType {
    type Response = Vec<Option<String>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.TYPE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref path) = self.path {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                path.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            results.push(Some(String::from_utf8_lossy(&data).to_string()));
                        }
                        Frame::BulkString(None) => results.push(None),
                        Frame::Null => results.push(None),
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::BulkString(Some(data)) => {
                Ok(vec![Some(String::from_utf8_lossy(&data).to_string())])
            }
            Frame::BulkString(None) => Ok(vec![None]),
            Frame::Null => Ok(vec![None]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// JSON.MERGE - Merge JSON values
///
/// Merges a JSON value into the existing value at path.
/// Available in Redis 7.1+
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonMerge;
///
/// let cmd = JsonMerge::new("doc", "$", r#"{"age":31}"#);
/// // Merges age into existing object
/// ```
#[derive(Debug, Clone)]
pub struct JsonMerge {
    key: String,
    path: String,
    value: String,
}

impl JsonMerge {
    /// Create a new JSON.MERGE command
    pub fn new(key: impl Into<String>, path: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            value: value.into(),
        }
    }
}

impl Command for JsonMerge {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("JSON.MERGE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.path.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.value.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::BulkString(Some(_)) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ReadOnly trait implementations for new commands
impl ReadOnly for JsonArrIndex {
    fn is_read_only(&self) -> bool {
        true
    }
}
impl ReadOnly for JsonArrLen {
    fn is_read_only(&self) -> bool {
        true
    }
}
impl ReadOnly for JsonObjKeys {
    fn is_read_only(&self) -> bool {
        true
    }
}
impl ReadOnly for JsonObjLen {
    fn is_read_only(&self) -> bool {
        true
    }
}
impl ReadOnly for JsonStrLen {
    fn is_read_only(&self) -> bool {
        true
    }
}
impl ReadOnly for JsonType {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands
impl ReadOnly for JsonArrInsert {}
impl ReadOnly for JsonArrPop {}
impl ReadOnly for JsonArrTrim {}
impl ReadOnly for JsonNumIncrBy {}
impl ReadOnly for JsonNumMultBy {}
impl ReadOnly for JsonStrAppend {}
impl ReadOnly for JsonClear {}
impl ReadOnly for JsonToggle {}
impl ReadOnly for JsonMerge {}

/// JSON.FORGET - Delete value at path (alias for JSON.DEL)
///
/// This is an alias for JSON.DEL. Deletes the JSON value at the specified path.
/// Returns the number of paths deleted.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonForget;
///
/// // Delete entire document
/// let cmd = JsonForget::new("user:1");
///
/// // Delete specific field
/// let cmd = JsonForget::new("user:1").path("$.age");
/// ```
#[derive(Debug, Clone)]
pub struct JsonForget {
    key: String,
    path: Option<String>,
}

impl JsonForget {
    /// Create a new JSON.FORGET command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set path to delete (defaults to root)
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl Command for JsonForget {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.FORGET"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref path) = self.path {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                path.as_bytes(),
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

/// JSON.RESP - Return JSON value in RESP format
///
/// Returns the JSON value at path encoded as a RESP value.
/// This is useful for converting JSON to Redis native data structures.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::JsonResp;
///
/// let cmd = JsonResp::new("doc", "$");
/// // Returns JSON as RESP array/map/string/integer based on type
/// ```
#[derive(Debug, Clone)]
pub struct JsonResp {
    key: String,
    path: Option<String>,
}

impl JsonResp {
    /// Create a new JSON.RESP command
    pub fn new(key: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: Some(path.into()),
        }
    }
}

impl Command for JsonResp {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.RESP"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref path) = self.path {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                path.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        // JSON.RESP returns a RESP-encoded representation of the JSON value
        // We return the raw Frame so users can interpret it as needed
        match frame {
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Ok(frame),
        }
    }
}

/// Subcommands for JSON.DEBUG
#[derive(Debug, Clone)]
pub enum JsonDebugSubcommand {
    /// Get memory usage in bytes
    Memory,
    /// Get help information
    Help,
}

/// JSON.DEBUG - Debugging commands
///
/// Provides debugging information about JSON values.
/// Primary use case is JSON.DEBUG MEMORY to get memory usage.
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::json::{JsonDebug, JsonDebugSubcommand};
///
/// // Get memory usage
/// let cmd = JsonDebug::new("doc", "$", JsonDebugSubcommand::Memory);
/// // Response: Vec<Option<i64>> - memory in bytes for each path match
///
/// // Get help
/// let cmd = JsonDebug::new("doc", "$", JsonDebugSubcommand::Help);
/// ```
#[derive(Debug, Clone)]
pub struct JsonDebug {
    key: String,
    path: Option<String>,
    subcommand: JsonDebugSubcommand,
}

impl JsonDebug {
    /// Create a new JSON.DEBUG command
    pub fn new(
        key: impl Into<String>,
        path: impl Into<String>,
        subcommand: JsonDebugSubcommand,
    ) -> Self {
        Self {
            key: key.into(),
            path: Some(path.into()),
            subcommand,
        }
    }

    /// Create a JSON.DEBUG MEMORY command (most common usage)
    pub fn memory(key: impl Into<String>, path: impl Into<String>) -> Self {
        Self::new(key, path, JsonDebugSubcommand::Memory)
    }

    /// Create a JSON.DEBUG HELP command
    pub fn help(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: None,
            subcommand: JsonDebugSubcommand::Help,
        }
    }
}

impl Command for JsonDebug {
    type Response = Vec<Option<i64>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("JSON.DEBUG"))),
            Frame::BulkString(Some(match self.subcommand {
                JsonDebugSubcommand::Memory => Bytes::from("MEMORY"),
                JsonDebugSubcommand::Help => Bytes::from("HELP"),
            })),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(ref path) = self.path {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                path.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::Integer(n) => results.push(Some(n)),
                        Frame::Null => results.push(None),
                        Frame::BulkString(None) => results.push(None),
                        Frame::BulkString(Some(_)) => {
                            // HELP returns strings, we'll just skip them
                            continue;
                        }
                        Frame::Error(e) => {
                            return Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e)));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Integer(n) => Ok(vec![Some(n)]),
            Frame::Null => Ok(vec![None]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ReadOnly trait implementations for new commands
impl ReadOnly for JsonForget {}
impl ReadOnly for JsonResp {
    fn is_read_only(&self) -> bool {
        true
    }
}
impl ReadOnly for JsonDebug {
    fn is_read_only(&self) -> bool {
        true
    }
}

// ============================================================================
// LAYER 2: ERGONOMIC SUGAR (TO BE IMPLEMENTED)
// ============================================================================

// TODO: Implement high-level serde-based API
// - JsonDocument<T> wrapper
// - Automatic serialization/deserialization
// - Type-safe builders
// - JSONPath helpers

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_set_basic() {
        let cmd = JsonSet::new("user:1", "$", r#"{"name":"Alice"}"#);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("JSON.SET"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("user:1"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("$"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_set_nx() {
        let cmd = JsonSet::new("user:1", "$", r#"{"name":"Alice"}"#).nx();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("NX"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_get_basic() {
        let cmd = JsonGet::new("user:1");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("JSON.GET"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("$")))); // Default root path
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_get_with_path() {
        let cmd = JsonGet::new("user:1").path("$.name");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("$.name"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_get_multiple_paths() {
        let cmd = JsonGet::new("user:1").path("$.name").path("$.age");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4); // JSON.GET + key + 2 paths
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_del() {
        let cmd = JsonDel::new("user:1").path("$.age");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("JSON.DEL"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_mget() {
        let cmd = JsonMGet::new(vec!["user:1".into(), "user:2".into()], "$.name");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4); // JSON.MGET + 2 keys + path
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("JSON.MGET"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_mset() {
        let cmd = JsonMSet::new()
            .set("user:1", "$", r#"{"name":"Alice"}"#)
            .set("user:2", "$", r#"{"name":"Bob"}"#);

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 7); // JSON.MSET + 2*(key+path+value)
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("JSON.MSET"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_arrappend() {
        let cmd = JsonArrAppend::new("doc", "$.tags")
            .value(r#""rust""#)
            .value(r#""redis""#);

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5); // JSON.ARRAPPEND + key + path + 2 values
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.ARRAPPEND")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_get_response() {
        let frame = Frame::BulkString(Some(Bytes::from(r#"{"name":"Alice"}"#)));
        let result = JsonGet::parse_response(frame).unwrap();
        assert_eq!(result, Some(r#"{"name":"Alice"}"#.to_string()));
    }

    #[test]
    fn test_json_del_response() {
        let frame = Frame::Integer(1);
        let result = JsonDel::parse_response(frame).unwrap();
        assert_eq!(result, 1);
    }

    // Array commands tests
    #[test]
    fn test_json_arrindex() {
        let cmd = JsonArrIndex::new("doc", "$.tags", r#""rust""#);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.ARRINDEX")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("doc"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("$.tags"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from(r#""rust""#))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_arrindex_with_range() {
        let cmd = JsonArrIndex::new("doc", "$.tags", r#""rust""#)
            .start(1)
            .stop(5);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 6); // +start +stop
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_arrinsert() {
        let cmd = JsonArrInsert::new("doc", "$.tags", 1)
            .value(r#""tower""#)
            .value(r#""async""#);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 6); // cmd + key + path + index + 2 values
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.ARRINSERT")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_arrlen() {
        let cmd = JsonArrLen::new("doc", "$.tags");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.ARRLEN")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_arrpop() {
        let cmd = JsonArrPop::new("doc", "$.tags").index(0);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4); // cmd + key + path + index
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.ARRPOP")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_arrtrim() {
        let cmd = JsonArrTrim::new("doc", "$.tags", 0, 4);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5); // cmd + key + path + start + stop
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.ARRTRIM")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    // Object commands tests
    #[test]
    fn test_json_objkeys() {
        let cmd = JsonObjKeys::new("user:1", "$");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.OBJKEYS")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_objlen() {
        let cmd = JsonObjLen::new("user:1", "$");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.OBJLEN")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    // Numeric commands tests
    #[test]
    fn test_json_numincrby() {
        let cmd = JsonNumIncrBy::new("stats", "$.count", 5.0);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.NUMINCRBY")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_nummultby() {
        let cmd = JsonNumMultBy::new("stats", "$.score", 2.0);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.NUMMULTBY")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    // String commands tests
    #[test]
    fn test_json_strappend() {
        let cmd = JsonStrAppend::new("doc", "$.name", r#"" Jr.""#);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.STRAPPEND")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_strlen() {
        let cmd = JsonStrLen::new("doc", "$.name");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.STRLEN")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    // Utility commands tests
    #[test]
    fn test_json_clear() {
        let cmd = JsonClear::new("doc", "$.tags");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("JSON.CLEAR"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_toggle() {
        let cmd = JsonToggle::new("doc", "$.active");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.TOGGLE")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_type() {
        let cmd = JsonType::new("doc", "$.name");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("JSON.TYPE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_merge() {
        let cmd = JsonMerge::new("doc", "$", r#"{"age":31}"#);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("JSON.MERGE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    // Response parsing tests
    #[test]
    fn test_json_arrindex_response() {
        let frame = Frame::Array(vec![Frame::Integer(2)]);
        let result = JsonArrIndex::parse_response(frame).unwrap();
        assert_eq!(result, vec![Some(2)]);
    }

    #[test]
    fn test_json_objkeys_response() {
        let frame = Frame::Array(vec![Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("name"))),
            Frame::BulkString(Some(Bytes::from("age"))),
        ])]);
        let result = JsonObjKeys::parse_response(frame).unwrap();
        assert_eq!(
            result,
            vec![Some(vec!["name".to_string(), "age".to_string()])]
        );
    }

    #[test]
    fn test_json_toggle_response() {
        let frame = Frame::Array(vec![Frame::Integer(1)]);
        let result = JsonToggle::parse_response(frame).unwrap();
        assert_eq!(result, vec![Some(true)]);

        let frame = Frame::Array(vec![Frame::Integer(0)]);
        let result = JsonToggle::parse_response(frame).unwrap();
        assert_eq!(result, vec![Some(false)]);
    }

    #[test]
    fn test_json_type_response() {
        let frame = Frame::Array(vec![Frame::BulkString(Some(Bytes::from("string")))]);
        let result = JsonType::parse_response(frame).unwrap();
        assert_eq!(result, vec![Some("string".to_string())]);
    }

    // New commands tests
    #[test]
    fn test_json_forget() {
        let cmd = JsonForget::new("user:1").path("$.age");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("JSON.FORGET")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("user:1"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("$.age"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_forget_response() {
        let frame = Frame::Integer(1);
        let result = JsonForget::parse_response(frame).unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_json_resp() {
        let cmd = JsonResp::new("doc", "$");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("JSON.RESP"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("doc"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("$"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_resp_response() {
        // JSON.RESP returns the raw frame
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("name"))),
            Frame::BulkString(Some(Bytes::from("Alice"))),
        ]);
        let result = JsonResp::parse_response(frame.clone()).unwrap();
        assert_eq!(result, frame);
    }

    #[test]
    fn test_json_debug_memory() {
        let cmd = JsonDebug::memory("doc", "$");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("JSON.DEBUG"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("MEMORY"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("doc"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("$"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_debug_help() {
        let cmd = JsonDebug::help("doc");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("JSON.DEBUG"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("HELP"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_json_debug_memory_response() {
        let frame = Frame::Array(vec![Frame::Integer(1024)]);
        let result = JsonDebug::parse_response(frame).unwrap();
        assert_eq!(result, vec![Some(1024)]);
    }

    #[test]
    fn test_json_debug_subcommand_enum() {
        let cmd = JsonDebug::new("doc", "$", JsonDebugSubcommand::Memory);
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("MEMORY"))));
            }
            _ => panic!("Expected Array frame"),
        }

        let cmd = JsonDebug::new("doc", "$", JsonDebugSubcommand::Help);
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("HELP"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }
}

/// JSON.DEBUG HELP command - Get help text for JSON.DEBUG subcommands
///
/// Available since RedisJSON 1.0.0.
#[derive(Debug, Clone, Copy)]
pub struct JsonDebugHelp;

impl JsonDebugHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for JsonDebugHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::commands::Command for JsonDebugHelp {
    type Response = Vec<String>;

    fn to_frame(&self) -> crate::codec::Frame {
        crate::codec::Frame::Array(vec![
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from("JSON.DEBUG"))),
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
