//! RedisJSON document commands.
//!
//! Available with the `json` feature (included by `stack`) and requiring
//! RedisJSON on the server. JSON paths and serialized JSON values are textual
//! grammar; Redis keys remain binary-safe. Commands return typed scalars,
//! optional bytes, or raw JSON text according to the selected operation/path.
//!
//! ```
//! use redis_tower_commands::{JsonGet, JsonSet};
//! use redis_tower_core::Command;
//!
//! let set = JsonSet::new("user:1", "$", r#"{"name":"Ada"}"#);
//! let get = JsonGet::new("user:1").path("$.name");
//! assert_eq!((set.name(), get.name()), ("JSON.SET", "JSON.GET"));
//! ```
//!
//! See the [command cookbook] for module reply normalization.
//!
//! [command cookbook]: https://github.com/joshrotenberg/redis-tower/blob/main/docs/COMMAND-COOKBOOK.md#raw-custom-and-module-replies

use crate::{CommandArg, SetStatus};
use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// Condition for JSON.SET (NX or XX).
#[derive(Clone)]
pub enum JsonSetCondition {
    /// Only set if the path does not exist.
    Nx,
    /// Only set if the path already exists.
    Xx,
}

/// JSON.SET key path value \[NX|XX\]
///
/// Sets the JSON value at `path` in the key. Creates the key if it does not
/// exist. Returns `Ok(())` on success.
///
/// Redis replies with null instead of `OK` when it leaves the document
/// unchanged: a [`JsonSet::nx`] or [`JsonSet::xx`] condition failed, or, with
/// or without a condition, the parent of `path` does not exist (setting
/// `$.missing.child` on `{}`). This command returns `Ok(())` either way.
/// Finish the builder with [`JsonSet::with_outcome`] to learn whether the
/// write was applied.
#[derive(Clone)]
pub struct JsonSet {
    key: CommandArg,
    path: String,
    value: String,
    condition: Option<JsonSetCondition>,
}

impl JsonSet {
    /// Create a new [`JsonSet`] command.
    pub fn new(
        key: impl Into<CommandArg>,
        path: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            value: value.into(),
            condition: None,
        }
    }

    /// Only set if the path does not already exist.
    pub fn nx(mut self) -> Self {
        self.condition = Some(JsonSetCondition::Nx);
        self
    }

    /// Only set if the path already exists.
    pub fn xx(mut self) -> Self {
        self.condition = Some(JsonSetCondition::Xx);
        self
    }

    /// Report whether Redis applied the write.
    ///
    /// The response is [`SetStatus::NotApplied`] when Redis replied with null
    /// (an `NX` or `XX` condition failed, or the parent of `path` does not
    /// exist) and [`SetStatus::Applied`] for `OK`, in both RESP2 and RESP3:
    ///
    /// ```
    /// use redis_tower_commands::JsonSet;
    /// use redis_tower_core::Command;
    ///
    /// let command = JsonSet::new("doc", "$.owner", r#""worker-1""#).nx().with_outcome();
    /// assert_eq!(command.name(), "JSON.SET");
    /// ```
    pub fn with_outcome(self) -> JsonSetWithOutcome {
        JsonSetWithOutcome { command: self }
    }
}

/// A [`JsonSet`] command whose response is a [`SetStatus`].
///
/// Create this command by calling [`JsonSet::with_outcome`] after configuring
/// the condition.
#[derive(Clone)]
pub struct JsonSetWithOutcome {
    command: JsonSet,
}

impl Command for JsonSetWithOutcome {
    type Response = SetStatus;

    fn to_frame(&self) -> Frame {
        self.command.to_frame()
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(SetStatus::Applied),
            // Unmet NX/XX condition or missing parent path: `$-1` in RESP2,
            // `_` in RESP3.
            Frame::Null | Frame::BulkString(None) => Ok(SetStatus::NotApplied),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "JSON.SET"
    }
}

impl Command for JsonSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("JSON.SET"),
            bulk(&self.key),
            bulk(self.path.as_str()),
            bulk(self.value.as_str()),
        ];
        match &self.condition {
            Some(JsonSetCondition::Nx) => args.push(bulk("NX")),
            Some(JsonSetCondition::Xx) => args.push(bulk("XX")),
            None => {}
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            // Unmet NX/XX condition or missing parent path: `$-1` in RESP2,
            // `_` in RESP3.
            Frame::Null | Frame::BulkString(None) => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "JSON.SET"
    }
}

/// JSON.GET key \[path ...\]
///
/// Returns the JSON value at one or more paths. When multiple paths are given,
/// returns a JSON object mapping each path to its value.
#[derive(Clone)]
pub struct JsonGet {
    key: CommandArg,
    paths: Vec<String>,
}

impl JsonGet {
    /// Create a new [`JsonGet`] command.
    pub fn new(key: impl Into<CommandArg>) -> Self {
        Self {
            key: key.into(),
            paths: Vec::new(),
        }
    }

    /// Add a path to retrieve.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.paths.push(path.into());
        self
    }

    /// Add multiple paths to retrieve.
    pub fn paths(mut self, paths: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.paths.extend(paths.into_iter().map(Into::into));
        self
    }
}

impl Command for JsonGet {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.GET"), bulk(&self.key)];
        for path in &self.paths {
            args.push(bulk(path.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(data) => Ok(data),
            Frame::Null => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "JSON.GET"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// JSON.DEL key \[path\]
///
/// Deletes a value at `path` in the JSON document stored at `key`. Returns
/// the number of paths deleted.
#[derive(Clone)]
pub struct JsonDel {
    key: CommandArg,
    path: Option<String>,
}

impl JsonDel {
    /// Create a new [`JsonDel`] command.
    pub fn new(key: impl Into<CommandArg>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set the path to delete. If omitted, the entire key is deleted.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl Command for JsonDel {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.DEL"), bulk(&self.key)];
        if let Some(path) = &self.path {
            args.push(bulk(path.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "JSON.DEL"
    }
}

/// JSON.MGET key \[key ...\] path
///
/// Returns the values at `path` from multiple keys. Returns `None` for keys
/// where the path does not exist.
#[derive(Clone)]
pub struct JsonMGet {
    keys: Vec<CommandArg>,
    path: String,
}

impl JsonMGet {
    /// Create a new [`JsonMGet`] command.
    pub fn new(
        keys: impl IntoIterator<Item = impl Into<CommandArg>>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            path: path.into(),
        }
    }
}

impl Command for JsonMGet {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.MGET")];
        for key in &self.keys {
            args.push(bulk(key));
        }
        args.push(bulk(self.path.as_str()));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(data) => Ok(data),
                    Frame::Null => Ok(None),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string or null",
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
        "JSON.MGET"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// JSON.TYPE key \[path\]
///
/// Returns the type of the JSON value at `path`.
///
/// Legacy paths return a single type directly. JSONPath expressions return a
/// match array; this command returns the first matched type, or `None` when the
/// key or path has no matches.
#[derive(Clone)]
pub struct JsonType {
    key: CommandArg,
    path: Option<String>,
}

impl JsonType {
    /// Create a new [`JsonType`] command.
    pub fn new(key: impl Into<CommandArg>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set the path to query. If omitted, returns the type of the root.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl Command for JsonType {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.TYPE"), bulk(&self.key)];
        if let Some(path) = &self.path {
            args.push(bulk(path.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        let mut current = frame;
        loop {
            match current {
                Frame::BulkString(data) => return Ok(data),
                Frame::SimpleString(data) => return Ok(Some(data)),
                Frame::Null | Frame::Array(None) => return Ok(None),
                Frame::Array(Some(frames)) => match frames.into_iter().next() {
                    Some(first) => current = first,
                    None => return Ok(None),
                },
                other => {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "string, null, or JSONPath match array",
                        actual: format!("{other:?}"),
                    });
                }
            }
        }
    }

    fn name(&self) -> &str {
        "JSON.TYPE"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// JSON.NUMINCRBY key path value
///
/// Increments the numeric value at `path` by `value`. Returns the new value
/// as JSON text: a JSON array with one entry per match for a `$` JSONPath
/// (`null` for a match that is not a number), or, for a legacy path, the bare
/// result of the last match that was a number. A legacy path with no numeric
/// match is an error.
///
/// RESP2 replies with that text directly. RESP3 replies with typed values,
/// which are rendered into the same text, numbers formatted as RedisJSON
/// formats them, so the response does not depend on the negotiated protocol.
#[derive(Clone)]
pub struct JsonNumIncrBy {
    key: CommandArg,
    path: String,
    value: f64,
}

impl JsonNumIncrBy {
    /// Create a new [`JsonNumIncrBy`] command.
    pub fn new(key: impl Into<CommandArg>, path: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            value,
        }
    }
}

impl Command for JsonNumIncrBy {
    type Response = Bytes;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("JSON.NUMINCRBY"),
            bulk(&self.key),
            bulk(self.path.as_str()),
            bulk(self.value.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // RESP2: the new value(s) as JSON text.
            Frame::BulkString(Some(data)) => Ok(data),
            // RESP3: one typed value per match, for `$` JSONPaths.
            Frame::Array(Some(values)) if self.path.starts_with('$') => {
                let rendered = values
                    .iter()
                    .map(json_number_text)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Bytes::from(format!("[{}]", rendered.join(","))))
            }
            // RESP3 sends every match for legacy paths too. RESP2 reports the
            // last match that was a number, and errors when none was.
            Frame::Array(Some(values)) => {
                let last = values
                    .iter()
                    .rev()
                    .find(|value| !matches!(value, Frame::Null | Frame::BulkString(None)));
                match last {
                    Some(value) => Ok(Bytes::from(json_number_text(value)?)),
                    None => Err(RedisError::UnexpectedResponse {
                        expected: "a numeric match for a legacy JSON.NUMINCRBY path",
                        actual: format!("{values:?}"),
                    }),
                }
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string (RESP2) or array (RESP3)",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "JSON.NUMINCRBY"
    }
}

/// Render one RESP3 `JSON.NUMINCRBY` match as the JSON text RESP2 returns.
fn json_number_text(frame: &Frame) -> Result<String, RedisError> {
    match frame {
        Frame::Integer(n) => Ok(n.to_string()),
        Frame::Double(d) => Ok(json_float_text(*d)),
        Frame::SpecialFloat(text) | Frame::BulkString(Some(text)) => {
            Ok(String::from_utf8_lossy(text).into_owned())
        }
        Frame::Null | Frame::BulkString(None) => Ok("null".to_string()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "number or null",
            actual: format!("{other:?}"),
        }),
    }
}

/// Render a double as serde_json does, which is how RedisJSON writes numbers
/// in its RESP2 replies: shortest round-trip digits, whole floats keep `.0`,
/// plain notation from `0.00001` up to 16 integer digits, and an unsigned or
/// negative exponent beyond that (`1e16`, `1.5e-7`).
fn json_float_text(d: f64) -> String {
    if !d.is_finite() {
        // JSON has no representation for these; RedisJSON never stores them.
        return d.to_string();
    }
    if d == 0.0 {
        return if d.is_sign_negative() { "-0.0" } else { "0.0" }.to_string();
    }
    // `{:e}` writes the shortest round-trip digits as `d.ddde<exp>`.
    let scientific = format!("{:e}", d.abs());
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("LowerExp always writes an exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let exponent: i32 = exponent
        .parse()
        .expect("LowerExp writes an integer exponent");
    let len = digits.len() as i32;
    // Position of the decimal point relative to the first digit.
    let point = exponent + 1;
    let last = point - len;
    let sign = if d < 0.0 { "-" } else { "" };
    if last >= 0 && point <= 16 {
        format!("{sign}{digits}{}.0", "0".repeat(last as usize))
    } else if 0 < point && point <= 16 {
        let (whole, fraction) = digits.split_at(point as usize);
        format!("{sign}{whole}.{fraction}")
    } else if -5 < point && point <= 0 {
        format!("{sign}0.{}{digits}", "0".repeat(-point as usize))
    } else {
        let (first, rest) = digits.split_at(1);
        let dot = if rest.is_empty() { "" } else { "." };
        format!("{sign}{first}{dot}{rest}e{exponent}")
    }
}

/// JSON.STRLEN key \[path\]
///
/// Returns the length of the JSON string at `path`. For multiple matches,
/// returns an array of integers.
#[derive(Clone)]
pub struct JsonStrLen {
    key: CommandArg,
    path: Option<String>,
}

impl JsonStrLen {
    /// Create a new [`JsonStrLen`] command.
    pub fn new(key: impl Into<CommandArg>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set the path to query.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl Command for JsonStrLen {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.STRLEN"), bulk(&self.key)];
        if let Some(path) = &self.path {
            args.push(bulk(path.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "JSON.STRLEN"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// JSON.STRAPPEND key \[path\] value
///
/// Appends a string to the JSON string at `path`. Returns the new length(s).
#[derive(Clone)]
pub struct JsonStrAppend {
    key: CommandArg,
    path: Option<String>,
    value: String,
}

impl JsonStrAppend {
    /// Create a new [`JsonStrAppend`] command.
    pub fn new(key: impl Into<CommandArg>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: None,
            value: value.into(),
        }
    }

    /// Set the path to append to.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl Command for JsonStrAppend {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.STRAPPEND"), bulk(&self.key)];
        if let Some(path) = &self.path {
            args.push(bulk(path.as_str()));
        }
        args.push(bulk(self.value.as_str()));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "JSON.STRAPPEND"
    }
}

/// JSON.ARRAPPEND key path value \[value ...\]
///
/// Appends one or more values to the array at `path`. Returns the new
/// length(s) of the array.
#[derive(Clone)]
pub struct JsonArrAppend {
    key: CommandArg,
    path: String,
    values: Vec<String>,
}

impl JsonArrAppend {
    /// Create a new [`JsonArrAppend`] command.
    pub fn new(key: impl Into<CommandArg>, path: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            values: Vec::new(),
        }
    }

    /// Add a JSON value to append.
    pub fn value(mut self, value: impl Into<String>) -> Self {
        self.values.push(value.into());
        self
    }

    /// Add multiple JSON values to append.
    pub fn values(mut self, values: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.values.extend(values.into_iter().map(Into::into));
        self
    }
}

impl Command for JsonArrAppend {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("JSON.ARRAPPEND"),
            bulk(&self.key),
            bulk(self.path.as_str()),
        ];
        for value in &self.values {
            args.push(bulk(value.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "JSON.ARRAPPEND"
    }
}

/// JSON.ARRLEN key \[path\]
///
/// Returns the length of the JSON array at `path`.
#[derive(Clone)]
pub struct JsonArrLen {
    key: CommandArg,
    path: Option<String>,
}

impl JsonArrLen {
    /// Create a new [`JsonArrLen`] command.
    pub fn new(key: impl Into<CommandArg>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set the path to query.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl Command for JsonArrLen {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.ARRLEN"), bulk(&self.key)];
        if let Some(path) = &self.path {
            args.push(bulk(path.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "JSON.ARRLEN"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// JSON.ARRINDEX key path value \[start \[stop\]\]
///
/// Searches for the first occurrence of `value` in the array at `path`.
/// Returns the index, or -1 if not found.
#[derive(Clone)]
pub struct JsonArrIndex {
    key: CommandArg,
    path: String,
    value: String,
    start: Option<i64>,
    stop: Option<i64>,
}

impl JsonArrIndex {
    /// Create a new [`JsonArrIndex`] command.
    pub fn new(
        key: impl Into<CommandArg>,
        path: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            value: value.into(),
            start: None,
            stop: None,
        }
    }

    /// Set the start index for the search.
    pub fn start(mut self, start: i64) -> Self {
        self.start = Some(start);
        self
    }

    /// Set the stop index for the search. Requires `start` to be set.
    pub fn stop(mut self, stop: i64) -> Self {
        self.stop = Some(stop);
        self
    }
}

impl Command for JsonArrIndex {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("JSON.ARRINDEX"),
            bulk(&self.key),
            bulk(self.path.as_str()),
            bulk(self.value.as_str()),
        ];
        if let Some(start) = self.start {
            args.push(bulk(start.to_string()));
            if let Some(stop) = self.stop {
                args.push(bulk(stop.to_string()));
            }
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "JSON.ARRINDEX"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// JSON.ARRPOP key \[path \[index\]\]
///
/// Removes and returns the element at `index` from the array at `path`.
/// Defaults to the last element (-1). Returns `None` when there was nothing to
/// pop.
///
/// A `$` JSONPath replies with one entry per match. This command returns the
/// single match's value, and errors when the path matched more than one array
/// (each of which Redis has already popped). Use [`JsonArrPop::all_matches`]
/// for paths that can match several arrays.
#[derive(Clone)]
pub struct JsonArrPop {
    key: CommandArg,
    path: Option<String>,
    index: Option<i64>,
}

impl JsonArrPop {
    /// Create a new [`JsonArrPop`] command.
    pub fn new(key: impl Into<CommandArg>) -> Self {
        Self {
            key: key.into(),
            path: None,
            index: None,
        }
    }

    /// Set the path of the array to pop from.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Set the index of the element to pop. Defaults to -1 (last element).
    pub fn index(mut self, index: i64) -> Self {
        self.index = Some(index);
        self
    }

    /// Return the popped value for every array the path matched.
    ///
    /// The response has one entry per match, `None` for a match that is not
    /// an array or is empty. A legacy path's single value is returned as a
    /// one-element list:
    ///
    /// ```
    /// use redis_tower_commands::JsonArrPop;
    /// use redis_tower_core::Command;
    ///
    /// let command = JsonArrPop::new("doc").path("$..queue").all_matches();
    /// assert_eq!(command.name(), "JSON.ARRPOP");
    /// ```
    pub fn all_matches(self) -> JsonArrPopAll {
        JsonArrPopAll { command: self }
    }
}

/// Decode one `JSON.ARRPOP` value: the popped JSON text, or null when there
/// was nothing to pop (`$-1` in RESP2, `_` in RESP3).
fn parse_arrpop_value(frame: Frame) -> Result<Option<Bytes>, RedisError> {
    match frame {
        Frame::BulkString(data) => Ok(data),
        Frame::Null => Ok(None),
        other => Err(RedisError::UnexpectedResponse {
            expected: "bulk string or null",
            actual: format!("{other:?}"),
        }),
    }
}

/// A [`JsonArrPop`] whose response holds one value per JSONPath match.
///
/// Create this command with [`JsonArrPop::all_matches`].
#[derive(Clone)]
pub struct JsonArrPopAll {
    command: JsonArrPop,
}

impl Command for JsonArrPopAll {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        self.command.to_frame()
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(matches)) => matches.into_iter().map(parse_arrpop_value).collect(),
            Frame::Array(None) => Ok(Vec::new()),
            single => Ok(vec![parse_arrpop_value(single)?]),
        }
    }

    fn name(&self) -> &str {
        "JSON.ARRPOP"
    }
}

impl Command for JsonArrPop {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.ARRPOP"), bulk(&self.key)];
        if let Some(path) = &self.path {
            args.push(bulk(path.as_str()));
            if let Some(index) = self.index {
                args.push(bulk(index.to_string()));
            }
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // `$` JSONPath: one entry per matched array.
            Frame::Array(Some(matches)) => {
                if matches.len() > 1 {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "at most one JSONPath match (use JsonArrPop::all_matches for several)",
                        actual: format!("{} matches", matches.len()),
                    });
                }
                match matches.into_iter().next() {
                    Some(value) => parse_arrpop_value(value),
                    None => Ok(None),
                }
            }
            Frame::Array(None) => Ok(None),
            // Legacy path: the popped value itself.
            single => parse_arrpop_value(single),
        }
    }

    fn name(&self) -> &str {
        "JSON.ARRPOP"
    }
}

/// JSON.OBJKEYS key \[path\]
///
/// Returns the keys of the JSON object at `path`.
#[derive(Clone)]
pub struct JsonObjKeys {
    key: CommandArg,
    path: Option<String>,
}

impl JsonObjKeys {
    /// Create a new [`JsonObjKeys`] command.
    pub fn new(key: impl Into<CommandArg>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set the path to query.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl Command for JsonObjKeys {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.OBJKEYS"), bulk(&self.key)];
        if let Some(path) = &self.path {
            args.push(bulk(path.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "JSON.OBJKEYS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// JSON.OBJLEN key \[path\]
///
/// Returns the number of keys in the JSON object at `path`.
#[derive(Clone)]
pub struct JsonObjLen {
    key: CommandArg,
    path: Option<String>,
}

impl JsonObjLen {
    /// Create a new [`JsonObjLen`] command.
    pub fn new(key: impl Into<CommandArg>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set the path to query.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl Command for JsonObjLen {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.OBJLEN"), bulk(&self.key)];
        if let Some(path) = &self.path {
            args.push(bulk(path.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "JSON.OBJLEN"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// JSON.MERGE key path value
///
/// Merges a JSON value into the existing document at `path` in `key`. The
/// value is merged recursively: existing keys are overwritten, new keys are
/// added, and setting a key to `null` removes it. Returns `Ok(())` on success.
#[derive(Clone)]
pub struct JsonMerge {
    key: CommandArg,
    path: String,
    value: String,
}

impl JsonMerge {
    /// Create a new `JSON.MERGE` command.
    pub fn new(
        key: impl Into<CommandArg>,
        path: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
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
        array(vec![
            bulk("JSON.MERGE"),
            bulk(&self.key),
            bulk(self.path.as_str()),
            bulk(self.value.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            Frame::Null => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "JSON.MERGE"
    }
}

/// JSON.MSET key path value \[key path value ...\]
///
/// Sets multiple JSON values across one or more keys in a single atomic call.
/// Each triple is a `(key, path, value)` where `value` is a serialized JSON
/// string. Returns `Ok(())` on success.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_commands::JsonMSet;
/// use redis_tower_core::RedisConnection;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// conn.execute(
///     JsonMSet::new()
///         .entry("k1", "$", "1")
///         .entry("k2", "$", "{\"a\":2}"),
/// )
/// .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Default)]
pub struct JsonMSet {
    entries: Vec<(CommandArg, String, String)>,
}

impl JsonMSet {
    /// Create an empty `JSON.MSET` command.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Add a `(key, path, value)` triple. `value` is a serialized JSON string.
    pub fn entry(
        mut self,
        key: impl Into<CommandArg>,
        path: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.entries.push((key.into(), path.into(), value.into()));
        self
    }
}

impl Command for JsonMSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.MSET")];
        for (key, path, value) in &self.entries {
            args.push(bulk(key));
            args.push(bulk(path.as_str()));
            args.push(bulk(value.as_str()));
        }
        array(args)
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
        "JSON.MSET"
    }
}

/// JSON.TOGGLE key path
///
/// Toggles boolean values stored at the matching paths. Returns the raw
/// RedisJSON reply: an array of new booleans (`1`/`0`, with nil for a path
/// whose value is not a boolean) under a JSONPath, or a scalar under a legacy
/// path.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_commands::JsonToggle;
/// use redis_tower_core::RedisConnection;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let toggled = conn.execute(JsonToggle::new("k", "$.enabled")).await?;
/// # let _ = toggled;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct JsonToggle {
    key: CommandArg,
    path: String,
}

impl JsonToggle {
    /// Create a new [`JsonToggle`] command.
    pub fn new(key: impl Into<CommandArg>, path: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
        }
    }
}

impl Command for JsonToggle {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("JSON.TOGGLE"),
            bulk(&self.key),
            bulk(self.path.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "JSON.TOGGLE"
    }
}

/// JSON.CLEAR key \[path\]
///
/// Clears container values (arrays and objects) and sets numeric values to `0`
/// at the matching paths. Returns the number of values cleared.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_commands::JsonClear;
/// use redis_tower_core::RedisConnection;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let cleared = conn.execute(JsonClear::new("k").path("$.items")).await?;
/// # let _ = cleared;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct JsonClear {
    key: CommandArg,
    path: Option<String>,
}

impl JsonClear {
    /// Create a new [`JsonClear`] command.
    pub fn new(key: impl Into<CommandArg>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Limit the clear to the given path. Defaults to the root.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl Command for JsonClear {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.CLEAR"), bulk(&self.key)];
        if let Some(path) = &self.path {
            args.push(bulk(path.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "JSON.CLEAR"
    }
}

/// JSON.ARRINSERT key path index value \[value ...\]
///
/// Inserts one or more JSON values into the array at `path` before `index`.
/// Each `value` is a serialized JSON string. Returns the raw RedisJSON reply:
/// an array of new array lengths (nil where the path is not an array) under a
/// JSONPath, or a single length under a legacy path.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_commands::JsonArrInsert;
/// use redis_tower_core::RedisConnection;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let lengths = conn
///     .execute(JsonArrInsert::new("k", "$.items", 0).value("1").value("2"))
///     .await?;
/// # let _ = lengths;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct JsonArrInsert {
    key: CommandArg,
    path: String,
    index: i64,
    values: Vec<String>,
}

impl JsonArrInsert {
    /// Create a new [`JsonArrInsert`] command.
    pub fn new(key: impl Into<CommandArg>, path: impl Into<String>, index: i64) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            index,
            values: Vec::new(),
        }
    }

    /// Add a JSON value to insert.
    pub fn value(mut self, value: impl Into<String>) -> Self {
        self.values.push(value.into());
        self
    }

    /// Add multiple JSON values to insert.
    pub fn values(mut self, values: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.values.extend(values.into_iter().map(Into::into));
        self
    }
}

impl Command for JsonArrInsert {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("JSON.ARRINSERT"),
            bulk(&self.key),
            bulk(self.path.as_str()),
            bulk(self.index.to_string()),
        ];
        for value in &self.values {
            args.push(bulk(value.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "JSON.ARRINSERT"
    }
}

/// JSON.ARRTRIM key path start stop
///
/// Trims the array at `path` to the inclusive `[start, stop]` range. Returns
/// the raw RedisJSON reply: an array of new array lengths (nil where the path
/// is not an array) under a JSONPath, or a single length under a legacy path.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_commands::JsonArrTrim;
/// use redis_tower_core::RedisConnection;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let lengths = conn
///     .execute(JsonArrTrim::new("k", "$.items", 0, 10))
///     .await?;
/// # let _ = lengths;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct JsonArrTrim {
    key: CommandArg,
    path: String,
    start: i64,
    stop: i64,
}

impl JsonArrTrim {
    /// Create a new [`JsonArrTrim`] command.
    pub fn new(key: impl Into<CommandArg>, path: impl Into<String>, start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            start,
            stop,
        }
    }
}

impl Command for JsonArrTrim {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("JSON.ARRTRIM"),
            bulk(&self.key),
            bulk(self.path.as_str()),
            bulk(self.start.to_string()),
            bulk(self.stop.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "JSON.ARRTRIM"
    }
}

/// JSON.FORGET key \[path\]
///
/// An alias for [`JsonDel`]. Deletes a value at `path` in the JSON document
/// stored at `key` and returns the number of paths deleted.
#[derive(Clone)]
pub struct JsonForget {
    key: CommandArg,
    path: Option<String>,
}

impl JsonForget {
    /// Create a new [`JsonForget`] command.
    pub fn new(key: impl Into<CommandArg>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set the path to delete. If omitted, the entire key is deleted.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl Command for JsonForget {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.FORGET"), bulk(&self.key)];
        if let Some(path) = &self.path {
            args.push(bulk(path.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "JSON.FORGET"
    }
}

/// JSON.DEBUG MEMORY key \[path\]
///
/// Reports the memory usage in bytes of a value in the JSON document stored at
/// `key`. With no path the size of the whole document is returned.
#[derive(Clone)]
pub struct JsonDebugMemory {
    key: CommandArg,
    path: Option<String>,
}

impl JsonDebugMemory {
    /// Create a new [`JsonDebugMemory`] command.
    pub fn new(key: impl Into<CommandArg>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Report memory for the value at a specific (legacy) path.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl Command for JsonDebugMemory {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.DEBUG"), bulk("MEMORY"), bulk(&self.key)];
        if let Some(path) = &self.path {
            args.push(bulk(path.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "JSON.DEBUG MEMORY"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    #[test]
    fn idempotency_flags() {
        // Read-only commands are safe to retry.
        assert!(JsonGet::new("k").idempotent());
        // Mutating commands keep the default (false).
        assert!(!JsonSet::new("k", "$", "1").idempotent());
    }

    #[test]
    fn json_mset_to_frame() {
        let cmd = JsonMSet::new()
            .entry("k1", "$", "1")
            .entry("k2", "$", "{\"a\":2}");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("JSON.MSET"),
                bulk("k1"),
                bulk("$"),
                bulk("1"),
                bulk("k2"),
                bulk("$"),
                bulk("{\"a\":2}"),
            ])
        );
    }

    #[test]
    fn json_toggle_to_frame() {
        let cmd = JsonToggle::new("k", "$.enabled");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("JSON.TOGGLE"), bulk("k"), bulk("$.enabled")])
        );
    }

    #[test]
    fn json_toggle_parse_returns_raw_frame() {
        let cmd = JsonToggle::new("k", "$.a");
        let frame = array(vec![Frame::Integer(1), Frame::Null]);
        assert_eq!(cmd.parse_response(frame.clone()).unwrap(), frame);
    }

    #[test]
    fn json_clear_default_path_to_frame() {
        let cmd = JsonClear::new("k");
        assert_eq!(cmd.to_frame(), array(vec![bulk("JSON.CLEAR"), bulk("k")]));
    }

    #[test]
    fn json_clear_with_path_to_frame() {
        let cmd = JsonClear::new("k").path("$.items");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("JSON.CLEAR"), bulk("k"), bulk("$.items")])
        );
    }

    #[test]
    fn json_arrinsert_to_frame() {
        let cmd = JsonArrInsert::new("k", "$.items", 0).value("1").value("2");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("JSON.ARRINSERT"),
                bulk("k"),
                bulk("$.items"),
                bulk("0"),
                bulk("1"),
                bulk("2"),
            ])
        );
    }

    #[test]
    fn json_arrtrim_to_frame() {
        let cmd = JsonArrTrim::new("k", "$.items", 0, 10);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("JSON.ARRTRIM"),
                bulk("k"),
                bulk("$.items"),
                bulk("0"),
                bulk("10"),
            ])
        );
    }

    #[test]
    fn json_forget_to_frame() {
        let cmd = JsonForget::new("doc").path("$.a");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("JSON.FORGET"), bulk("doc"), bulk("$.a")])
        );
        assert_eq!(
            JsonForget::new("doc").to_frame(),
            array(vec![bulk("JSON.FORGET"), bulk("doc")])
        );
    }

    #[test]
    fn json_forget_parse_integer() {
        let cmd = JsonForget::new("doc");
        assert_eq!(cmd.parse_response(Frame::Integer(1)).unwrap(), 1);
    }

    #[test]
    fn json_debug_memory_to_frame() {
        assert_eq!(
            JsonDebugMemory::new("doc").to_frame(),
            array(vec![bulk("JSON.DEBUG"), bulk("MEMORY"), bulk("doc")])
        );
        assert_eq!(
            JsonDebugMemory::new("doc").path(".").to_frame(),
            array(vec![
                bulk("JSON.DEBUG"),
                bulk("MEMORY"),
                bulk("doc"),
                bulk(".")
            ])
        );
    }

    #[test]
    fn json_debug_memory_parse_integer() {
        let cmd = JsonDebugMemory::new("doc");
        assert_eq!(cmd.parse_response(Frame::Integer(64)).unwrap(), 64);
        assert!(cmd.idempotent());
    }

    #[test]
    fn json_type_parses_legacy_and_jsonpath_responses() {
        let cmd = JsonType::new("doc").path("$.name");

        assert_eq!(
            cmd.parse_response(Frame::BulkString(Some(Bytes::from("string"))))
                .unwrap(),
            Some(Bytes::from("string"))
        );
        assert_eq!(
            cmd.parse_response(Frame::Array(Some(vec![Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from("string"))),
            ]))])))
            .unwrap(),
            Some(Bytes::from("string"))
        );
    }

    #[test]
    fn json_type_returns_none_for_missing_jsonpath() {
        let cmd = JsonType::new("doc").path("$.missing");

        assert_eq!(
            cmd.parse_response(Frame::Array(Some(Vec::new()))).unwrap(),
            None
        );
        assert_eq!(
            cmd.parse_response(Frame::Array(Some(vec![Frame::Array(Some(Vec::new()))])))
                .unwrap(),
            None
        );
    }
}
