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
#[derive(Clone)]
pub struct JsonSet {
    key: String,
    path: String,
    value: String,
    condition: Option<JsonSetCondition>,
}

impl JsonSet {
    pub fn new(key: impl Into<String>, path: impl Into<String>, value: impl Into<String>) -> Self {
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
}

impl Command for JsonSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("JSON.SET"),
            bulk(self.key.as_str()),
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
            Frame::Null => Ok(()),
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
    key: String,
    paths: Vec<String>,
}

impl JsonGet {
    pub fn new(key: impl Into<String>) -> Self {
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
        let mut args = vec![bulk("JSON.GET"), bulk(self.key.as_str())];
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
}

/// JSON.DEL key \[path\]
///
/// Deletes a value at `path` in the JSON document stored at `key`. Returns
/// the number of paths deleted.
#[derive(Clone)]
pub struct JsonDel {
    key: String,
    path: Option<String>,
}

impl JsonDel {
    pub fn new(key: impl Into<String>) -> Self {
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
        let mut args = vec![bulk("JSON.DEL"), bulk(self.key.as_str())];
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
    keys: Vec<String>,
    path: String,
}

impl JsonMGet {
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>, path: impl Into<String>) -> Self {
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
            args.push(bulk(key.as_str()));
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
}

/// JSON.TYPE key \[path\]
///
/// Returns the type of the JSON value at `path`.
#[derive(Clone)]
pub struct JsonType {
    key: String,
    path: Option<String>,
}

impl JsonType {
    pub fn new(key: impl Into<String>) -> Self {
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
        let mut args = vec![bulk("JSON.TYPE"), bulk(self.key.as_str())];
        if let Some(path) = &self.path {
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
        "JSON.TYPE"
    }
}

/// JSON.NUMINCRBY key path value
///
/// Increments the numeric value at `path` by `value`. Returns the new value
/// as a string.
#[derive(Clone)]
pub struct JsonNumIncrBy {
    key: String,
    path: String,
    value: f64,
}

impl JsonNumIncrBy {
    pub fn new(key: impl Into<String>, path: impl Into<String>, value: f64) -> Self {
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
            bulk(self.key.as_str()),
            bulk(self.path.as_str()),
            bulk(self.value.to_string()),
        ])
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
        "JSON.NUMINCRBY"
    }
}

/// JSON.STRLEN key \[path\]
///
/// Returns the length of the JSON string at `path`. For multiple matches,
/// returns an array of integers.
#[derive(Clone)]
pub struct JsonStrLen {
    key: String,
    path: Option<String>,
}

impl JsonStrLen {
    pub fn new(key: impl Into<String>) -> Self {
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
        let mut args = vec![bulk("JSON.STRLEN"), bulk(self.key.as_str())];
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
}

/// JSON.STRAPPEND key \[path\] value
///
/// Appends a string to the JSON string at `path`. Returns the new length(s).
#[derive(Clone)]
pub struct JsonStrAppend {
    key: String,
    path: Option<String>,
    value: String,
}

impl JsonStrAppend {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
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
        let mut args = vec![bulk("JSON.STRAPPEND"), bulk(self.key.as_str())];
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
    key: String,
    path: String,
    values: Vec<String>,
}

impl JsonArrAppend {
    pub fn new(key: impl Into<String>, path: impl Into<String>) -> Self {
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
            bulk(self.key.as_str()),
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
    key: String,
    path: Option<String>,
}

impl JsonArrLen {
    pub fn new(key: impl Into<String>) -> Self {
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
        let mut args = vec![bulk("JSON.ARRLEN"), bulk(self.key.as_str())];
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
}

/// JSON.ARRINDEX key path value \[start \[stop\]\]
///
/// Searches for the first occurrence of `value` in the array at `path`.
/// Returns the index, or -1 if not found.
#[derive(Clone)]
pub struct JsonArrIndex {
    key: String,
    path: String,
    value: String,
    start: Option<i64>,
    stop: Option<i64>,
}

impl JsonArrIndex {
    pub fn new(key: impl Into<String>, path: impl Into<String>, value: impl Into<String>) -> Self {
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
            bulk(self.key.as_str()),
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
}

/// JSON.ARRPOP key \[path \[index\]\]
///
/// Removes and returns the element at `index` from the array at `path`.
/// Defaults to the last element (-1).
#[derive(Clone)]
pub struct JsonArrPop {
    key: String,
    path: Option<String>,
    index: Option<i64>,
}

impl JsonArrPop {
    pub fn new(key: impl Into<String>) -> Self {
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
}

impl Command for JsonArrPop {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("JSON.ARRPOP"), bulk(self.key.as_str())];
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
            Frame::BulkString(data) => Ok(data),
            Frame::Null => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or null",
                actual: format!("{other:?}"),
            }),
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
    key: String,
    path: Option<String>,
}

impl JsonObjKeys {
    pub fn new(key: impl Into<String>) -> Self {
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
        let mut args = vec![bulk("JSON.OBJKEYS"), bulk(self.key.as_str())];
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
}

/// JSON.OBJLEN key \[path\]
///
/// Returns the number of keys in the JSON object at `path`.
#[derive(Clone)]
pub struct JsonObjLen {
    key: String,
    path: Option<String>,
}

impl JsonObjLen {
    pub fn new(key: impl Into<String>) -> Self {
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
        let mut args = vec![bulk("JSON.OBJLEN"), bulk(self.key.as_str())];
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
}

/// JSON.MERGE key path value
///
/// Merges a JSON value into the existing document at `path` in `key`. The
/// value is merged recursively: existing keys are overwritten, new keys are
/// added, and setting a key to `null` removes it. Returns `Ok(())` on success.
#[derive(Clone)]
pub struct JsonMerge {
    key: String,
    path: String,
    value: String,
}

impl JsonMerge {
    /// Create a new `JSON.MERGE` command.
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
        array(vec![
            bulk("JSON.MERGE"),
            bulk(self.key.as_str()),
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
/// ```ignore
/// use redis_tower::commands::JsonMSet;
///
/// let cmd = JsonMSet::new()
///     .entry("k1", "$", "1")
///     .entry("k2", "$", "{\"a\":2}");
/// ```
#[derive(Clone, Default)]
pub struct JsonMSet {
    entries: Vec<(String, String, String)>,
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
        key: impl Into<String>,
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
            args.push(bulk(key.as_str()));
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
/// ```ignore
/// use redis_tower::commands::JsonToggle;
///
/// let cmd = JsonToggle::new("k", "$.enabled");
/// ```
#[derive(Clone)]
pub struct JsonToggle {
    key: String,
    path: String,
}

impl JsonToggle {
    pub fn new(key: impl Into<String>, path: impl Into<String>) -> Self {
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
            bulk(self.key.as_str()),
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
/// ```ignore
/// use redis_tower::commands::JsonClear;
///
/// let cmd = JsonClear::new("k").path("$.items");
/// ```
#[derive(Clone)]
pub struct JsonClear {
    key: String,
    path: Option<String>,
}

impl JsonClear {
    pub fn new(key: impl Into<String>) -> Self {
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
        let mut args = vec![bulk("JSON.CLEAR"), bulk(self.key.as_str())];
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
/// ```ignore
/// use redis_tower::commands::JsonArrInsert;
///
/// let cmd = JsonArrInsert::new("k", "$.items", 0).value("1").value("2");
/// ```
#[derive(Clone)]
pub struct JsonArrInsert {
    key: String,
    path: String,
    index: i64,
    values: Vec<String>,
}

impl JsonArrInsert {
    pub fn new(key: impl Into<String>, path: impl Into<String>, index: i64) -> Self {
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
            bulk(self.key.as_str()),
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
/// ```ignore
/// use redis_tower::commands::JsonArrTrim;
///
/// let cmd = JsonArrTrim::new("k", "$.items", 0, 10);
/// ```
#[derive(Clone)]
pub struct JsonArrTrim {
    key: String,
    path: String,
    start: i64,
    stop: i64,
}

impl JsonArrTrim {
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
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("JSON.ARRTRIM"),
            bulk(self.key.as_str()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

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
}
