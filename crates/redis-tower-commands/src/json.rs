use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// Condition for JSON.SET (NX or XX).
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
/// See: <https://redis.io/docs/latest/commands/json.set/>
pub struct JsonSet {
    key: String,
    path: String,
    value: String,
    condition: Option<JsonSetCondition>,
}

impl JsonSet {
    /// Creates a new [`JsonSet`] command.
    pub fn new(key: impl Into<String>, path: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            value: value.into(),
            condition: None,
        }
    }

    /// Only set if the path does not already exist.
    #[must_use]
    pub fn nx(mut self) -> Self {
        self.condition = Some(JsonSetCondition::Nx);
        self
    }

    /// Only set if the path already exists.
    #[must_use]
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
///
/// See: <https://redis.io/docs/latest/commands/json.get/>
pub struct JsonGet {
    key: String,
    paths: Vec<String>,
}

impl JsonGet {
    /// Creates a new [`JsonGet`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            paths: Vec::new(),
        }
    }

    /// Add a path to retrieve.
    #[must_use]
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.paths.push(path.into());
        self
    }

    /// Add multiple paths to retrieve.
    #[must_use]
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
///
/// See: <https://redis.io/docs/latest/commands/json.del/>
pub struct JsonDel {
    key: String,
    path: Option<String>,
}

impl JsonDel {
    /// Creates a new [`JsonDel`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set the path to delete. If omitted, the entire key is deleted.
    #[must_use]
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
///
/// See: <https://redis.io/docs/latest/commands/json.mget/>
pub struct JsonMGet {
    keys: Vec<String>,
    path: String,
}

impl JsonMGet {
    /// Creates a new [`JsonMGet`] command.
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
///
/// See: <https://redis.io/docs/latest/commands/json.type/>
pub struct JsonType {
    key: String,
    path: Option<String>,
}

impl JsonType {
    /// Creates a new [`JsonType`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set the path to query. If omitted, returns the type of the root.
    #[must_use]
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
///
/// See: <https://redis.io/docs/latest/commands/json.numincrby/>
pub struct JsonNumIncrBy {
    key: String,
    path: String,
    value: f64,
}

impl JsonNumIncrBy {
    /// Creates a new [`JsonNumIncrBy`] command.
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
///
/// See: <https://redis.io/docs/latest/commands/json.strlen/>
pub struct JsonStrLen {
    key: String,
    path: Option<String>,
}

impl JsonStrLen {
    /// Creates a new [`JsonStrLen`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set the path to query.
    #[must_use]
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
///
/// See: <https://redis.io/docs/latest/commands/json.strappend/>
pub struct JsonStrAppend {
    key: String,
    path: Option<String>,
    value: String,
}

impl JsonStrAppend {
    /// Creates a new [`JsonStrAppend`] command.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: None,
            value: value.into(),
        }
    }

    /// Set the path to append to.
    #[must_use]
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
///
/// See: <https://redis.io/docs/latest/commands/json.arrappend/>
pub struct JsonArrAppend {
    key: String,
    path: String,
    values: Vec<String>,
}

impl JsonArrAppend {
    /// Creates a new [`JsonArrAppend`] command.
    pub fn new(key: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
            values: Vec::new(),
        }
    }

    /// Add a JSON value to append.
    #[must_use]
    pub fn value(mut self, value: impl Into<String>) -> Self {
        self.values.push(value.into());
        self
    }

    /// Add multiple JSON values to append.
    #[must_use]
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
///
/// See: <https://redis.io/docs/latest/commands/json.arrlen/>
pub struct JsonArrLen {
    key: String,
    path: Option<String>,
}

impl JsonArrLen {
    /// Creates a new [`JsonArrLen`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set the path to query.
    #[must_use]
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
///
/// See: <https://redis.io/docs/latest/commands/json.arrindex/>
pub struct JsonArrIndex {
    key: String,
    path: String,
    value: String,
    start: Option<i64>,
    stop: Option<i64>,
}

impl JsonArrIndex {
    /// Creates a new [`JsonArrIndex`] command.
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
    #[must_use]
    pub fn start(mut self, start: i64) -> Self {
        self.start = Some(start);
        self
    }

    /// Set the stop index for the search. Requires `start` to be set.
    #[must_use]
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
///
/// See: <https://redis.io/docs/latest/commands/json.arrpop/>
pub struct JsonArrPop {
    key: String,
    path: Option<String>,
    index: Option<i64>,
}

impl JsonArrPop {
    /// Creates a new [`JsonArrPop`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: None,
            index: None,
        }
    }

    /// Set the path of the array to pop from.
    #[must_use]
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Set the index of the element to pop. Defaults to -1 (last element).
    #[must_use]
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
///
/// See: <https://redis.io/docs/latest/commands/json.objkeys/>
pub struct JsonObjKeys {
    key: String,
    path: Option<String>,
}

impl JsonObjKeys {
    /// Creates a new [`JsonObjKeys`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set the path to query.
    #[must_use]
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
///
/// See: <https://redis.io/docs/latest/commands/json.objlen/>
pub struct JsonObjLen {
    key: String,
    path: Option<String>,
}

impl JsonObjLen {
    /// Creates a new [`JsonObjLen`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            path: None,
        }
    }

    /// Set the path to query.
    #[must_use]
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
