use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// HGET key field
///
/// Returns the value associated with `field` in the hash stored at `key`,
/// or `None` if the field or key does not exist.
#[derive(Clone)]
pub struct HGet {
    key: String,
    field: String,
}

impl HGet {
    pub fn new(key: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
        }
    }
}

impl Command for HGet {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("HGET"),
            bulk(self.key.as_str()),
            bulk(self.field.as_str()),
        ])
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
        "HGET"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// HSET key field value \[field value ...\]
///
/// Sets one or more field-value pairs in the hash stored at `key`.
/// Returns the number of fields that were added (not updated).
#[derive(Clone)]
pub struct HSet {
    key: String,
    fields: Vec<(String, String)>,
}

impl HSet {
    pub fn new(key: impl Into<String>, field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            fields: vec![(field.into(), value.into())],
        }
    }

    /// Constructs an [`HSet`] from an iterator of `(field, value)` pairs.
    ///
    /// This is the bulk-insert constructor: equivalent to calling `.field()` for every
    /// pair in the iterator. Accepts any `IntoIterator<Item = (impl Into<String>, impl Into<String>)>`,
    /// including `HashMap`, `Vec<(&str, &str)>`, and similar collections.
    ///
    /// Produces the same wire frame as the incremental builder:
    ///
    /// ```rust,ignore
    /// // These two are equivalent:
    /// let a = HSet::new("h", "f1", "v1").field("f2", "v2");
    /// let b = HSet::from_fields("h", [("f1", "v1"), ("f2", "v2")]);
    /// assert_eq!(a.to_frame(), b.to_frame());
    /// ```
    pub fn from_fields(
        key: impl Into<String>,
        fields: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        Self {
            key: key.into(),
            fields: fields
                .into_iter()
                .map(|(f, v)| (f.into(), v.into()))
                .collect(),
        }
    }

    /// Add an additional field-value pair.
    pub fn field(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push((name.into(), value.into()));
        self
    }
}

impl Command for HSet {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("HSET"), bulk(self.key.as_str())];
        for (field, value) in &self.fields {
            args.push(bulk(field.as_str()));
            args.push(bulk(value.as_str()));
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
        "HSET"
    }
}

/// HDEL key field \[field ...\]
///
/// Removes the specified fields from the hash stored at `key`.
/// Returns the number of fields that were removed.
#[derive(Clone)]
pub struct HDel {
    key: String,
    fields: Vec<String>,
}

impl HDel {
    pub fn new(key: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            fields: vec![field.into()],
        }
    }

    pub fn fields(
        key: impl Into<String>,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for HDel {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("HDEL"), bulk(self.key.as_str())];
        for field in &self.fields {
            args.push(bulk(field.as_str()));
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
        "HDEL"
    }
}

/// HEXISTS key field
///
/// Returns `true` if `field` exists in the hash stored at `key`.
#[derive(Clone)]
pub struct HExists {
    key: String,
    field: String,
}

impl HExists {
    pub fn new(key: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
        }
    }
}

impl Command for HExists {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("HEXISTS"),
            bulk(self.key.as_str()),
            bulk(self.field.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n == 1),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "HEXISTS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// HGETALL key
///
/// Returns all fields and values of the hash stored at `key` as a list
/// of `(field, value)` pairs.
#[derive(Clone)]
pub struct HGetAll {
    key: String,
}

impl HGetAll {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for HGetAll {
    type Response = Vec<(Bytes, Bytes)>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("HGETALL"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => {
                if frames.len() % 2 != 0 {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "array with even number of elements",
                        actual: format!("array with {} elements", frames.len()),
                    });
                }
                let mut pairs = Vec::with_capacity(frames.len() / 2);
                let mut iter = frames.into_iter();
                while let (Some(field), Some(value)) = (iter.next(), iter.next()) {
                    let field = match field {
                        Frame::BulkString(Some(data)) => data,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    let value = match value {
                        Frame::BulkString(Some(data)) => data,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    pairs.push((field, value));
                }
                Ok(pairs)
            }
            Frame::Map(entries) => {
                let mut pairs = Vec::with_capacity(entries.len());
                for (k, v) in entries {
                    let field = match k {
                        Frame::BulkString(Some(data)) => data,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    let value = match v {
                        Frame::BulkString(Some(data)) => data,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    pairs.push((field, value));
                }
                Ok(pairs)
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or map",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "HGETALL"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// HINCRBY key field increment
///
/// Increments the integer value of `field` in the hash stored at `key`
/// by `increment`. Returns the new value.
#[derive(Clone)]
pub struct HIncrBy {
    key: String,
    field: String,
    increment: i64,
}

impl HIncrBy {
    pub fn new(key: impl Into<String>, field: impl Into<String>, increment: i64) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
            increment,
        }
    }
}

impl Command for HIncrBy {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("HINCRBY"),
            bulk(self.key.as_str()),
            bulk(self.field.as_str()),
            bulk(self.increment.to_string()),
        ])
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
        "HINCRBY"
    }
}

/// HKEYS key
///
/// Returns all field names in the hash stored at `key`.
#[derive(Clone)]
pub struct HKeys {
    key: String,
}

impl HKeys {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for HKeys {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("HKEYS"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(data)) => Ok(data),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string",
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
        "HKEYS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// HVALS key
///
/// Returns all values in the hash stored at `key`.
#[derive(Clone)]
pub struct HVals {
    key: String,
}

impl HVals {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for HVals {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("HVALS"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(data)) => Ok(data),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string",
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
        "HVALS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// HLEN key
///
/// Returns the number of fields contained in the hash stored at `key`.
#[derive(Clone)]
pub struct HLen {
    key: String,
}

impl HLen {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for HLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("HLEN"), bulk(self.key.as_str())])
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
        "HLEN"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Hash field expiration commands (Redis 7.4+)
// ---------------------------------------------------------------------------

/// Parse a response that returns one integer per field (used by all hash
/// field expiration commands).
fn parse_per_field_response(frame: Frame) -> Result<Vec<i64>, RedisError> {
    match frame {
        Frame::Array(Some(frames)) => frames
            .into_iter()
            .map(|f| match f {
                Frame::Integer(n) => Ok(n),
                other => Err(RedisError::UnexpectedResponse {
                    expected: "integer",
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

/// HEXPIRE key seconds FIELDS numfields field [field ...]
///
/// Sets an expiration (in seconds) on the specified hash fields.
/// Returns one status code per field.
#[derive(Clone)]
pub struct HExpire {
    key: String,
    seconds: i64,
    fields: Vec<String>,
}

impl HExpire {
    pub fn new(
        key: impl Into<String>,
        seconds: i64,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            seconds,
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for HExpire {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("HEXPIRE"),
            bulk(self.key.as_str()),
            bulk(self.seconds.to_string()),
            bulk("FIELDS"),
            bulk(self.fields.len().to_string()),
        ];
        for f in &self.fields {
            args.push(bulk(f.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_per_field_response(frame)
    }

    fn name(&self) -> &str {
        "HEXPIRE"
    }
}

/// HEXPIREAT key unix-time-seconds FIELDS numfields field [field ...]
///
/// Sets an expiration on hash fields using an absolute Unix timestamp (seconds).
/// Returns one status code per field.
#[derive(Clone)]
pub struct HExpireAt {
    key: String,
    timestamp: i64,
    fields: Vec<String>,
}

impl HExpireAt {
    pub fn new(
        key: impl Into<String>,
        timestamp: i64,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            timestamp,
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for HExpireAt {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("HEXPIREAT"),
            bulk(self.key.as_str()),
            bulk(self.timestamp.to_string()),
            bulk("FIELDS"),
            bulk(self.fields.len().to_string()),
        ];
        for f in &self.fields {
            args.push(bulk(f.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_per_field_response(frame)
    }

    fn name(&self) -> &str {
        "HEXPIREAT"
    }
}

/// HPEXPIRE key milliseconds FIELDS numfields field [field ...]
///
/// Sets an expiration (in milliseconds) on the specified hash fields.
/// Returns one status code per field.
#[derive(Clone)]
pub struct HPExpire {
    key: String,
    milliseconds: i64,
    fields: Vec<String>,
}

impl HPExpire {
    pub fn new(
        key: impl Into<String>,
        milliseconds: i64,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            milliseconds,
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for HPExpire {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("HPEXPIRE"),
            bulk(self.key.as_str()),
            bulk(self.milliseconds.to_string()),
            bulk("FIELDS"),
            bulk(self.fields.len().to_string()),
        ];
        for f in &self.fields {
            args.push(bulk(f.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_per_field_response(frame)
    }

    fn name(&self) -> &str {
        "HPEXPIRE"
    }
}

/// HPEXPIREAT key unix-time-milliseconds FIELDS numfields field [field ...]
///
/// Sets an expiration on hash fields using an absolute Unix timestamp (milliseconds).
/// Returns one status code per field.
#[derive(Clone)]
pub struct HPExpireAt {
    key: String,
    timestamp: i64,
    fields: Vec<String>,
}

impl HPExpireAt {
    pub fn new(
        key: impl Into<String>,
        timestamp: i64,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            timestamp,
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for HPExpireAt {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("HPEXPIREAT"),
            bulk(self.key.as_str()),
            bulk(self.timestamp.to_string()),
            bulk("FIELDS"),
            bulk(self.fields.len().to_string()),
        ];
        for f in &self.fields {
            args.push(bulk(f.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_per_field_response(frame)
    }

    fn name(&self) -> &str {
        "HPEXPIREAT"
    }
}

/// HTTL key FIELDS numfields field [field ...]
///
/// Returns the remaining TTL (in seconds) for the specified hash fields.
/// Returns one value per field.
#[derive(Clone)]
pub struct HTtl {
    key: String,
    fields: Vec<String>,
}

impl HTtl {
    pub fn new(
        key: impl Into<String>,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for HTtl {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("HTTL"),
            bulk(self.key.as_str()),
            bulk("FIELDS"),
            bulk(self.fields.len().to_string()),
        ];
        for f in &self.fields {
            args.push(bulk(f.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_per_field_response(frame)
    }

    fn name(&self) -> &str {
        "HTTL"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// HPTTL key FIELDS numfields field [field ...]
///
/// Returns the remaining TTL (in milliseconds) for the specified hash fields.
/// Returns one value per field.
#[derive(Clone)]
pub struct HPTtl {
    key: String,
    fields: Vec<String>,
}

impl HPTtl {
    pub fn new(
        key: impl Into<String>,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for HPTtl {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("HPTTL"),
            bulk(self.key.as_str()),
            bulk("FIELDS"),
            bulk(self.fields.len().to_string()),
        ];
        for f in &self.fields {
            args.push(bulk(f.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_per_field_response(frame)
    }

    fn name(&self) -> &str {
        "HPTTL"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// HPERSIST key FIELDS numfields field [field ...]
///
/// Removes the expiration from the specified hash fields.
/// Returns one status code per field.
#[derive(Clone)]
pub struct HPersist {
    key: String,
    fields: Vec<String>,
}

impl HPersist {
    pub fn new(
        key: impl Into<String>,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for HPersist {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("HPERSIST"),
            bulk(self.key.as_str()),
            bulk("FIELDS"),
            bulk(self.fields.len().to_string()),
        ];
        for f in &self.fields {
            args.push(bulk(f.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_per_field_response(frame)
    }

    fn name(&self) -> &str {
        "HPERSIST"
    }
}

/// HSETNX key field value
///
/// Sets `field` in the hash stored at `key` to `value`, only if `field`
/// does not yet exist. Returns `true` if the field was set, `false` if it
/// already existed.
#[derive(Clone)]
pub struct HSetNx {
    key: String,
    field: String,
    value: String,
}

impl HSetNx {
    pub fn new(key: impl Into<String>, field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
            value: value.into(),
        }
    }
}

impl Command for HSetNx {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("HSETNX"),
            bulk(self.key.as_str()),
            bulk(self.field.as_str()),
            bulk(self.value.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n == 1),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "HSETNX"
    }
}

/// HINCRBYFLOAT key field increment
///
/// Increments the floating-point value of `field` in the hash stored at
/// `key` by `increment`. Returns the new value as `f64`.
#[derive(Clone)]
pub struct HIncrByFloat {
    key: String,
    field: String,
    increment: f64,
}

impl HIncrByFloat {
    pub fn new(key: impl Into<String>, field: impl Into<String>, increment: f64) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
            increment,
        }
    }
}

impl Command for HIncrByFloat {
    type Response = f64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("HINCRBYFLOAT"),
            bulk(self.key.as_str()),
            bulk(self.field.as_str()),
            bulk(self.increment.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(&data);
                s.parse::<f64>()
                    .map_err(|_| RedisError::UnexpectedResponse {
                        expected: "float string",
                        actual: format!("{s}"),
                    })
            }
            Frame::Double(d) => Ok(d),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or double",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "HINCRBYFLOAT"
    }
}

/// HRANDFIELD key \[count\]
///
/// Returns one or more random field names from the hash stored at `key`.
/// Without `count`, returns a single random field; with `count`, returns
/// up to that many fields. The result is always returned as a `Vec<Bytes>`.
#[derive(Clone)]
pub struct HRandField {
    key: String,
    count: Option<i64>,
}

impl HRandField {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    /// Request `count` random fields. A negative count allows duplicates.
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for HRandField {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("HRANDFIELD"), bulk(self.key.as_str())];
        if let Some(count) = self.count {
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // Single field returned when no count argument was sent.
            Frame::BulkString(Some(data)) => Ok(vec![data]),
            Frame::BulkString(None) | Frame::Null => Ok(vec![]),
            // Multiple fields returned when count argument was sent.
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(data)) => Ok(data),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string",
                        actual: format!("{other:?}"),
                    }),
                })
                .collect(),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "HRANDFIELD"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// HEXPIRETIME key FIELDS numfields field [field ...]
///
/// Returns the absolute Unix expiration timestamp (in seconds) for the
/// specified hash fields. Returns one value per field.
#[derive(Clone)]
pub struct HExpireTime {
    key: String,
    fields: Vec<String>,
}

impl HExpireTime {
    pub fn new(
        key: impl Into<String>,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for HExpireTime {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("HEXPIRETIME"),
            bulk(self.key.as_str()),
            bulk("FIELDS"),
            bulk(self.fields.len().to_string()),
        ];
        for f in &self.fields {
            args.push(bulk(f.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_per_field_response(frame)
    }

    fn name(&self) -> &str {
        "HEXPIRETIME"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// HMGET key field [field ...]
///
/// Returns the values associated with the specified fields in the hash stored
/// at `key`. For each field, returns `Some(value)` if it exists, or `None` if
/// the field is missing.
#[derive(Clone)]
pub struct HMGet {
    key: String,
    fields: Vec<String>,
}

impl HMGet {
    pub fn new(key: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            fields: vec![field.into()],
        }
    }

    pub fn fields(
        key: impl Into<String>,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }

    /// Add another field to request.
    pub fn field(mut self, f: impl Into<String>) -> Self {
        self.fields.push(f.into());
        self
    }
}

impl Command for HMGet {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("HMGET"), bulk(self.key.as_str())];
        for f in &self.fields {
            args.push(bulk(f.as_str()));
        }
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
        "HMGET"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// HSTRLEN key field
///
/// Returns the string length of the value associated with `field` in the hash
/// stored at `key`, or 0 if the field or key does not exist.
#[derive(Clone)]
pub struct HStrLen {
    key: String,
    field: String,
}

impl HStrLen {
    pub fn new(key: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
        }
    }
}

impl Command for HStrLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("HSTRLEN"),
            bulk(self.key.as_str()),
            bulk(self.field.as_str()),
        ])
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
        "HSTRLEN"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// HPEXPIRETIME key FIELDS numfields field [field ...]
///
/// Returns the absolute Unix expiration timestamp (in milliseconds) for the
/// specified hash fields. Returns one value per field.
#[derive(Clone)]
pub struct HPExpireTime {
    key: String,
    fields: Vec<String>,
}

impl HPExpireTime {
    pub fn new(
        key: impl Into<String>,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for HPExpireTime {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("HPEXPIRETIME"),
            bulk(self.key.as_str()),
            bulk("FIELDS"),
            bulk(self.fields.len().to_string()),
        ];
        for f in &self.fields {
            args.push(bulk(f.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_per_field_response(frame)
    }

    fn name(&self) -> &str {
        "HPEXPIRETIME"
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

    // -- HGet --

    #[test]
    fn hget_to_frame() {
        let cmd = HGet::new("myhash", "field1");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("HGET"), bulk("myhash"), bulk("field1")])
        );
    }

    #[test]
    fn hget_parse_value() {
        let cmd = HGet::new("h", "f");
        let frame = Frame::BulkString(Some(Bytes::from("val")));
        assert_eq!(cmd.parse_response(frame).unwrap(), Some(Bytes::from("val")));
    }

    #[test]
    fn hget_parse_null() {
        let cmd = HGet::new("h", "f");
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    #[test]
    fn hget_parse_error_on_integer() {
        let cmd = HGet::new("h", "f");
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    // -- HSet --

    #[test]
    fn hset_single_to_frame() {
        let cmd = HSet::new("myhash", "f1", "v1");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("HSET"), bulk("myhash"), bulk("f1"), bulk("v1")])
        );
    }

    #[test]
    fn hset_multiple_to_frame() {
        let cmd = HSet::new("h", "f1", "v1").field("f2", "v2");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("HSET"),
                bulk("h"),
                bulk("f1"),
                bulk("v1"),
                bulk("f2"),
                bulk("v2"),
            ])
        );
    }

    #[test]
    fn hset_parse_integer() {
        let cmd = HSet::new("h", "f", "v");
        assert_eq!(cmd.parse_response(Frame::Integer(1)).unwrap(), 1);
    }

    #[test]
    fn hset_from_fields_to_frame() {
        let cmd = HSet::from_fields("h", [("f1", "v1"), ("f2", "v2")]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("HSET"),
                bulk("h"),
                bulk("f1"),
                bulk("v1"),
                bulk("f2"),
                bulk("v2"),
            ])
        );
    }

    #[test]
    fn hset_from_fields_matches_incremental() {
        let incremental = HSet::new("h", "f1", "v1").field("f2", "v2");
        let bulk = HSet::from_fields("h", [("f1", "v1"), ("f2", "v2")]);
        assert_eq!(incremental.to_frame(), bulk.to_frame());
    }

    #[test]
    fn hset_from_fields_vec() {
        let fields = vec![("name", "Alice"), ("city", "Portland")];
        let cmd = HSet::from_fields("user:1", fields);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("HSET"),
                bulk("user:1"),
                bulk("name"),
                bulk("Alice"),
                bulk("city"),
                bulk("Portland"),
            ])
        );
    }

    // -- HDel --

    #[test]
    fn hdel_to_frame() {
        let cmd = HDel::new("h", "f1");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("HDEL"), bulk("h"), bulk("f1")])
        );
    }

    #[test]
    fn hdel_multiple_to_frame() {
        let cmd = HDel::fields("h", vec!["f1", "f2"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("HDEL"), bulk("h"), bulk("f1"), bulk("f2")])
        );
    }

    // -- HExists --

    #[test]
    fn hexists_to_frame() {
        let cmd = HExists::new("h", "f");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("HEXISTS"), bulk("h"), bulk("f")])
        );
    }

    #[test]
    fn hexists_parse_true() {
        let cmd = HExists::new("h", "f");
        assert!(cmd.parse_response(Frame::Integer(1)).unwrap());
    }

    #[test]
    fn hexists_parse_false() {
        let cmd = HExists::new("h", "f");
        assert!(!cmd.parse_response(Frame::Integer(0)).unwrap());
    }

    // -- HGetAll --

    #[test]
    fn hgetall_to_frame() {
        let cmd = HGetAll::new("myhash");
        assert_eq!(cmd.to_frame(), array(vec![bulk("HGETALL"), bulk("myhash")]));
    }

    #[test]
    fn hgetall_parse_flat_array() {
        let cmd = HGetAll::new("h");
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("f1"))),
            Frame::BulkString(Some(Bytes::from("v1"))),
            Frame::BulkString(Some(Bytes::from("f2"))),
            Frame::BulkString(Some(Bytes::from("v2"))),
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(
            result,
            vec![
                (Bytes::from("f1"), Bytes::from("v1")),
                (Bytes::from("f2"), Bytes::from("v2")),
            ]
        );
    }

    #[test]
    fn hgetall_parse_map() {
        let cmd = HGetAll::new("h");
        let frame = Frame::Map(vec![(
            Frame::BulkString(Some(Bytes::from("f1"))),
            Frame::BulkString(Some(Bytes::from("v1"))),
        )]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![(Bytes::from("f1"), Bytes::from("v1"))]);
    }

    #[test]
    fn hgetall_parse_odd_array_error() {
        let cmd = HGetAll::new("h");
        let frame = array(vec![Frame::BulkString(Some(Bytes::from("f1")))]);
        assert!(cmd.parse_response(frame).is_err());
    }

    #[test]
    fn hgetall_parse_error_on_integer() {
        let cmd = HGetAll::new("h");
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    // -- HIncrBy --

    #[test]
    fn hincrby_to_frame() {
        let cmd = HIncrBy::new("h", "f", 5);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("HINCRBY"), bulk("h"), bulk("f"), bulk("5")])
        );
    }

    // -- HIncrByFloat --

    #[test]
    fn hincrbyfloat_to_frame() {
        let cmd = HIncrByFloat::new("h", "f", 1.5);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("HINCRBYFLOAT"),
                bulk("h"),
                bulk("f"),
                bulk("1.5")
            ])
        );
    }

    #[test]
    fn hincrbyfloat_parse_bulk_string() {
        let cmd = HIncrByFloat::new("h", "f", 1.0);
        let frame = Frame::BulkString(Some(Bytes::from("3.5")));
        let result = cmd.parse_response(frame).unwrap();
        assert!((result - 3.5).abs() < f64::EPSILON);
    }

    #[test]
    fn hincrbyfloat_parse_double() {
        let cmd = HIncrByFloat::new("h", "f", 1.0);
        let result = cmd.parse_response(Frame::Double(3.5)).unwrap();
        assert!((result - 3.5).abs() < f64::EPSILON);
    }

    // -- HSetNx --

    #[test]
    fn hsetnx_to_frame() {
        let cmd = HSetNx::new("h", "f", "v");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("HSETNX"), bulk("h"), bulk("f"), bulk("v")])
        );
    }

    #[test]
    fn hsetnx_parse_true() {
        let cmd = HSetNx::new("h", "f", "v");
        assert!(cmd.parse_response(Frame::Integer(1)).unwrap());
    }

    // -- HExpire --

    #[test]
    fn hexpire_to_frame() {
        let cmd = HExpire::new("h", 60, vec!["f1", "f2"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("HEXPIRE"),
                bulk("h"),
                bulk("60"),
                bulk("FIELDS"),
                bulk("2"),
                bulk("f1"),
                bulk("f2"),
            ])
        );
    }

    #[test]
    fn hexpire_parse_per_field() {
        let cmd = HExpire::new("h", 60, vec!["f1", "f2"]);
        let frame = array(vec![Frame::Integer(1), Frame::Integer(0)]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![1, 0]);
    }

    // -- HRandField --

    #[test]
    fn hrandfield_to_frame_no_count() {
        let cmd = HRandField::new("h");
        assert_eq!(cmd.to_frame(), array(vec![bulk("HRANDFIELD"), bulk("h")]));
    }

    #[test]
    fn hrandfield_to_frame_with_count() {
        let cmd = HRandField::new("h").count(3);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("HRANDFIELD"), bulk("h"), bulk("3")])
        );
    }

    #[test]
    fn hrandfield_parse_single() {
        let cmd = HRandField::new("h");
        let frame = Frame::BulkString(Some(Bytes::from("field1")));
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![Bytes::from("field1")]);
    }

    #[test]
    fn hrandfield_parse_null_empty() {
        let cmd = HRandField::new("h");
        let result = cmd.parse_response(Frame::Null).unwrap();
        assert!(result.is_empty());
    }

    // -- HMGet --

    #[test]
    fn hmget_to_frame() {
        let cmd = HMGet::fields("h", vec!["f1", "f2"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("HMGET"), bulk("h"), bulk("f1"), bulk("f2")])
        );
    }

    #[test]
    fn hmget_builder_to_frame() {
        let cmd = HMGet::new("h", "f1").field("f2");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("HMGET"), bulk("h"), bulk("f1"), bulk("f2")])
        );
    }

    #[test]
    fn hmget_parse_mixed() {
        let cmd = HMGet::fields("h", vec!["f1", "f2"]);
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("v1"))),
            Frame::Null,
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![Some(Bytes::from("v1")), None]);
    }

    // -- HStrLen --

    #[test]
    fn hstrlen_to_frame() {
        let cmd = HStrLen::new("h", "f");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("HSTRLEN"), bulk("h"), bulk("f")])
        );
    }

    #[test]
    fn hstrlen_parse_integer() {
        let cmd = HStrLen::new("h", "f");
        assert_eq!(cmd.parse_response(Frame::Integer(5)).unwrap(), 5);
    }

    // -- HPExpireTime --

    #[test]
    fn hpexpiretime_to_frame() {
        let cmd = HPExpireTime::new("h", vec!["f1", "f2"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("HPEXPIRETIME"),
                bulk("h"),
                bulk("FIELDS"),
                bulk("2"),
                bulk("f1"),
                bulk("f2"),
            ])
        );
    }

    #[test]
    fn hpexpiretime_parse_per_field() {
        let cmd = HPExpireTime::new("h", vec!["f1"]);
        let frame = array(vec![Frame::Integer(1700000000000)]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![1700000000000]);
    }
}
