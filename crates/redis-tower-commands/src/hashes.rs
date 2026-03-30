use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// HGET key field
///
/// Returns the value associated with `field` in the hash stored at `key`,
/// or `None` if the field or key does not exist.
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
}

/// HSET key field value \[field value ...\]
///
/// Sets one or more field-value pairs in the hash stored at `key`.
/// Returns the number of fields that were added (not updated).
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
}

/// HGETALL key
///
/// Returns all fields and values of the hash stored at `key` as a list
/// of `(field, value)` pairs.
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
}

/// HINCRBY key field increment
///
/// Increments the integer value of `field` in the hash stored at `key`
/// by `increment`. Returns the new value.
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
}

/// HVALS key
///
/// Returns all values in the hash stored at `key`.
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
}

/// HLEN key
///
/// Returns the number of fields contained in the hash stored at `key`.
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
}

/// HPTTL key FIELDS numfields field [field ...]
///
/// Returns the remaining TTL (in milliseconds) for the specified hash fields.
/// Returns one value per field.
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
}

/// HPERSIST key FIELDS numfields field [field ...]
///
/// Removes the expiration from the specified hash fields.
/// Returns one status code per field.
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
}

/// HEXPIRETIME key FIELDS numfields field [field ...]
///
/// Returns the absolute Unix expiration timestamp (in seconds) for the
/// specified hash fields. Returns one value per field.
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
}
