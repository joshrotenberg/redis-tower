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
