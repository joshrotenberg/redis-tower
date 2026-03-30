use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// DEL key [key ...]
///
/// Removes the specified keys. Returns the number of keys removed.
pub struct Del {
    keys: Vec<String>,
}

impl Del {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for Del {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("DEL")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
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
        "DEL"
    }
}

/// EXISTS key [key ...]
///
/// Returns the number of specified keys that exist.
pub struct Exists {
    keys: Vec<String>,
}

impl Exists {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for Exists {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("EXISTS")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
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
        "EXISTS"
    }
}

/// EXPIRE key seconds
///
/// Sets a timeout on `key`. Returns `true` if the timeout was set.
pub struct Expire {
    key: String,
    seconds: u64,
}

impl Expire {
    pub fn new(key: impl Into<String>, seconds: u64) -> Self {
        Self {
            key: key.into(),
            seconds,
        }
    }
}

impl Command for Expire {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("EXPIRE"),
            bulk(self.key.as_str()),
            bulk(self.seconds.to_string()),
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
        "EXPIRE"
    }
}

/// TTL key
///
/// Returns the remaining time to live of a key in seconds.
/// Returns -2 if the key does not exist, -1 if no expiry is set.
pub struct Ttl {
    key: String,
}

impl Ttl {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Ttl {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("TTL"), bulk(self.key.as_str())])
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
        "TTL"
    }
}

/// RENAME key newkey
///
/// Renames `key` to `newkey`. Errors if `key` does not exist.
pub struct Rename {
    key: String,
    new_key: String,
}

impl Rename {
    pub fn new(key: impl Into<String>, new_key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            new_key: new_key.into(),
        }
    }
}

impl Command for Rename {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("RENAME"),
            bulk(self.key.as_str()),
            bulk(self.new_key.as_str()),
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
        "RENAME"
    }
}

/// TYPE key
///
/// Returns the type of the value stored at `key` as a string
/// (e.g., "string", "list", "set", "zset", "hash", "none").
pub struct Type {
    key: String,
}

impl Type {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Type {
    type Response = String;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("TYPE"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "simple string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "TYPE"
    }
}

/// UNLINK key [key ...]
///
/// Removes the specified keys without blocking the server.
/// Returns the number of keys removed.
pub struct Unlink {
    keys: Vec<String>,
}

impl Unlink {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for Unlink {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("UNLINK")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
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
        "UNLINK"
    }
}

/// PERSIST key
///
/// Removes the existing timeout on `key`. Returns `true` if the timeout was removed.
pub struct Persist {
    key: String,
}

impl Persist {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Persist {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("PERSIST"), bulk(self.key.as_str())])
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
        "PERSIST"
    }
}

/// PEXPIRE key milliseconds
///
/// Sets a timeout on `key` in milliseconds. Returns `true` if the timeout was set.
pub struct PExpire {
    key: String,
    milliseconds: u64,
}

impl PExpire {
    pub fn new(key: impl Into<String>, milliseconds: u64) -> Self {
        Self {
            key: key.into(),
            milliseconds,
        }
    }
}

impl Command for PExpire {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("PEXPIRE"),
            bulk(self.key.as_str()),
            bulk(self.milliseconds.to_string()),
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
        "PEXPIRE"
    }
}

/// PEXPIREAT key ms-timestamp
///
/// Sets an expiry on `key` as an absolute Unix timestamp in milliseconds.
/// Returns `true` if the timeout was set.
pub struct PExpireAt {
    key: String,
    ms_timestamp: i64,
}

impl PExpireAt {
    pub fn new(key: impl Into<String>, ms_timestamp: i64) -> Self {
        Self {
            key: key.into(),
            ms_timestamp,
        }
    }
}

impl Command for PExpireAt {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("PEXPIREAT"),
            bulk(self.key.as_str()),
            bulk(self.ms_timestamp.to_string()),
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
        "PEXPIREAT"
    }
}

/// COPY source destination \[REPLACE\]
///
/// Copies the value stored at `source` to `destination`.
/// Returns `true` if the key was copied.
pub struct Copy {
    source: String,
    destination: String,
    replace: bool,
}

impl Copy {
    pub fn new(source: impl Into<String>, destination: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
            replace: false,
        }
    }

    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }
}

impl Command for Copy {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("COPY"),
            bulk(self.source.as_str()),
            bulk(self.destination.as_str()),
        ];
        if self.replace {
            args.push(bulk("REPLACE"));
        }
        array(args)
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
        "COPY"
    }
}

/// KEYS pattern
///
/// Returns all keys matching `pattern`.
pub struct Keys {
    pattern: String,
}

impl Keys {
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
        }
    }
}

impl Command for Keys {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("KEYS"), bulk(self.pattern.as_str())])
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
        "KEYS"
    }
}

/// RANDOMKEY
///
/// Returns a random key from the keyspace, or `None` if the database is empty.
pub struct RandomKey;

impl RandomKey {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RandomKey {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for RandomKey {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("RANDOMKEY")])
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
        "RANDOMKEY"
    }
}

/// TOUCH key [key ...]
///
/// Alters the last access time of the specified keys.
/// Returns the number of keys that were touched.
pub struct Touch {
    keys: Vec<String>,
}

impl Touch {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for Touch {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TOUCH")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
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
        "TOUCH"
    }
}

/// EXPIRETIME key
///
/// Returns the absolute Unix timestamp (in seconds) at which the key will expire.
/// Returns -1 if the key exists but has no expiry, -2 if the key does not exist.
pub struct ExpireTime {
    key: String,
}

impl ExpireTime {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ExpireTime {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("EXPIRETIME"), bulk(self.key.as_str())])
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
        "EXPIRETIME"
    }
}

/// PEXPIRETIME key
///
/// Returns the absolute Unix timestamp (in milliseconds) at which the key will expire.
/// Returns -1 if the key exists but has no expiry, -2 if the key does not exist.
pub struct PExpireTime {
    key: String,
}

impl PExpireTime {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for PExpireTime {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("PEXPIRETIME"), bulk(self.key.as_str())])
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
        "PEXPIRETIME"
    }
}
