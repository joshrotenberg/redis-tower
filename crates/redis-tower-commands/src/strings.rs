use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// GET key
///
/// Returns the value of `key`, or `None` if the key does not exist.
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Get {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("GET"), bulk(self.key.as_str())])
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
        "GET"
    }
}

/// SET key value [EX seconds] [PX milliseconds] [NX|XX] [GET]
///
/// Sets `key` to hold `value`. Returns `Ok` on success, or the old value
/// if `GET` is specified.
pub struct Set {
    key: String,
    value: String,
    ex: Option<u64>,
    px: Option<u64>,
    condition: Option<SetCondition>,
    get: bool,
}

/// Condition for SET (NX or XX).
pub enum SetCondition {
    /// Only set if the key does not exist.
    Nx,
    /// Only set if the key already exists.
    Xx,
}

impl Set {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            ex: None,
            px: None,
            condition: None,
            get: false,
        }
    }

    /// Set expiration in seconds.
    pub fn ex(mut self, seconds: u64) -> Self {
        self.ex = Some(seconds);
        self.px = None;
        self
    }

    /// Set expiration in milliseconds.
    pub fn px(mut self, milliseconds: u64) -> Self {
        self.px = Some(milliseconds);
        self.ex = None;
        self
    }

    /// Only set if the key does not exist.
    pub fn nx(mut self) -> Self {
        self.condition = Some(SetCondition::Nx);
        self
    }

    /// Only set if the key already exists.
    pub fn xx(mut self) -> Self {
        self.condition = Some(SetCondition::Xx);
        self
    }

    /// Return the old value stored at `key`.
    pub fn get(mut self) -> Self {
        self.get = true;
        self
    }
}

impl Command for Set {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("SET"),
            bulk(self.key.as_str()),
            bulk(self.value.as_str()),
        ];

        if let Some(ex) = self.ex {
            args.push(bulk("EX"));
            args.push(bulk(ex.to_string()));
        }
        if let Some(px) = self.px {
            args.push(bulk("PX"));
            args.push(bulk(px.to_string()));
        }
        match &self.condition {
            Some(SetCondition::Nx) => args.push(bulk("NX")),
            Some(SetCondition::Xx) => args.push(bulk("XX")),
            None => {}
        }
        if self.get {
            args.push(bulk("GET"));
        }

        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(None),
            Frame::BulkString(data) => Ok(data),
            Frame::Null => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK, bulk string, or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SET"
    }
}

/// INCR key
///
/// Increments the integer value of `key` by one.
pub struct Incr {
    key: String,
}

impl Incr {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Incr {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("INCR"), bulk(self.key.as_str())])
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
        "INCR"
    }
}

/// MGET key [key ...]
///
/// Returns the values of all specified keys.
pub struct MGet {
    keys: Vec<String>,
}

impl MGet {
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for MGet {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("MGET")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
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
        "MGET"
    }
}
