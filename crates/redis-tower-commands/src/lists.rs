use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// LPUSH key element \[element ...\]
///
/// Prepends one or more elements to the head of the list stored at `key`.
/// Returns the length of the list after the push operation.
pub struct LPush {
    key: String,
    elements: Vec<String>,
}

impl LPush {
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            elements: vec![element.into()],
        }
    }

    pub fn elements(
        key: impl Into<String>,
        elements: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            elements: elements.into_iter().map(Into::into).collect(),
        }
    }

    /// Add another element to push.
    pub fn element(mut self, element: impl Into<String>) -> Self {
        self.elements.push(element.into());
        self
    }
}

impl Command for LPush {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("LPUSH"), bulk(self.key.as_str())];
        for element in &self.elements {
            args.push(bulk(element.as_str()));
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
        "LPUSH"
    }
}

/// RPUSH key element \[element ...\]
///
/// Appends one or more elements to the tail of the list stored at `key`.
/// Returns the length of the list after the push operation.
pub struct RPush {
    key: String,
    elements: Vec<String>,
}

impl RPush {
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            elements: vec![element.into()],
        }
    }

    pub fn elements(
        key: impl Into<String>,
        elements: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            elements: elements.into_iter().map(Into::into).collect(),
        }
    }

    /// Add another element to push.
    pub fn element(mut self, element: impl Into<String>) -> Self {
        self.elements.push(element.into());
        self
    }
}

impl Command for RPush {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("RPUSH"), bulk(self.key.as_str())];
        for element in &self.elements {
            args.push(bulk(element.as_str()));
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
        "RPUSH"
    }
}

/// LPOP key
///
/// Removes and returns the first element of the list stored at `key`.
/// Returns `None` if the key does not exist.
pub struct LPop {
    key: String,
}

impl LPop {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for LPop {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("LPOP"), bulk(self.key.as_str())])
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
        "LPOP"
    }
}

/// RPOP key
///
/// Removes and returns the last element of the list stored at `key`.
/// Returns `None` if the key does not exist.
pub struct RPop {
    key: String,
}

impl RPop {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for RPop {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("RPOP"), bulk(self.key.as_str())])
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
        "RPOP"
    }
}

/// LRANGE key start stop
///
/// Returns the specified elements of the list stored at `key`. The offsets
/// `start` and `stop` are zero-based indices, with negative values counting
/// from the end of the list.
pub struct LRange {
    key: String,
    start: i64,
    stop: i64,
}

impl LRange {
    pub fn new(key: impl Into<String>, start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            start,
            stop,
        }
    }
}

impl Command for LRange {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LRANGE"),
            bulk(self.key.as_str()),
            bulk(self.start.to_string()),
            bulk(self.stop.to_string()),
        ])
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
            Frame::Array(None) => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "LRANGE"
    }
}

/// LLEN key
///
/// Returns the length of the list stored at `key`. If the key does not
/// exist, it is interpreted as an empty list and 0 is returned.
pub struct LLen {
    key: String,
}

impl LLen {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for LLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("LLEN"), bulk(self.key.as_str())])
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
        "LLEN"
    }
}

/// LINDEX key index
///
/// Returns the element at `index` in the list stored at `key`. The index
/// is zero-based, with negative values counting from the end of the list.
/// Returns `None` if the index is out of range.
pub struct LIndex {
    key: String,
    index: i64,
}

impl LIndex {
    pub fn new(key: impl Into<String>, index: i64) -> Self {
        Self {
            key: key.into(),
            index,
        }
    }
}

impl Command for LIndex {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LINDEX"),
            bulk(self.key.as_str()),
            bulk(self.index.to_string()),
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
        "LINDEX"
    }
}

/// LSET key index element
///
/// Sets the list element at `index` to `element`. An error is returned for
/// out-of-range indices.
pub struct LSet {
    key: String,
    index: i64,
    element: String,
}

impl LSet {
    pub fn new(key: impl Into<String>, index: i64, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            index,
            element: element.into(),
        }
    }
}

impl Command for LSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LSET"),
            bulk(self.key.as_str()),
            bulk(self.index.to_string()),
            bulk(self.element.as_str()),
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
        "LSET"
    }
}
