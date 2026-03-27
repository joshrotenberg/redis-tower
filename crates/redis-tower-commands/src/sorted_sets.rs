use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// ZADD key score member \[score member ...\]
///
/// Adds the specified members with scores to the sorted set stored at `key`.
/// Returns the number of members added (excluding members already present
/// whose score was updated).
pub struct ZAdd {
    key: String,
    members: Vec<(f64, String)>,
}

impl ZAdd {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            members: Vec::new(),
        }
    }

    /// Adds a member with the given score.
    pub fn member(mut self, score: f64, member: impl Into<String>) -> Self {
        self.members.push((score, member.into()));
        self
    }
}

impl Command for ZAdd {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZADD"), bulk(self.key.as_str())];
        for (score, member) in &self.members {
            args.push(bulk(score.to_string()));
            args.push(bulk(member.as_str()));
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
        "ZADD"
    }
}

/// ZREM key member \[member ...\]
///
/// Removes the specified members from the sorted set stored at `key`. Returns
/// the number of members that were removed.
pub struct ZRem {
    key: String,
    members: Vec<String>,
}

impl ZRem {
    pub fn new(key: impl Into<String>, member: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            members: vec![member.into()],
        }
    }

    pub fn members(
        key: impl Into<String>,
        members: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            members: members.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for ZRem {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZREM"), bulk(self.key.as_str())];
        for member in &self.members {
            args.push(bulk(member.as_str()));
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
        "ZREM"
    }
}

/// ZRANGE key start stop
///
/// Returns the specified range of members in the sorted set stored at `key`,
/// ordered from lowest to highest score. `start` and `stop` are zero-based
/// indices, where -1 is the last element.
pub struct ZRange {
    key: String,
    start: i64,
    stop: i64,
}

impl ZRange {
    pub fn new(key: impl Into<String>, start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            start,
            stop,
        }
    }
}

impl Command for ZRange {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZRANGE"),
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
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "ZRANGE"
    }
}

/// ZSCORE key member
///
/// Returns the score of `member` in the sorted set at `key`, or `None` if
/// the \[member\] or key does not exist.
pub struct ZScore {
    key: String,
    member: String,
}

impl ZScore {
    pub fn new(key: impl Into<String>, member: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            member: member.into(),
        }
    }
}

impl Command for ZScore {
    type Response = Option<f64>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZSCORE"),
            bulk(self.key.as_str()),
            bulk(self.member.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(&data);
                let score = s
                    .parse::<f64>()
                    .map_err(|_| RedisError::UnexpectedResponse {
                        expected: "float string",
                        actual: format!("{s}"),
                    })?;
                Ok(Some(score))
            }
            Frame::BulkString(None) | Frame::Null => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "ZSCORE"
    }
}

/// ZCARD key
///
/// Returns the number of members in the sorted set stored at `key`.
pub struct ZCard {
    key: String,
}

impl ZCard {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ZCard {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("ZCARD"), bulk(self.key.as_str())])
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
        "ZCARD"
    }
}

/// ZINCRBY key increment member
///
/// Increments the score of `member` in the sorted set at `key` by
/// `increment`. Returns the new score of the \[member\].
pub struct ZIncrBy {
    key: String,
    increment: f64,
    member: String,
}

impl ZIncrBy {
    pub fn new(key: impl Into<String>, increment: f64, member: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            increment,
            member: member.into(),
        }
    }
}

impl Command for ZIncrBy {
    type Response = f64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZINCRBY"),
            bulk(self.key.as_str()),
            bulk(self.increment.to_string()),
            bulk(self.member.as_str()),
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
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "ZINCRBY"
    }
}

/// ZRANK key member
///
/// Returns the rank of `member` in the sorted set at `key` (zero-based,
/// lowest score = rank 0), or `None` if the \[member\] or key does not exist.
pub struct ZRank {
    key: String,
    member: String,
}

impl ZRank {
    pub fn new(key: impl Into<String>, member: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            member: member.into(),
        }
    }
}

impl Command for ZRank {
    type Response = Option<i64>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZRANK"),
            bulk(self.key.as_str()),
            bulk(self.member.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(Some(n)),
            Frame::Null | Frame::BulkString(None) => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "ZRANK"
    }
}

/// ZRANGEBYSCORE key min max
///
/// Returns all members in the sorted set at `key` with a score between
/// `min` and `max` (inclusive). The `min` and `max` arguments can be
/// `"-inf"`, `"+inf"`, or numeric strings.
pub struct ZRangeByScore {
    key: String,
    min: String,
    max: String,
}

impl ZRangeByScore {
    pub fn new(key: impl Into<String>, min: impl Into<String>, max: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            min: min.into(),
            max: max.into(),
        }
    }
}

impl Command for ZRangeByScore {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZRANGEBYSCORE"),
            bulk(self.key.as_str()),
            bulk(self.min.as_str()),
            bulk(self.max.as_str()),
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
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "ZRANGEBYSCORE"
    }
}
