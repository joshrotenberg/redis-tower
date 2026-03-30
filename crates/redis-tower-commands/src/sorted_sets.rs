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
            Frame::Double(d) => Ok(Some(d)),
            Frame::BulkString(None) | Frame::Null => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string, double, or null",
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
            Frame::Double(d) => Ok(d),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or double",
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

/// ZPOPMIN key \[count\]
///
/// Removes and returns the members with the lowest scores in the sorted set
/// stored at `key`. Returns a list of `(member, score)` pairs.
pub struct ZPopMin {
    key: String,
    count: Option<i64>,
}

impl ZPopMin {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    /// Sets the number of members to pop.
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for ZPopMin {
    type Response = Vec<(Bytes, f64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZPOPMIN"), bulk(self.key.as_str())];
        if let Some(count) = self.count {
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => {
                if frames.len() % 2 != 0 {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "even number of elements (member/score pairs)",
                        actual: format!("array of length {}", frames.len()),
                    });
                }
                frames
                    .chunks(2)
                    .map(|pair| {
                        let member = match &pair[0] {
                            Frame::BulkString(Some(data)) => data.clone(),
                            other => {
                                return Err(RedisError::UnexpectedResponse {
                                    expected: "bulk string",
                                    actual: format!("{other:?}"),
                                });
                            }
                        };
                        let score = match &pair[1] {
                            Frame::BulkString(Some(data)) => {
                                let s = String::from_utf8_lossy(data);
                                s.parse::<f64>()
                                    .map_err(|_| RedisError::UnexpectedResponse {
                                        expected: "float string",
                                        actual: format!("{s}"),
                                    })?
                            }
                            Frame::Double(d) => *d,
                            other => {
                                return Err(RedisError::UnexpectedResponse {
                                    expected: "bulk string or double",
                                    actual: format!("{other:?}"),
                                });
                            }
                        };
                        Ok((member, score))
                    })
                    .collect()
            }
            Frame::Array(None) => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "ZPOPMIN"
    }
}

/// ZPOPMAX key \[count\]
///
/// Removes and returns the members with the highest scores in the sorted set
/// stored at `key`. Returns a list of `(member, score)` pairs.
pub struct ZPopMax {
    key: String,
    count: Option<i64>,
}

impl ZPopMax {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    /// Sets the number of members to pop.
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for ZPopMax {
    type Response = Vec<(Bytes, f64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZPOPMAX"), bulk(self.key.as_str())];
        if let Some(count) = self.count {
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => {
                if frames.len() % 2 != 0 {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "even number of elements (member/score pairs)",
                        actual: format!("array of length {}", frames.len()),
                    });
                }
                frames
                    .chunks(2)
                    .map(|pair| {
                        let member = match &pair[0] {
                            Frame::BulkString(Some(data)) => data.clone(),
                            other => {
                                return Err(RedisError::UnexpectedResponse {
                                    expected: "bulk string",
                                    actual: format!("{other:?}"),
                                });
                            }
                        };
                        let score = match &pair[1] {
                            Frame::BulkString(Some(data)) => {
                                let s = String::from_utf8_lossy(data);
                                s.parse::<f64>()
                                    .map_err(|_| RedisError::UnexpectedResponse {
                                        expected: "float string",
                                        actual: format!("{s}"),
                                    })?
                            }
                            Frame::Double(d) => *d,
                            other => {
                                return Err(RedisError::UnexpectedResponse {
                                    expected: "bulk string or double",
                                    actual: format!("{other:?}"),
                                });
                            }
                        };
                        Ok((member, score))
                    })
                    .collect()
            }
            Frame::Array(None) => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "ZPOPMAX"
    }
}

/// ZCOUNT key min max
///
/// Returns the number of members in the sorted set at `key` with a score
/// between `min` and `max` (inclusive by default). The `min` and `max`
/// arguments can be `"-inf"`, `"+inf"`, or numeric strings (prefix with
/// `"("` for exclusive bounds).
pub struct ZCount {
    key: String,
    min: String,
    max: String,
}

impl ZCount {
    pub fn new(key: impl Into<String>, min: impl Into<String>, max: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            min: min.into(),
            max: max.into(),
        }
    }
}

impl Command for ZCount {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZCOUNT"),
            bulk(self.key.as_str()),
            bulk(self.min.as_str()),
            bulk(self.max.as_str()),
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
        "ZCOUNT"
    }
}

/// ZLEXCOUNT key min max
///
/// Returns the number of members in the sorted set at `key` between the
/// lexicographical range specified by `min` and `max`. Valid values for
/// `min` and `max` are `"-"`, `"+"`, `"[value"` (inclusive), or `"(value"`
/// (exclusive).
pub struct ZLexCount {
    key: String,
    min: String,
    max: String,
}

impl ZLexCount {
    pub fn new(key: impl Into<String>, min: impl Into<String>, max: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            min: min.into(),
            max: max.into(),
        }
    }
}

impl Command for ZLexCount {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZLEXCOUNT"),
            bulk(self.key.as_str()),
            bulk(self.min.as_str()),
            bulk(self.max.as_str()),
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
        "ZLEXCOUNT"
    }
}

/// ZRANDMEMBER key \[count\]
///
/// Returns one or more random members from the sorted set at `key`.
/// When called without `count`, returns a single random member.
/// When called with `count`, returns up to that many distinct members.
pub struct ZRandMember {
    key: String,
    count: Option<i64>,
}

impl ZRandMember {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    /// Sets the number of members to return.
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for ZRandMember {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZRANDMEMBER"), bulk(self.key.as_str())];
        if let Some(count) = self.count {
            args.push(bulk(count.to_string()));
        }
        array(args)
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
            Frame::BulkString(Some(data)) => Ok(vec![data]),
            Frame::BulkString(None) | Frame::Null => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "ZRANDMEMBER"
    }
}

/// ZMSCORE key member \[member ...\]
///
/// Returns the scores associated with the specified members in the sorted
/// set at `key`. For each member, returns `Some(score)` if the member
/// exists, or `None` if it does not.
pub struct ZMScore {
    key: String,
    members: Vec<String>,
}

impl ZMScore {
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

impl Command for ZMScore {
    type Response = Vec<Option<f64>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZMSCORE"), bulk(self.key.as_str())];
        for member in &self.members {
            args.push(bulk(member.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(data)) => {
                        let s = String::from_utf8_lossy(&data);
                        let score =
                            s.parse::<f64>()
                                .map_err(|_| RedisError::UnexpectedResponse {
                                    expected: "float string",
                                    actual: format!("{s}"),
                                })?;
                        Ok(Some(score))
                    }
                    Frame::Double(d) => Ok(Some(d)),
                    Frame::BulkString(None) | Frame::Null => Ok(None),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string, double, or null",
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
        "ZMSCORE"
    }
}
