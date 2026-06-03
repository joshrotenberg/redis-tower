use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// ZADD key score member \[score member ...\]
///
/// Adds the specified members with scores to the sorted set stored at `key`.
/// Returns the number of members added (excluding members already present
/// whose score was updated).
///
/// See: <https://redis.io/commands/zadd>
pub struct ZAdd {
    key: String,
    members: Vec<(f64, String)>,
}

impl ZAdd {
    /// Creates a new [`ZAdd`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            members: Vec::new(),
        }
    }

    /// Adds a member with the given score.
    #[must_use]
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
///
/// See: <https://redis.io/commands/zrem>
pub struct ZRem {
    key: String,
    members: Vec<String>,
}

impl ZRem {
    /// Creates a new [`ZRem`] command.
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
///
/// See: <https://redis.io/commands/zrange>
pub struct ZRange {
    key: String,
    start: i64,
    stop: i64,
}

impl ZRange {
    /// Creates a new [`ZRange`] command.
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
///
/// See: <https://redis.io/commands/zscore>
pub struct ZScore {
    key: String,
    member: String,
}

impl ZScore {
    /// Creates a new [`ZScore`] command.
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
///
/// See: <https://redis.io/commands/zcard>
pub struct ZCard {
    key: String,
}

impl ZCard {
    /// Creates a new [`ZCard`] command.
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
///
/// See: <https://redis.io/commands/zincrby>
pub struct ZIncrBy {
    key: String,
    increment: f64,
    member: String,
}

impl ZIncrBy {
    /// Creates a new [`ZIncrBy`] command.
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
///
/// See: <https://redis.io/commands/zrank>
pub struct ZRank {
    key: String,
    member: String,
}

impl ZRank {
    /// Creates a new [`ZRank`] command.
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
///
/// See: <https://redis.io/commands/zrangebyscore>
pub struct ZRangeByScore {
    key: String,
    min: String,
    max: String,
}

impl ZRangeByScore {
    /// Creates a new [`ZRangeByScore`] command.
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
///
/// See: <https://redis.io/commands/zpopmin>
pub struct ZPopMin {
    key: String,
    count: Option<i64>,
}

impl ZPopMin {
    /// Creates a new [`ZPopMin`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    /// Sets the number of members to pop.
    #[must_use]
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
///
/// See: <https://redis.io/commands/zpopmax>
pub struct ZPopMax {
    key: String,
    count: Option<i64>,
}

impl ZPopMax {
    /// Creates a new [`ZPopMax`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    /// Sets the number of members to pop.
    #[must_use]
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
///
/// See: <https://redis.io/commands/zcount>
pub struct ZCount {
    key: String,
    min: String,
    max: String,
}

impl ZCount {
    /// Creates a new [`ZCount`] command.
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
///
/// See: <https://redis.io/commands/zlexcount>
pub struct ZLexCount {
    key: String,
    min: String,
    max: String,
}

impl ZLexCount {
    /// Creates a new [`ZLexCount`] command.
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
///
/// See: <https://redis.io/commands/zrandmember>
pub struct ZRandMember {
    key: String,
    count: Option<i64>,
}

impl ZRandMember {
    /// Creates a new [`ZRandMember`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    /// Sets the number of members to return.
    #[must_use]
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
///
/// See: <https://redis.io/commands/zmscore>
pub struct ZMScore {
    key: String,
    members: Vec<String>,
}

impl ZMScore {
    /// Creates a new [`ZMScore`] command.
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

/// Aggregation function used by ZINTERSTORE, ZUNIONSTORE, and similar commands.
#[derive(Debug, Clone, Copy)]
pub enum Aggregate {
    Sum,
    Min,
    Max,
}

impl Aggregate {
    fn as_str(&self) -> &str {
        match self {
            Aggregate::Sum => "SUM",
            Aggregate::Min => "MIN",
            Aggregate::Max => "MAX",
        }
    }
}

/// ZINTERSTORE destination numkeys key \[key ...\] \[WEIGHTS weight ...\] \[AGGREGATE SUM|MIN|MAX\]
///
/// Computes the intersection of the sorted sets given by the specified keys,
/// and stores the result in `destination`. Returns the number of elements in
/// the resulting sorted set.
///
/// See: <https://redis.io/commands/zinterstore>
pub struct ZInterStore {
    destination: String,
    keys: Vec<String>,
    weights: Option<Vec<f64>>,
    aggregate: Option<Aggregate>,
}

impl ZInterStore {
    /// Creates a new [`ZInterStore`] command.
    pub fn new(
        destination: impl Into<String>,
        keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            destination: destination.into(),
            keys: keys.into_iter().map(Into::into).collect(),
            weights: None,
            aggregate: None,
        }
    }

    /// Sets the weight multipliers for each input sorted set.
    #[must_use]
    pub fn weights(mut self, weights: impl IntoIterator<Item = f64>) -> Self {
        self.weights = Some(weights.into_iter().collect());
        self
    }

    /// Sets the aggregation function for combining scores.
    #[must_use]
    pub fn aggregate(mut self, aggregate: Aggregate) -> Self {
        self.aggregate = Some(aggregate);
        self
    }
}

impl Command for ZInterStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("ZINTERSTORE"),
            bulk(self.destination.as_str()),
            bulk(self.keys.len().to_string()),
        ];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        if let Some(weights) = &self.weights {
            args.push(bulk("WEIGHTS"));
            for w in weights {
                args.push(bulk(w.to_string()));
            }
        }
        if let Some(agg) = &self.aggregate {
            args.push(bulk("AGGREGATE"));
            args.push(bulk(agg.as_str()));
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
        "ZINTERSTORE"
    }
}

/// ZUNIONSTORE destination numkeys key \[key ...\] \[WEIGHTS weight ...\] \[AGGREGATE SUM|MIN|MAX\]
///
/// Computes the union of the sorted sets given by the specified keys, and
/// stores the result in `destination`. Returns the number of elements in
/// the resulting sorted set.
///
/// See: <https://redis.io/commands/zunionstore>
pub struct ZUnionStore {
    destination: String,
    keys: Vec<String>,
    weights: Option<Vec<f64>>,
    aggregate: Option<Aggregate>,
}

impl ZUnionStore {
    /// Creates a new [`ZUnionStore`] command.
    pub fn new(
        destination: impl Into<String>,
        keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            destination: destination.into(),
            keys: keys.into_iter().map(Into::into).collect(),
            weights: None,
            aggregate: None,
        }
    }

    /// Sets the weight multipliers for each input sorted set.
    #[must_use]
    pub fn weights(mut self, weights: impl IntoIterator<Item = f64>) -> Self {
        self.weights = Some(weights.into_iter().collect());
        self
    }

    /// Sets the aggregation function for combining scores.
    #[must_use]
    pub fn aggregate(mut self, aggregate: Aggregate) -> Self {
        self.aggregate = Some(aggregate);
        self
    }
}

impl Command for ZUnionStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("ZUNIONSTORE"),
            bulk(self.destination.as_str()),
            bulk(self.keys.len().to_string()),
        ];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        if let Some(weights) = &self.weights {
            args.push(bulk("WEIGHTS"));
            for w in weights {
                args.push(bulk(w.to_string()));
            }
        }
        if let Some(agg) = &self.aggregate {
            args.push(bulk("AGGREGATE"));
            args.push(bulk(agg.as_str()));
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
        "ZUNIONSTORE"
    }
}

/// ZDIFFSTORE destination numkeys key \[key ...\]
///
/// Computes the difference between the first sorted set and all successive
/// sorted sets given by the specified keys, and stores the result in
/// `destination`. Returns the number of elements in the resulting sorted set.
///
/// See: <https://redis.io/commands/zdiffstore>
pub struct ZDiffStore {
    destination: String,
    keys: Vec<String>,
}

impl ZDiffStore {
    /// Creates a new [`ZDiffStore`] command.
    pub fn new(
        destination: impl Into<String>,
        keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            destination: destination.into(),
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for ZDiffStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("ZDIFFSTORE"),
            bulk(self.destination.as_str()),
            bulk(self.keys.len().to_string()),
        ];
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
        "ZDIFFSTORE"
    }
}

/// ZINTERCARD numkeys key \[key ...\] \[LIMIT limit\]
///
/// Returns the cardinality of the intersection of the sorted sets given by
/// the specified keys. The optional `LIMIT` argument caps the work done
/// when the intersection cardinality reaches the limit.
///
/// See: <https://redis.io/commands/zintercard>
pub struct ZInterCard {
    keys: Vec<String>,
    limit: Option<i64>,
}

impl ZInterCard {
    /// Creates a new [`ZInterCard`] command.
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            limit: None,
        }
    }

    /// Sets the upper bound for the returned cardinality.
    #[must_use]
    pub fn limit(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl Command for ZInterCard {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZINTERCARD"), bulk(self.keys.len().to_string())];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        if let Some(limit) = self.limit {
            args.push(bulk("LIMIT"));
            args.push(bulk(limit.to_string()));
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
        "ZINTERCARD"
    }
}

/// ZRANGESTORE dst src min max \[BYSCORE|BYLEX\] \[REV\] \[LIMIT offset count\]
///
/// Stores the specified range of members from the sorted set at `src` into
/// `dst`. Returns the number of elements in the resulting sorted set.
///
/// See: <https://redis.io/commands/zrangestore>
pub struct ZRangeStore {
    dst: String,
    src: String,
    min: String,
    max: String,
    by_score: bool,
    by_lex: bool,
    rev: bool,
    limit: Option<(i64, i64)>,
}

impl ZRangeStore {
    /// Creates a new [`ZRangeStore`] command.
    pub fn new(
        dst: impl Into<String>,
        src: impl Into<String>,
        min: impl Into<String>,
        max: impl Into<String>,
    ) -> Self {
        Self {
            dst: dst.into(),
            src: src.into(),
            min: min.into(),
            max: max.into(),
            by_score: false,
            by_lex: false,
            rev: false,
            limit: None,
        }
    }

    /// Uses score-based range interpretation.
    #[must_use]
    pub fn by_score(mut self) -> Self {
        self.by_score = true;
        self.by_lex = false;
        self
    }

    /// Uses lexicographic range interpretation.
    #[must_use]
    pub fn by_lex(mut self) -> Self {
        self.by_lex = true;
        self.by_score = false;
        self
    }

    /// Reverses the sort order.
    #[must_use]
    pub fn rev(mut self) -> Self {
        self.rev = true;
        self
    }

    /// Limits the results to `count` elements starting at `offset`.
    #[must_use]
    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.limit = Some((offset, count));
        self
    }
}

impl Command for ZRangeStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("ZRANGESTORE"),
            bulk(self.dst.as_str()),
            bulk(self.src.as_str()),
            bulk(self.min.as_str()),
            bulk(self.max.as_str()),
        ];
        if self.by_score {
            args.push(bulk("BYSCORE"));
        } else if self.by_lex {
            args.push(bulk("BYLEX"));
        }
        if self.rev {
            args.push(bulk("REV"));
        }
        if let Some((offset, count)) = self.limit {
            args.push(bulk("LIMIT"));
            args.push(bulk(offset.to_string()));
            args.push(bulk(count.to_string()));
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
        "ZRANGESTORE"
    }
}

/// The direction argument for ZMPOP.
#[derive(Debug, Clone, Copy)]
pub enum ZMPopDirection {
    Min,
    Max,
}

impl ZMPopDirection {
    fn as_str(&self) -> &str {
        match self {
            ZMPopDirection::Min => "MIN",
            ZMPopDirection::Max => "MAX",
        }
    }
}

/// ZMPOP numkeys key \[key ...\] MIN|MAX \[COUNT count\]
///
/// Pops one or more members with the lowest or highest scores from the first
/// non-empty sorted set among the specified keys. Returns `None` when no
/// elements could be popped from any of the sorted sets, or
/// `Some((key, members))` where `key` is the name of the sorted set and
/// `members` is a list of `(member, score)` pairs.
///
/// See: <https://redis.io/commands/zmpop>
pub struct ZMPop {
    keys: Vec<String>,
    direction: ZMPopDirection,
    count: Option<i64>,
}

impl ZMPop {
    /// Creates a new [`ZMPop`] command.
    pub fn new(
        keys: impl IntoIterator<Item = impl Into<String>>,
        direction: ZMPopDirection,
    ) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            direction,
            count: None,
        }
    }

    /// Sets the number of members to pop.
    #[must_use]
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for ZMPop {
    type Response = Option<(Bytes, Vec<(Bytes, f64)>)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZMPOP"), bulk(self.keys.len().to_string())];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        args.push(bulk(self.direction.as_str()));
        if let Some(count) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) if frames.len() == 2 => {
                let key = match &frames[0] {
                    Frame::BulkString(Some(data)) => data.clone(),
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "bulk string (key name)",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                let members = match &frames[1] {
                    Frame::Array(Some(pairs)) => pairs
                        .iter()
                        .map(|pair| match pair {
                            Frame::Array(Some(inner)) if inner.len() == 2 => {
                                let member = match &inner[0] {
                                    Frame::BulkString(Some(data)) => data.clone(),
                                    other => {
                                        return Err(RedisError::UnexpectedResponse {
                                            expected: "bulk string (member)",
                                            actual: format!("{other:?}"),
                                        });
                                    }
                                };
                                let score = match &inner[1] {
                                    Frame::BulkString(Some(data)) => {
                                        let s = String::from_utf8_lossy(data);
                                        s.parse::<f64>().map_err(|_| {
                                            RedisError::UnexpectedResponse {
                                                expected: "float string",
                                                actual: format!("{s}"),
                                            }
                                        })?
                                    }
                                    Frame::Double(d) => *d,
                                    other => {
                                        return Err(RedisError::UnexpectedResponse {
                                            expected: "bulk string or double (score)",
                                            actual: format!("{other:?}"),
                                        });
                                    }
                                };
                                Ok((member, score))
                            }
                            other => Err(RedisError::UnexpectedResponse {
                                expected: "array of [member, score]",
                                actual: format!("{other:?}"),
                            }),
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "array of member/score pairs",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                Ok(Some((key, members)))
            }
            Frame::Array(None) | Frame::Null | Frame::BulkString(None) => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "two-element array or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "ZMPOP"
    }
}

/// ZREMRANGEBYRANK key start stop
///
/// Removes all members in the sorted set stored at `key` with rank between
/// `start` and `stop` (inclusive, zero-based). Returns the number of members
/// removed.
///
/// See: <https://redis.io/commands/zremrangebyrank>
pub struct ZRemRangeByRank {
    key: String,
    start: i64,
    stop: i64,
}

impl ZRemRangeByRank {
    /// Creates a new [`ZRemRangeByRank`] command.
    pub fn new(key: impl Into<String>, start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            start,
            stop,
        }
    }
}

impl Command for ZRemRangeByRank {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZREMRANGEBYRANK"),
            bulk(self.key.as_str()),
            bulk(self.start.to_string()),
            bulk(self.stop.to_string()),
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
        "ZREMRANGEBYRANK"
    }
}

/// ZREMRANGEBYSCORE key min max
///
/// Removes all members in the sorted set stored at `key` with a score between
/// `min` and `max` (inclusive). The `min` and `max` arguments can be `"-inf"`,
/// `"+inf"`, or numeric strings (prefix with `"("` for exclusive bounds).
/// Returns the number of members removed.
///
/// See: <https://redis.io/commands/zremrangebyscore>
pub struct ZRemRangeByScore {
    key: String,
    min: String,
    max: String,
}

impl ZRemRangeByScore {
    /// Creates a new [`ZRemRangeByScore`] command.
    pub fn new(key: impl Into<String>, min: impl Into<String>, max: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            min: min.into(),
            max: max.into(),
        }
    }
}

impl Command for ZRemRangeByScore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZREMRANGEBYSCORE"),
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
        "ZREMRANGEBYSCORE"
    }
}

/// ZREMRANGEBYLEX key min max
///
/// Removes all members in the sorted set stored at `key` between the
/// lexicographical range specified by `min` and `max`. Valid values for
/// `min` and `max` are `"-"`, `"+"`, `"[value"` (inclusive), or `"(value"`
/// (exclusive). Returns the number of members removed.
///
/// See: <https://redis.io/commands/zremrangebylex>
pub struct ZRemRangeByLex {
    key: String,
    min: String,
    max: String,
}

impl ZRemRangeByLex {
    /// Creates a new [`ZRemRangeByLex`] command.
    pub fn new(key: impl Into<String>, min: impl Into<String>, max: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            min: min.into(),
            max: max.into(),
        }
    }
}

impl Command for ZRemRangeByLex {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZREMRANGEBYLEX"),
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
        "ZREMRANGEBYLEX"
    }
}

/// ZREVRANK key member
///
/// Returns the rank of `member` in the sorted set at `key` with scores
/// ordered from high to low (zero-based, highest score = rank 0), or `None`
/// if the member or key does not exist.
///
/// See: <https://redis.io/commands/zrevrank>
pub struct ZRevRank {
    key: String,
    member: String,
}

impl ZRevRank {
    /// Creates a new [`ZRevRank`] command.
    pub fn new(key: impl Into<String>, member: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            member: member.into(),
        }
    }
}

impl Command for ZRevRank {
    type Response = Option<i64>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZREVRANK"),
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
        "ZREVRANK"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    // -- ZAdd --

    #[test]
    fn zadd_to_frame() {
        let cmd = ZAdd::new("myzset").member(1.0, "a").member(2.0, "b");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZADD"),
                bulk("myzset"),
                bulk("1"),
                bulk("a"),
                bulk("2"),
                bulk("b"),
            ])
        );
    }

    #[test]
    fn zadd_parse_integer() {
        let cmd = ZAdd::new("myzset").member(1.0, "a");
        assert_eq!(cmd.parse_response(Frame::Integer(1)).unwrap(), 1);
    }

    #[test]
    fn zadd_parse_error_on_string() {
        let cmd = ZAdd::new("myzset").member(1.0, "a");
        assert!(
            cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
                .is_err()
        );
    }

    // -- ZRem --

    #[test]
    fn zrem_to_frame() {
        let cmd = ZRem::new("myzset", "a");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("ZREM"), bulk("myzset"), bulk("a")])
        );
    }

    // -- ZRange --

    #[test]
    fn zrange_to_frame() {
        let cmd = ZRange::new("myzset", 0, -1);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("ZRANGE"), bulk("myzset"), bulk("0"), bulk("-1"),])
        );
    }

    #[test]
    fn zrange_parse_array() {
        let cmd = ZRange::new("myzset", 0, -1);
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::BulkString(Some(Bytes::from("b"))),
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    #[test]
    fn zrange_parse_error_on_integer() {
        let cmd = ZRange::new("myzset", 0, -1);
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    // -- ZScore --

    #[test]
    fn zscore_to_frame() {
        let cmd = ZScore::new("myzset", "a");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("ZSCORE"), bulk("myzset"), bulk("a")])
        );
    }

    #[test]
    fn zscore_parse_bulk_string() {
        let cmd = ZScore::new("myzset", "a");
        let frame = Frame::BulkString(Some(Bytes::from("1.5")));
        let result = cmd.parse_response(frame).unwrap().unwrap();
        assert!((result - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn zscore_parse_double() {
        let cmd = ZScore::new("myzset", "a");
        let result = cmd.parse_response(Frame::Double(2.5)).unwrap().unwrap();
        assert!((result - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn zscore_parse_null() {
        let cmd = ZScore::new("myzset", "missing");
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    #[test]
    fn zscore_parse_error_on_integer() {
        let cmd = ZScore::new("myzset", "a");
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    // -- ZCard --

    #[test]
    fn zcard_to_frame() {
        let cmd = ZCard::new("myzset");
        assert_eq!(cmd.to_frame(), array(vec![bulk("ZCARD"), bulk("myzset")]));
    }

    // -- ZIncrBy --

    #[test]
    fn zincrby_to_frame() {
        let cmd = ZIncrBy::new("myzset", 2.0, "member");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZINCRBY"),
                bulk("myzset"),
                bulk("2"),
                bulk("member"),
            ])
        );
    }

    #[test]
    fn zincrby_parse_bulk_string() {
        let cmd = ZIncrBy::new("myzset", 2.0, "m");
        let frame = Frame::BulkString(Some(Bytes::from("5.0")));
        let result = cmd.parse_response(frame).unwrap();
        assert!((result - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn zincrby_parse_double() {
        let cmd = ZIncrBy::new("myzset", 2.0, "m");
        assert!((cmd.parse_response(Frame::Double(5.0)).unwrap() - 5.0).abs() < f64::EPSILON);
    }

    // -- ZRank --

    #[test]
    fn zrank_to_frame() {
        let cmd = ZRank::new("myzset", "member");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("ZRANK"), bulk("myzset"), bulk("member")])
        );
    }

    #[test]
    fn zrank_parse_integer() {
        let cmd = ZRank::new("myzset", "m");
        assert_eq!(cmd.parse_response(Frame::Integer(0)).unwrap(), Some(0));
    }

    #[test]
    fn zrank_parse_null() {
        let cmd = ZRank::new("myzset", "m");
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    // -- ZPopMin --

    #[test]
    fn zpopmin_to_frame() {
        let cmd = ZPopMin::new("myzset").count(2);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("ZPOPMIN"), bulk("myzset"), bulk("2")])
        );
    }

    #[test]
    fn zpopmin_parse_pairs() {
        let cmd = ZPopMin::new("myzset");
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::BulkString(Some(Bytes::from("1.0"))),
            Frame::BulkString(Some(Bytes::from("b"))),
            Frame::BulkString(Some(Bytes::from("2.0"))),
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, Bytes::from("a"));
        assert!((result[0].1 - 1.0).abs() < f64::EPSILON);
        assert_eq!(result[1].0, Bytes::from("b"));
    }

    #[test]
    fn zpopmin_parse_empty() {
        let cmd = ZPopMin::new("myzset");
        let result = cmd.parse_response(Frame::Array(None)).unwrap();
        assert!(result.is_empty());
    }

    // -- ZInterStore --

    #[test]
    fn zinterstore_to_frame() {
        let cmd = ZInterStore::new("dest", vec!["s1", "s2"])
            .weights(vec![1.0, 2.0])
            .aggregate(Aggregate::Max);
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(Some(args)) => {
                assert_eq!(args[0], bulk("ZINTERSTORE"));
                assert_eq!(args[1], bulk("dest"));
                assert_eq!(args[2], bulk("2"));
                assert_eq!(args[3], bulk("s1"));
                assert_eq!(args[4], bulk("s2"));
                assert!(args.contains(&bulk("WEIGHTS")));
                assert!(args.contains(&bulk("AGGREGATE")));
                assert!(args.contains(&bulk("MAX")));
            }
            _ => panic!("expected array"),
        }
    }

    // -- ZMScore --

    #[test]
    fn zmscore_to_frame() {
        let cmd = ZMScore::members("myzset", vec!["a", "b"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("ZMSCORE"), bulk("myzset"), bulk("a"), bulk("b"),])
        );
    }

    #[test]
    fn zmscore_parse_mixed() {
        let cmd = ZMScore::members("myzset", vec!["a", "b"]);
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("1.5"))),
            Frame::Null,
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert!((result[0].unwrap() - 1.5).abs() < f64::EPSILON);
        assert_eq!(result[1], None);
    }

    // -- ZRangeByScore --

    #[test]
    fn zrangebyscore_to_frame() {
        let cmd = ZRangeByScore::new("myzset", "-inf", "+inf");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZRANGEBYSCORE"),
                bulk("myzset"),
                bulk("-inf"),
                bulk("+inf"),
            ])
        );
    }
}
