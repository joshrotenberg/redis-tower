use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// ZADD key score member \[score member ...\]
///
/// Adds the specified members with scores to the sorted set stored at `key`.
/// Returns the number of members added (excluding members already present
/// whose score was updated).
#[derive(Clone)]
pub struct ZAdd {
    key: String,
    members: Vec<(f64, String)>,
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
    ch: bool,
}

impl ZAdd {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            members: Vec::new(),
            nx: false,
            xx: false,
            gt: false,
            lt: false,
            ch: false,
        }
    }

    /// Constructs a [`ZAdd`] pre-populated from an iterator of `(score, member)` pairs.
    ///
    /// This is the bulk-insert constructor: equivalent to calling `.member()` for every
    /// pair in the iterator. Accepts any `IntoIterator<Item = (f64, impl Into<String>)>`,
    /// including `Vec<(f64, String)>` and similar collections.
    ///
    /// Option flags (`nx`, `xx`, `gt`, `lt`, `ch`) can be chained as usual:
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use redis_tower_commands::ZAdd;
    /// use redis_tower_core::RedisConnection;
    ///
    /// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
    ///
    /// let scores = vec![(100.0, "alice"), (200.0, "bob")];
    /// let added = conn
    ///     .execute(ZAdd::from_members("leaderboard", scores).ch())
    ///     .await?;
    /// # let _ = added;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Produces the same wire frame as the incremental builder for the same members:
    ///
    /// ```no_run
    /// use redis_tower_commands::ZAdd;
    /// use redis_tower_core::Command;
    ///
    /// // These two are equivalent:
    /// let a = ZAdd::new("z").member(1.0, "a").member(2.0, "b");
    /// let b = ZAdd::from_members("z", [(1.0, "a"), (2.0, "b")]);
    /// assert_eq!(a.to_frame(), b.to_frame());
    /// ```
    pub fn from_members(
        key: impl Into<String>,
        members: impl IntoIterator<Item = (f64, impl Into<String>)>,
    ) -> Self {
        Self {
            key: key.into(),
            members: members
                .into_iter()
                .map(|(score, member)| (score, member.into()))
                .collect(),
            nx: false,
            xx: false,
            gt: false,
            lt: false,
            ch: false,
        }
    }

    /// Adds a member with the given score.
    pub fn member(mut self, score: f64, member: impl Into<String>) -> Self {
        self.members.push((score, member.into()));
        self
    }

    /// Only add new members; do not update existing members (NX).
    pub fn nx(mut self) -> Self {
        self.nx = true;
        self
    }

    /// Only update existing members; do not add new members (XX).
    pub fn xx(mut self) -> Self {
        self.xx = true;
        self
    }

    /// Only update existing members if the new score is greater (GT).
    pub fn gt(mut self) -> Self {
        self.gt = true;
        self
    }

    /// Only update existing members if the new score is less (LT).
    pub fn lt(mut self) -> Self {
        self.lt = true;
        self
    }

    /// Return the number of changed members rather than only added ones (CH).
    pub fn ch(mut self) -> Self {
        self.ch = true;
        self
    }
}

impl Command for ZAdd {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZADD"), bulk(self.key.as_str())];
        if self.nx {
            args.push(bulk("NX"));
        }
        if self.xx {
            args.push(bulk("XX"));
        }
        if self.gt {
            args.push(bulk("GT"));
        }
        if self.lt {
            args.push(bulk("LT"));
        }
        if self.ch {
            args.push(bulk("CH"));
        }
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
#[derive(Clone)]
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
#[derive(Clone)]
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZSCORE key member
///
/// Returns the score of `member` in the sorted set at `key`, or `None` if
/// the \[member\] or key does not exist.
#[derive(Clone)]
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZCARD key
///
/// Returns the number of members in the sorted set stored at `key`.
#[derive(Clone)]
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZINCRBY key increment member
///
/// Increments the score of `member` in the sorted set at `key` by
/// `increment`. Returns the new score of the \[member\].
#[derive(Clone)]
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
#[derive(Clone)]
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZRANGEBYSCORE key min max
///
/// Returns all members in the sorted set at `key` with a score between
/// `min` and `max` (inclusive). The `min` and `max` arguments can be
/// `"-inf"`, `"+inf"`, or numeric strings.
#[derive(Clone)]
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZPOPMIN key \[count\]
///
/// Removes and returns the members with the lowest scores in the sorted set
/// stored at `key`. Returns a list of `(member, score)` pairs.
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZLEXCOUNT key min max
///
/// Returns the number of members in the sorted set at `key` between the
/// lexicographical range specified by `min` and `max`. Valid values for
/// `min` and `max` are `"-"`, `"+"`, `"[value"` (inclusive), or `"(value"`
/// (exclusive).
#[derive(Clone)]
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZRANDMEMBER key \[count\]
///
/// Returns one or more random members from the sorted set at `key`.
/// When called without `count`, returns a single random member.
/// When called with `count`, returns up to that many distinct members.
#[derive(Clone)]
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZMSCORE key member \[member ...\]
///
/// Returns the scores associated with the specified members in the sorted
/// set at `key`. For each member, returns `Some(score)` if the member
/// exists, or `None` if it does not.
#[derive(Clone)]
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

    fn idempotent(&self) -> bool {
        true
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
#[derive(Clone)]
pub struct ZInterStore {
    destination: String,
    keys: Vec<String>,
    weights: Option<Vec<f64>>,
    aggregate: Option<Aggregate>,
}

impl ZInterStore {
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
    pub fn weights(mut self, weights: impl IntoIterator<Item = f64>) -> Self {
        self.weights = Some(weights.into_iter().collect());
        self
    }

    /// Sets the aggregation function for combining scores.
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
#[derive(Clone)]
pub struct ZUnionStore {
    destination: String,
    keys: Vec<String>,
    weights: Option<Vec<f64>>,
    aggregate: Option<Aggregate>,
}

impl ZUnionStore {
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
    pub fn weights(mut self, weights: impl IntoIterator<Item = f64>) -> Self {
        self.weights = Some(weights.into_iter().collect());
        self
    }

    /// Sets the aggregation function for combining scores.
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
#[derive(Clone)]
pub struct ZDiffStore {
    destination: String,
    keys: Vec<String>,
}

impl ZDiffStore {
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
#[derive(Clone)]
pub struct ZInterCard {
    keys: Vec<String>,
    limit: Option<i64>,
}

impl ZInterCard {
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            limit: None,
        }
    }

    /// Sets the upper bound for the returned cardinality.
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZRANGESTORE dst src min max \[BYSCORE|BYLEX\] \[REV\] \[LIMIT offset count\]
///
/// Stores the specified range of members from the sorted set at `src` into
/// `dst`. Returns the number of elements in the resulting sorted set.
#[derive(Clone)]
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
    pub fn by_score(mut self) -> Self {
        self.by_score = true;
        self.by_lex = false;
        self
    }

    /// Uses lexicographic range interpretation.
    pub fn by_lex(mut self) -> Self {
        self.by_lex = true;
        self.by_score = false;
        self
    }

    /// Reverses the sort order.
    pub fn rev(mut self) -> Self {
        self.rev = true;
        self
    }

    /// Limits the results to `count` elements starting at `offset`.
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
#[derive(Clone)]
pub struct ZMPop {
    keys: Vec<String>,
    direction: ZMPopDirection,
    count: Option<i64>,
}

impl ZMPop {
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
#[derive(Clone)]
pub struct ZRemRangeByRank {
    key: String,
    start: i64,
    stop: i64,
}

impl ZRemRangeByRank {
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
#[derive(Clone)]
pub struct ZRemRangeByScore {
    key: String,
    min: String,
    max: String,
}

impl ZRemRangeByScore {
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
#[derive(Clone)]
pub struct ZRemRangeByLex {
    key: String,
    min: String,
    max: String,
}

impl ZRemRangeByLex {
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
#[derive(Clone)]
pub struct ZRevRank {
    key: String,
    member: String,
}

impl ZRevRank {
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// Parse a flat array of bulk-string members into a `Vec<Bytes>`.
fn parse_member_array(frame: Frame) -> Result<Vec<Bytes>, RedisError> {
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

/// Parse a WITHSCORES response (flat `[member, score, ...]` array in RESP2 or
/// an array of `[member, score]` pairs in RESP3) into `Vec<(Bytes, f64)>`.
fn parse_member_score_pairs(frame: Frame) -> Result<Vec<(Bytes, f64)>, RedisError> {
    fn parse_score(frame: &Frame) -> Result<f64, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                s.parse::<f64>()
                    .map_err(|_| RedisError::UnexpectedResponse {
                        expected: "float string",
                        actual: format!("{s}"),
                    })
            }
            Frame::Double(d) => Ok(*d),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or double",
                actual: format!("{other:?}"),
            }),
        }
    }
    fn parse_member(frame: &Frame) -> Result<Bytes, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(data.clone()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }
    match frame {
        Frame::Array(None) => Ok(Vec::new()),
        Frame::Array(Some(frames)) => {
            // RESP3: array of [member, score] pairs.
            if frames
                .iter()
                .all(|f| matches!(f, Frame::Array(Some(inner)) if inner.len() == 2))
            {
                return frames
                    .iter()
                    .map(|pair| match pair {
                        Frame::Array(Some(inner)) => {
                            Ok((parse_member(&inner[0])?, parse_score(&inner[1])?))
                        }
                        other => Err(RedisError::UnexpectedResponse {
                            expected: "array of [member, score]",
                            actual: format!("{other:?}"),
                        }),
                    })
                    .collect();
            }
            // RESP2: flat [member, score, member, score, ...] array.
            if frames.len() % 2 != 0 {
                return Err(RedisError::UnexpectedResponse {
                    expected: "even number of elements (member/score pairs)",
                    actual: format!("array of length {}", frames.len()),
                });
            }
            frames
                .chunks(2)
                .map(|pair| Ok((parse_member(&pair[0])?, parse_score(&pair[1])?)))
                .collect()
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "array",
            actual: format!("{other:?}"),
        }),
    }
}

/// ZADD key INCR score member
///
/// Increments the score of `member` by `score` (ZADD with the INCR flag). When
/// used together with NX/XX the operation may be skipped, in which case `None`
/// is returned; otherwise returns the new score of the member.
#[derive(Clone)]
pub struct ZAddIncr {
    key: String,
    score: f64,
    member: String,
}

impl ZAddIncr {
    pub fn new(key: impl Into<String>, score: f64, member: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            score,
            member: member.into(),
        }
    }
}

impl Command for ZAddIncr {
    type Response = Option<f64>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZADD"),
            bulk(self.key.as_str()),
            bulk("INCR"),
            bulk(self.score.to_string()),
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
        "ZADD"
    }
}

/// ZDIFF numkeys key \[key ...\]
///
/// Returns the difference between the first sorted set and all successive
/// sorted sets. Returns only the members (without scores).
#[derive(Clone)]
pub struct ZDiff {
    keys: Vec<String>,
}

impl ZDiff {
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for ZDiff {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZDIFF"), bulk(self.keys.len().to_string())];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_member_array(frame)
    }

    fn name(&self) -> &str {
        "ZDIFF"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZDIFF numkeys key \[key ...\] WITHSCORES
///
/// Returns the difference between the first sorted set and all successive
/// sorted sets, including each member's score.
#[derive(Clone)]
pub struct ZDiffWithScores {
    keys: Vec<String>,
}

impl ZDiffWithScores {
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for ZDiffWithScores {
    type Response = Vec<(Bytes, f64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZDIFF"), bulk(self.keys.len().to_string())];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        args.push(bulk("WITHSCORES"));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_member_score_pairs(frame)
    }

    fn name(&self) -> &str {
        "ZDIFF"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZUNION numkeys key \[key ...\] \[WEIGHTS weight ...\] \[AGGREGATE SUM|MIN|MAX\]
///
/// Returns the union of the sorted sets given by the specified keys. Returns
/// only the members (without scores).
#[derive(Clone)]
pub struct ZUnion {
    keys: Vec<String>,
    weights: Option<Vec<f64>>,
    aggregate: Option<Aggregate>,
}

impl ZUnion {
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            weights: None,
            aggregate: None,
        }
    }

    /// Sets the weight multipliers for each input sorted set.
    pub fn weights(mut self, weights: impl IntoIterator<Item = f64>) -> Self {
        self.weights = Some(weights.into_iter().collect());
        self
    }

    /// Sets the aggregation function for combining scores.
    pub fn aggregate(mut self, aggregate: Aggregate) -> Self {
        self.aggregate = Some(aggregate);
        self
    }
}

impl Command for ZUnion {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZUNION"), bulk(self.keys.len().to_string())];
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
        parse_member_array(frame)
    }

    fn name(&self) -> &str {
        "ZUNION"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZUNION numkeys key \[key ...\] \[WEIGHTS weight ...\] \[AGGREGATE SUM|MIN|MAX\] WITHSCORES
///
/// Returns the union of the sorted sets given by the specified keys, including
/// each member's score.
#[derive(Clone)]
pub struct ZUnionWithScores {
    keys: Vec<String>,
    weights: Option<Vec<f64>>,
    aggregate: Option<Aggregate>,
}

impl ZUnionWithScores {
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            weights: None,
            aggregate: None,
        }
    }

    /// Sets the weight multipliers for each input sorted set.
    pub fn weights(mut self, weights: impl IntoIterator<Item = f64>) -> Self {
        self.weights = Some(weights.into_iter().collect());
        self
    }

    /// Sets the aggregation function for combining scores.
    pub fn aggregate(mut self, aggregate: Aggregate) -> Self {
        self.aggregate = Some(aggregate);
        self
    }
}

impl Command for ZUnionWithScores {
    type Response = Vec<(Bytes, f64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZUNION"), bulk(self.keys.len().to_string())];
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
        args.push(bulk("WITHSCORES"));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_member_score_pairs(frame)
    }

    fn name(&self) -> &str {
        "ZUNION"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZINTER numkeys key \[key ...\] \[WEIGHTS weight ...\] \[AGGREGATE SUM|MIN|MAX\]
///
/// Returns the intersection of the sorted sets given by the specified keys.
/// Returns only the members (without scores).
#[derive(Clone)]
pub struct ZInter {
    keys: Vec<String>,
    weights: Option<Vec<f64>>,
    aggregate: Option<Aggregate>,
}

impl ZInter {
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            weights: None,
            aggregate: None,
        }
    }

    /// Sets the weight multipliers for each input sorted set.
    pub fn weights(mut self, weights: impl IntoIterator<Item = f64>) -> Self {
        self.weights = Some(weights.into_iter().collect());
        self
    }

    /// Sets the aggregation function for combining scores.
    pub fn aggregate(mut self, aggregate: Aggregate) -> Self {
        self.aggregate = Some(aggregate);
        self
    }
}

impl Command for ZInter {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZINTER"), bulk(self.keys.len().to_string())];
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
        parse_member_array(frame)
    }

    fn name(&self) -> &str {
        "ZINTER"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZINTER numkeys key \[key ...\] \[WEIGHTS weight ...\] \[AGGREGATE SUM|MIN|MAX\] WITHSCORES
///
/// Returns the intersection of the sorted sets given by the specified keys,
/// including each member's score.
#[derive(Clone)]
pub struct ZInterWithScores {
    keys: Vec<String>,
    weights: Option<Vec<f64>>,
    aggregate: Option<Aggregate>,
}

impl ZInterWithScores {
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            weights: None,
            aggregate: None,
        }
    }

    /// Sets the weight multipliers for each input sorted set.
    pub fn weights(mut self, weights: impl IntoIterator<Item = f64>) -> Self {
        self.weights = Some(weights.into_iter().collect());
        self
    }

    /// Sets the aggregation function for combining scores.
    pub fn aggregate(mut self, aggregate: Aggregate) -> Self {
        self.aggregate = Some(aggregate);
        self
    }
}

impl Command for ZInterWithScores {
    type Response = Vec<(Bytes, f64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ZINTER"), bulk(self.keys.len().to_string())];
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
        args.push(bulk("WITHSCORES"));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_member_score_pairs(frame)
    }

    fn name(&self) -> &str {
        "ZINTER"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// BZMPOP timeout numkeys key \[key ...\] MIN|MAX \[COUNT count\]
///
/// Blocking variant of ZMPOP. Pops one or more members with the lowest or
/// highest scores from the first non-empty sorted set among the specified
/// keys, blocking up to `timeout` seconds (0 to block indefinitely). Returns
/// `None` on timeout, or `Some((key, members))` where `members` is a list of
/// `(member, score)` pairs.
#[derive(Clone)]
pub struct BZMPop {
    timeout: f64,
    keys: Vec<String>,
    direction: ZMPopDirection,
    count: Option<u64>,
}

impl BZMPop {
    pub fn new(
        timeout: f64,
        keys: impl IntoIterator<Item = impl Into<String>>,
        direction: ZMPopDirection,
    ) -> Self {
        Self {
            timeout,
            keys: keys.into_iter().map(Into::into).collect(),
            direction,
            count: None,
        }
    }

    /// Sets the number of members to pop.
    pub fn count(mut self, count: u64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for BZMPop {
    type Response = Option<(Bytes, Vec<(Bytes, f64)>)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("BZMPOP"),
            bulk(self.timeout.to_string()),
            bulk(self.keys.len().to_string()),
        ];
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
        "BZMPOP"
    }
}

/// ZREVRANGE key start stop
///
/// Returns the specified range of members in the sorted set stored at `key`,
/// ordered from highest to lowest score. `start` and `stop` are zero-based
/// indices, where -1 is the last element.
///
/// Deprecated since Redis 6.2 in favor of `ZRANGE ... REV`, but still widely
/// used. Use [`ZRevRangeWithScores`] to also return each member's score.
#[derive(Clone)]
pub struct ZRevRange {
    key: String,
    start: i64,
    stop: i64,
}

impl ZRevRange {
    pub fn new(key: impl Into<String>, start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            start,
            stop,
        }
    }
}

impl Command for ZRevRange {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZREVRANGE"),
            bulk(self.key.as_str()),
            bulk(self.start.to_string()),
            bulk(self.stop.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_member_array(frame)
    }

    fn name(&self) -> &str {
        "ZREVRANGE"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZREVRANGE key start stop WITHSCORES
///
/// Returns the specified range of members in the sorted set stored at `key`,
/// ordered from highest to lowest score, including each member's score.
///
/// Deprecated since Redis 6.2 in favor of `ZRANGE ... REV WITHSCORES`.
#[derive(Clone)]
pub struct ZRevRangeWithScores {
    key: String,
    start: i64,
    stop: i64,
}

impl ZRevRangeWithScores {
    pub fn new(key: impl Into<String>, start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            start,
            stop,
        }
    }
}

impl Command for ZRevRangeWithScores {
    type Response = Vec<(Bytes, f64)>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ZREVRANGE"),
            bulk(self.key.as_str()),
            bulk(self.start.to_string()),
            bulk(self.stop.to_string()),
            bulk("WITHSCORES"),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_member_score_pairs(frame)
    }

    fn name(&self) -> &str {
        "ZREVRANGE"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZRANGEBYLEX key min max \[LIMIT offset count\]
///
/// Returns all members in the sorted set at `key` between the lexicographical
/// range specified by `min` and `max`. Valid values for `min` and `max` are
/// `"-"`, `"+"`, `"[value"` (inclusive), or `"(value"` (exclusive). Assumes
/// all members share the same score.
///
/// Deprecated since Redis 6.2 in favor of `ZRANGE ... BYLEX`.
#[derive(Clone)]
pub struct ZRangeByLex {
    key: String,
    min: String,
    max: String,
    limit: Option<(i64, i64)>,
}

impl ZRangeByLex {
    pub fn new(key: impl Into<String>, min: impl Into<String>, max: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            min: min.into(),
            max: max.into(),
            limit: None,
        }
    }

    /// Limits the results to `count` elements starting at `offset`.
    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.limit = Some((offset, count));
        self
    }
}

impl Command for ZRangeByLex {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("ZRANGEBYLEX"),
            bulk(self.key.as_str()),
            bulk(self.min.as_str()),
            bulk(self.max.as_str()),
        ];
        if let Some((offset, count)) = self.limit {
            args.push(bulk("LIMIT"));
            args.push(bulk(offset.to_string()));
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_member_array(frame)
    }

    fn name(&self) -> &str {
        "ZRANGEBYLEX"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZREVRANGEBYLEX key max min \[LIMIT offset count\]
///
/// Returns all members in the sorted set at `key` between the lexicographical
/// range specified by `max` and `min`, ordered from higher to lower. Note the
/// reversed argument order: `max` comes before `min`. Valid values for `min`
/// and `max` are `"-"`, `"+"`, `"[value"` (inclusive), or `"(value"`
/// (exclusive). Assumes all members share the same score.
///
/// Deprecated since Redis 6.2 in favor of `ZRANGE ... BYLEX REV`.
#[derive(Clone)]
pub struct ZRevRangeByLex {
    key: String,
    max: String,
    min: String,
    limit: Option<(i64, i64)>,
}

impl ZRevRangeByLex {
    pub fn new(key: impl Into<String>, max: impl Into<String>, min: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            max: max.into(),
            min: min.into(),
            limit: None,
        }
    }

    /// Limits the results to `count` elements starting at `offset`.
    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.limit = Some((offset, count));
        self
    }
}

impl Command for ZRevRangeByLex {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("ZREVRANGEBYLEX"),
            bulk(self.key.as_str()),
            bulk(self.max.as_str()),
            bulk(self.min.as_str()),
        ];
        if let Some((offset, count)) = self.limit {
            args.push(bulk("LIMIT"));
            args.push(bulk(offset.to_string()));
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_member_array(frame)
    }

    fn name(&self) -> &str {
        "ZREVRANGEBYLEX"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZREVRANGEBYSCORE key max min \[LIMIT offset count\]
///
/// Returns all members in the sorted set at `key` with a score between `min`
/// and `max`, ordered from highest to lowest score. Note the reversed argument
/// order: `max` comes before `min`. The `min` and `max` arguments can be
/// `"-inf"`, `"+inf"`, or numeric strings (prefix with `"("` for exclusive
/// bounds). Use [`ZRevRangeByScoreWithScores`] to also return each member's
/// score.
///
/// Deprecated since Redis 6.2 in favor of `ZRANGE ... BYSCORE REV`.
#[derive(Clone)]
pub struct ZRevRangeByScore {
    key: String,
    max: String,
    min: String,
    limit: Option<(i64, i64)>,
}

impl ZRevRangeByScore {
    pub fn new(key: impl Into<String>, max: impl Into<String>, min: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            max: max.into(),
            min: min.into(),
            limit: None,
        }
    }

    /// Limits the results to `count` elements starting at `offset`.
    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.limit = Some((offset, count));
        self
    }
}

impl Command for ZRevRangeByScore {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("ZREVRANGEBYSCORE"),
            bulk(self.key.as_str()),
            bulk(self.max.as_str()),
            bulk(self.min.as_str()),
        ];
        if let Some((offset, count)) = self.limit {
            args.push(bulk("LIMIT"));
            args.push(bulk(offset.to_string()));
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_member_array(frame)
    }

    fn name(&self) -> &str {
        "ZREVRANGEBYSCORE"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ZREVRANGEBYSCORE key max min WITHSCORES \[LIMIT offset count\]
///
/// Returns all members in the sorted set at `key` with a score between `min`
/// and `max`, ordered from highest to lowest score, including each member's
/// score. Note the reversed argument order: `max` comes before `min`.
///
/// Deprecated since Redis 6.2 in favor of `ZRANGE ... BYSCORE REV WITHSCORES`.
#[derive(Clone)]
pub struct ZRevRangeByScoreWithScores {
    key: String,
    max: String,
    min: String,
    limit: Option<(i64, i64)>,
}

impl ZRevRangeByScoreWithScores {
    pub fn new(key: impl Into<String>, max: impl Into<String>, min: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            max: max.into(),
            min: min.into(),
            limit: None,
        }
    }

    /// Limits the results to `count` elements starting at `offset`.
    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.limit = Some((offset, count));
        self
    }
}

impl Command for ZRevRangeByScoreWithScores {
    type Response = Vec<(Bytes, f64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("ZREVRANGEBYSCORE"),
            bulk(self.key.as_str()),
            bulk(self.max.as_str()),
            bulk(self.min.as_str()),
            bulk("WITHSCORES"),
        ];
        if let Some((offset, count)) = self.limit {
            args.push(bulk("LIMIT"));
            args.push(bulk(offset.to_string()));
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_member_score_pairs(frame)
    }

    fn name(&self) -> &str {
        "ZREVRANGEBYSCORE"
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

    #[test]
    fn zadd_from_members_to_frame() {
        let cmd = ZAdd::from_members("myzset", [(1.0_f64, "a"), (2.0_f64, "b")]);
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
    fn zadd_from_members_matches_incremental() {
        let incremental = ZAdd::new("z").member(1.0, "a").member(2.0, "b");
        let bulk = ZAdd::from_members("z", [(1.0_f64, "a"), (2.0_f64, "b")]);
        assert_eq!(incremental.to_frame(), bulk.to_frame());
    }

    #[test]
    fn zadd_from_members_vec() {
        let scores: Vec<(f64, &str)> = vec![(100.0, "alice"), (200.0, "bob"), (150.0, "carol")];
        let cmd = ZAdd::from_members("leaderboard", scores);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZADD"),
                bulk("leaderboard"),
                bulk("100"),
                bulk("alice"),
                bulk("200"),
                bulk("bob"),
                bulk("150"),
                bulk("carol"),
            ])
        );
    }

    #[test]
    fn zadd_from_members_with_options() {
        // Verify that option flags can be chained after from_members
        let cmd = ZAdd::from_members("z", [(1.0_f64, "a")]).ch();
        match cmd.to_frame() {
            Frame::Array(Some(args)) => {
                assert_eq!(args[0], bulk("ZADD"));
                assert_eq!(args[1], bulk("z"));
                assert!(args.contains(&bulk("CH")));
            }
            _ => panic!("expected array"),
        }
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

    // -- ZAdd options --

    #[test]
    fn zadd_with_options_to_frame() {
        let cmd = ZAdd::new("myzset").nx().ch().member(1.0, "a");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZADD"),
                bulk("myzset"),
                bulk("NX"),
                bulk("CH"),
                bulk("1"),
                bulk("a"),
            ])
        );
    }

    #[test]
    fn zadd_gt_lt_to_frame() {
        let cmd = ZAdd::new("myzset").gt().lt().member(2.0, "b");
        match cmd.to_frame() {
            Frame::Array(Some(args)) => {
                assert!(args.contains(&bulk("GT")));
                assert!(args.contains(&bulk("LT")));
                assert!(!args.contains(&bulk("XX")));
            }
            _ => panic!("expected array"),
        }
    }

    // -- ZAddIncr --

    #[test]
    fn zadd_incr_to_frame() {
        let cmd = ZAddIncr::new("myzset", 5.0, "member");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZADD"),
                bulk("myzset"),
                bulk("INCR"),
                bulk("5"),
                bulk("member"),
            ])
        );
    }

    #[test]
    fn zadd_incr_parse_score() {
        let cmd = ZAddIncr::new("myzset", 5.0, "m");
        let frame = Frame::BulkString(Some(Bytes::from("7.5")));
        let result = cmd.parse_response(frame).unwrap().unwrap();
        assert!((result - 7.5).abs() < f64::EPSILON);
    }

    #[test]
    fn zadd_incr_parse_null() {
        let cmd = ZAddIncr::new("myzset", 5.0, "m");
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    // -- ZDiff --

    #[test]
    fn zdiff_to_frame() {
        let cmd = ZDiff::new(vec!["s1", "s2"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("ZDIFF"), bulk("2"), bulk("s1"), bulk("s2")])
        );
    }

    #[test]
    fn zdiff_parse_members() {
        let cmd = ZDiff::new(vec!["s1"]);
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::BulkString(Some(Bytes::from("b"))),
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    #[test]
    fn zdiff_with_scores_to_frame() {
        let cmd = ZDiffWithScores::new(vec!["s1", "s2"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZDIFF"),
                bulk("2"),
                bulk("s1"),
                bulk("s2"),
                bulk("WITHSCORES"),
            ])
        );
    }

    #[test]
    fn zdiff_with_scores_parse_flat() {
        let cmd = ZDiffWithScores::new(vec!["s1"]);
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::BulkString(Some(Bytes::from("1.5"))),
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, Bytes::from("a"));
        assert!((result[0].1 - 1.5).abs() < f64::EPSILON);
    }

    // -- ZUnion / ZInter --

    #[test]
    fn zunion_to_frame() {
        let cmd = ZUnion::new(vec!["s1", "s2"]).aggregate(Aggregate::Max);
        match cmd.to_frame() {
            Frame::Array(Some(args)) => {
                assert_eq!(args[0], bulk("ZUNION"));
                assert_eq!(args[1], bulk("2"));
                assert!(args.contains(&bulk("AGGREGATE")));
                assert!(args.contains(&bulk("MAX")));
                assert!(!args.contains(&bulk("WITHSCORES")));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn zunion_with_scores_to_frame() {
        let cmd = ZUnionWithScores::new(vec!["s1"]).weights(vec![2.0]);
        match cmd.to_frame() {
            Frame::Array(Some(args)) => {
                assert_eq!(args[0], bulk("ZUNION"));
                assert!(args.contains(&bulk("WEIGHTS")));
                assert!(args.contains(&bulk("WITHSCORES")));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn zinter_to_frame() {
        let cmd = ZInter::new(vec!["s1", "s2"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("ZINTER"), bulk("2"), bulk("s1"), bulk("s2")])
        );
    }

    #[test]
    fn zinter_with_scores_parse_pairs_resp3() {
        let cmd = ZInterWithScores::new(vec!["s1"]);
        let frame = array(vec![array(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::Double(3.0),
        ])]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, Bytes::from("a"));
        assert!((result[0].1 - 3.0).abs() < f64::EPSILON);
    }

    // -- BZMPop --

    #[test]
    fn bzmpop_to_frame() {
        let cmd = BZMPop::new(1.5, vec!["k1", "k2"], ZMPopDirection::Min).count(2);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("BZMPOP"),
                bulk("1.5"),
                bulk("2"),
                bulk("k1"),
                bulk("k2"),
                bulk("MIN"),
                bulk("COUNT"),
                bulk("2"),
            ])
        );
    }

    #[test]
    fn bzmpop_parse_null() {
        let cmd = BZMPop::new(1.0, vec!["k1"], ZMPopDirection::Max);
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    #[test]
    fn bzmpop_parse_result() {
        let cmd = BZMPop::new(1.0, vec!["k1"], ZMPopDirection::Min);
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("k1"))),
            array(vec![array(vec![
                Frame::BulkString(Some(Bytes::from("a"))),
                Frame::BulkString(Some(Bytes::from("1.0"))),
            ])]),
        ]);
        let result = cmd.parse_response(frame).unwrap().unwrap();
        assert_eq!(result.0, Bytes::from("k1"));
        assert_eq!(result.1[0].0, Bytes::from("a"));
        assert!((result.1[0].1 - 1.0).abs() < f64::EPSILON);
    }

    // -- ZRevRange --

    #[test]
    fn zrevrange_to_frame() {
        let cmd = ZRevRange::new("myzset", 0, -1);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZREVRANGE"),
                bulk("myzset"),
                bulk("0"),
                bulk("-1"),
            ])
        );
    }

    #[test]
    fn zrevrange_parse_members() {
        let cmd = ZRevRange::new("myzset", 0, -1);
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("b"))),
            Frame::BulkString(Some(Bytes::from("a"))),
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![Bytes::from("b"), Bytes::from("a")]);
    }

    #[test]
    fn zrevrange_parse_error_on_integer() {
        let cmd = ZRevRange::new("myzset", 0, -1);
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    #[test]
    fn zrevrange_with_scores_to_frame() {
        let cmd = ZRevRangeWithScores::new("myzset", 0, -1);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZREVRANGE"),
                bulk("myzset"),
                bulk("0"),
                bulk("-1"),
                bulk("WITHSCORES"),
            ])
        );
    }

    #[test]
    fn zrevrange_with_scores_parse_flat() {
        let cmd = ZRevRangeWithScores::new("myzset", 0, -1);
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("b"))),
            Frame::BulkString(Some(Bytes::from("2.0"))),
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::BulkString(Some(Bytes::from("1.0"))),
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, Bytes::from("b"));
        assert!((result[0].1 - 2.0).abs() < f64::EPSILON);
        assert_eq!(result[1].0, Bytes::from("a"));
    }

    // -- ZRangeByLex --

    #[test]
    fn zrangebylex_to_frame() {
        let cmd = ZRangeByLex::new("myzset", "-", "+");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZRANGEBYLEX"),
                bulk("myzset"),
                bulk("-"),
                bulk("+"),
            ])
        );
    }

    #[test]
    fn zrangebylex_with_limit_to_frame() {
        let cmd = ZRangeByLex::new("myzset", "[a", "(c").limit(0, 10);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZRANGEBYLEX"),
                bulk("myzset"),
                bulk("[a"),
                bulk("(c"),
                bulk("LIMIT"),
                bulk("0"),
                bulk("10"),
            ])
        );
    }

    #[test]
    fn zrangebylex_parse_members() {
        let cmd = ZRangeByLex::new("myzset", "-", "+");
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::BulkString(Some(Bytes::from("b"))),
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    // -- ZRevRangeByLex --

    #[test]
    fn zrevrangebylex_to_frame() {
        let cmd = ZRevRangeByLex::new("myzset", "+", "-");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZREVRANGEBYLEX"),
                bulk("myzset"),
                bulk("+"),
                bulk("-"),
            ])
        );
    }

    #[test]
    fn zrevrangebylex_with_limit_to_frame() {
        let cmd = ZRevRangeByLex::new("myzset", "(c", "[a").limit(1, 5);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZREVRANGEBYLEX"),
                bulk("myzset"),
                bulk("(c"),
                bulk("[a"),
                bulk("LIMIT"),
                bulk("1"),
                bulk("5"),
            ])
        );
    }

    // -- ZRevRangeByScore --

    #[test]
    fn zrevrangebyscore_to_frame() {
        let cmd = ZRevRangeByScore::new("myzset", "+inf", "-inf");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZREVRANGEBYSCORE"),
                bulk("myzset"),
                bulk("+inf"),
                bulk("-inf"),
            ])
        );
    }

    #[test]
    fn zrevrangebyscore_with_limit_to_frame() {
        let cmd = ZRevRangeByScore::new("myzset", "10", "0").limit(2, 3);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZREVRANGEBYSCORE"),
                bulk("myzset"),
                bulk("10"),
                bulk("0"),
                bulk("LIMIT"),
                bulk("2"),
                bulk("3"),
            ])
        );
    }

    #[test]
    fn zrevrangebyscore_parse_members() {
        let cmd = ZRevRangeByScore::new("myzset", "+inf", "-inf");
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("b"))),
            Frame::BulkString(Some(Bytes::from("a"))),
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![Bytes::from("b"), Bytes::from("a")]);
    }

    #[test]
    fn zrevrangebyscore_with_scores_to_frame() {
        let cmd = ZRevRangeByScoreWithScores::new("myzset", "+inf", "-inf").limit(0, 2);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZREVRANGEBYSCORE"),
                bulk("myzset"),
                bulk("+inf"),
                bulk("-inf"),
                bulk("WITHSCORES"),
                bulk("LIMIT"),
                bulk("0"),
                bulk("2"),
            ])
        );
    }

    #[test]
    fn zrevrangebyscore_with_scores_parse_pairs_resp3() {
        let cmd = ZRevRangeByScoreWithScores::new("myzset", "+inf", "-inf");
        let frame = array(vec![array(vec![
            Frame::BulkString(Some(Bytes::from("b"))),
            Frame::Double(2.0),
        ])]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, Bytes::from("b"));
        assert!((result[0].1 - 2.0).abs() < f64::EPSILON);
    }
}
