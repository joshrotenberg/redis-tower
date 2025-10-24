//! Sorted set commands for Redis.
//!
//! Sorted sets are similar to sets but each member has an associated score,
//! allowing members to be ordered from lowest to highest score.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// Add one or more members to a sorted set, or update its score if it already exists.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::sorted_sets::Zadd;
/// let cmd = Zadd::new("leaderboard")
///     .member(100.0, "player1")
///     .member(200.0, "player2");
/// ```
#[derive(Debug, Clone)]
pub struct Zadd {
    pub(crate) key: String,
    pub(crate) members: Vec<(f64, Bytes)>,
    pub(crate) nx: bool,
    pub(crate) xx: bool,
    pub(crate) gt: bool,
    pub(crate) lt: bool,
    pub(crate) ch: bool,
    pub(crate) incr: bool,
}

impl Zadd {
    /// Create a new ZADD command for the given key.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            members: Vec::new(),
            nx: false,
            xx: false,
            gt: false,
            lt: false,
            ch: false,
            incr: false,
        }
    }

    /// Add a member with its score.
    pub fn member(mut self, score: f64, member: impl Into<Bytes>) -> Self {
        self.members.push((score, member.into()));
        self
    }

    /// Only add new elements. Don't update already existing elements.
    pub fn nx(mut self) -> Self {
        self.nx = true;
        self
    }

    /// Only update elements that already exist. Don't add new elements.
    pub fn xx(mut self) -> Self {
        self.xx = true;
        self
    }

    /// Only update existing elements if the new score is greater than the current score.
    pub fn gt(mut self) -> Self {
        self.gt = true;
        self
    }

    /// Only update existing elements if the new score is less than the current score.
    pub fn lt(mut self) -> Self {
        self.lt = true;
        self
    }

    /// Modify the return value to return the number of changed elements.
    pub fn ch(mut self) -> Self {
        self.ch = true;
        self
    }

    /// When this option is specified ZADD acts like ZINCRBY.
    pub fn incr(mut self) -> Self {
        self.incr = true;
        self
    }
}

impl Command for Zadd {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from_static(b"ZADD"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ];

        if self.nx {
            args.push(Frame::BulkString(Some(Bytes::from_static(b"NX"))));
        }
        if self.xx {
            args.push(Frame::BulkString(Some(Bytes::from_static(b"XX"))));
        }
        if self.gt {
            args.push(Frame::BulkString(Some(Bytes::from_static(b"GT"))));
        }
        if self.lt {
            args.push(Frame::BulkString(Some(Bytes::from_static(b"LT"))));
        }
        if self.ch {
            args.push(Frame::BulkString(Some(Bytes::from_static(b"CH"))));
        }
        if self.incr {
            args.push(Frame::BulkString(Some(Bytes::from_static(b"INCR"))));
        }

        for (score, member) in &self.members {
            args.push(Frame::BulkString(Some(Bytes::from(score.to_string()))));
            args.push(Frame::BulkString(Some(member.clone())));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Remove one or more members from a sorted set.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::sorted_sets::Zrem;
/// let cmd = Zrem::new("leaderboard")
///     .member("player1")
///     .member("player2");
/// ```
#[derive(Debug, Clone)]
pub struct Zrem {
    pub(crate) key: String,
    pub(crate) members: Vec<Bytes>,
}

impl Zrem {
    /// Create a new ZREM command for the given key.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            members: Vec::new(),
        }
    }

    /// Add a member to remove.
    pub fn member(mut self, member: impl Into<Bytes>) -> Self {
        self.members.push(member.into());
        self
    }
}

impl Command for Zrem {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from_static(b"ZREM"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ];

        for member in &self.members {
            args.push(Frame::BulkString(Some(member.clone())));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Get the number of members in a sorted set.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::sorted_sets::Zcard;
/// let cmd = Zcard::new("leaderboard");
/// ```
#[derive(Debug, Clone)]
pub struct Zcard {
    pub(crate) key: String,
}

impl Zcard {
    /// Create a new ZCARD command for the given key.
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Zcard {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from_static(b"ZCARD"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Get the score associated with the given member in a sorted set.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::sorted_sets::Zscore;
/// let cmd = Zscore::new("leaderboard", "player1");
/// ```
#[derive(Debug, Clone)]
pub struct Zscore {
    pub(crate) key: String,
    pub(crate) member: Bytes,
}

impl Zscore {
    /// Create a new ZSCORE command for the given key and member.
    pub fn new(key: impl Into<String>, member: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            member: member.into(),
        }
    }
}

impl Command for Zscore {
    type Response = Option<f64>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from_static(b"ZSCORE"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(self.member.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(bytes)) => {
                let s = String::from_utf8_lossy(&bytes);
                s.parse::<f64>()
                    .map(Some)
                    .map_err(|e| RedisError::Protocol(format!("Invalid float: {}", e)))
            }
            Frame::BulkString(None) | Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Unexpected response type".to_string())),
        }
    }
}

/// Return a range of members in a sorted set, by index.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::sorted_sets::Zrange;
/// let cmd = Zrange::new("leaderboard", 0, 9)
///     .withscores();
/// ```
#[derive(Debug, Clone)]
pub struct Zrange {
    pub(crate) key: String,
    pub(crate) start: i64,
    pub(crate) stop: i64,
    pub(crate) withscores: bool,
}

impl Zrange {
    /// Create a new ZRANGE command for the given key and range.
    pub fn new(key: impl Into<String>, start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            start,
            stop,
            withscores: false,
        }
    }

    /// Return the scores along with the members.
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }
}

/// Result from ZRANGE with scores.
#[derive(Debug, Clone, PartialEq)]
pub struct ZrangeResult {
    /// Members with their scores as (member, score) tuples.
    pub members: Vec<(Bytes, f64)>,
}

impl Command for Zrange {
    type Response = ZrangeResult;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from_static(b"ZRANGE"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(Bytes::from(self.start.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.stop.to_string()))),
        ];

        if self.withscores {
            args.push(Frame::BulkString(Some(Bytes::from_static(b"WITHSCORES"))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                if items.is_empty() {
                    return Ok(ZrangeResult {
                        members: Vec::new(),
                    });
                }

                let mut members = Vec::new();

                // Detect if scores are present by checking if we can parse the second element as a float
                // If WITHSCORES is used, array length is even (member, score pairs)
                // If not used, array contains only members
                let has_scores = if items.len() >= 2 {
                    // Try to parse second element as float to detect WITHSCORES
                    if let Frame::BulkString(Some(bytes)) = &items[1] {
                        String::from_utf8_lossy(bytes).parse::<f64>().is_ok()
                    } else {
                        false
                    }
                } else {
                    false
                };

                let mut i = 0;
                while i < items.len() {
                    let member = match &items[i] {
                        Frame::BulkString(Some(bytes)) => bytes.clone(),
                        _ => {
                            return Err(RedisError::Protocol(
                                "Expected bulk string for member".to_string(),
                            ));
                        }
                    };

                    let score = if has_scores && i + 1 < items.len() {
                        match &items[i + 1] {
                            Frame::BulkString(Some(bytes)) => {
                                let s = String::from_utf8_lossy(bytes);
                                s.parse::<f64>().map_err(|e| {
                                    RedisError::Protocol(format!("Invalid float: {}", e))
                                })?
                            }
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected bulk string for score".to_string(),
                                ));
                            }
                        }
                    } else {
                        0.0 // Default score when WITHSCORES not used
                    };

                    members.push((member, score));
                    i += if has_scores { 2 } else { 1 };
                }

                Ok(ZrangeResult { members })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Unexpected response type".to_string())),
        }
    }
}

/// Return a range of members in a sorted set, by index, with scores ordered from high to low.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::sorted_sets::Zrevrange;
/// let cmd = Zrevrange::new("leaderboard", 0, 9)
///     .withscores();
/// ```
#[derive(Debug, Clone)]
pub struct Zrevrange {
    pub(crate) key: String,
    pub(crate) start: i64,
    pub(crate) stop: i64,
    pub(crate) withscores: bool,
}

impl Zrevrange {
    /// Create a new ZREVRANGE command for the given key and range.
    pub fn new(key: impl Into<String>, start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            start,
            stop,
            withscores: false,
        }
    }

    /// Return the scores along with the members.
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }
}

impl Command for Zrevrange {
    type Response = ZrangeResult;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from_static(b"ZREVRANGE"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(Bytes::from(self.start.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.stop.to_string()))),
        ];

        if self.withscores {
            args.push(Frame::BulkString(Some(Bytes::from_static(b"WITHSCORES"))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Zrange::parse_response(frame)
    }
}

/// Determine the index of a member in a sorted set.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::sorted_sets::Zrank;
/// let cmd = Zrank::new("leaderboard", "player1");
/// ```
#[derive(Debug, Clone)]
pub struct Zrank {
    pub(crate) key: String,
    pub(crate) member: Bytes,
}

impl Zrank {
    /// Create a new ZRANK command for the given key and member.
    pub fn new(key: impl Into<String>, member: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            member: member.into(),
        }
    }
}

impl Command for Zrank {
    type Response = Option<i64>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from_static(b"ZRANK"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(self.member.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(Some(n)),
            Frame::BulkString(None) | Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Unexpected response type".to_string())),
        }
    }
}

/// Determine the index of a member in a sorted set, with scores ordered from high to low.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::sorted_sets::Zrevrank;
/// let cmd = Zrevrank::new("leaderboard", "player1");
/// ```
#[derive(Debug, Clone)]
pub struct Zrevrank {
    pub(crate) key: String,
    pub(crate) member: Bytes,
}

impl Zrevrank {
    /// Create a new ZREVRANK command for the given key and member.
    pub fn new(key: impl Into<String>, member: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            member: member.into(),
        }
    }
}

impl Command for Zrevrank {
    type Response = Option<i64>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from_static(b"ZREVRANK"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(self.member.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Zrank::parse_response(frame)
    }
}

/// Increment the score of a member in a sorted set.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::sorted_sets::Zincrby;
/// let cmd = Zincrby::new("leaderboard", 10.0, "player1");
/// ```
#[derive(Debug, Clone)]
pub struct Zincrby {
    pub(crate) key: String,
    pub(crate) increment: f64,
    pub(crate) member: Bytes,
}

impl Zincrby {
    /// Create a new ZINCRBY command for the given key, increment, and member.
    pub fn new(key: impl Into<String>, increment: f64, member: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            increment,
            member: member.into(),
        }
    }
}

impl Command for Zincrby {
    type Response = f64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from_static(b"ZINCRBY"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(Bytes::from(self.increment.to_string()))),
            Frame::BulkString(Some(self.member.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(bytes)) => {
                let s = String::from_utf8_lossy(&bytes);
                s.parse::<f64>()
                    .map_err(|e| RedisError::Protocol(format!("Invalid float: {}", e)))
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Unexpected response type".to_string())),
        }
    }
}

/// Incrementally iterate sorted sets elements and associated scores.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::sorted_sets::Zscan;
/// let cmd = Zscan::new("leaderboard", 0)
///     .pattern("player*")
///     .count(100);
/// ```
#[derive(Debug, Clone)]
pub struct Zscan {
    pub(crate) key: String,
    pub(crate) cursor: u64,
    pub(crate) pattern: Option<String>,
    pub(crate) count: Option<usize>,
}

impl Zscan {
    /// Create a new ZSCAN command for the given key and cursor.
    pub fn new(key: impl Into<String>, cursor: u64) -> Self {
        Self {
            key: key.into(),
            cursor,
            pattern: None,
            count: None,
        }
    }

    /// Only iterate elements matching a given glob-style pattern.
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Set the number of elements to return per iteration (hint).
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }
}

/// Result from ZSCAN.
#[derive(Debug, Clone, PartialEq)]
pub struct ZscanResult {
    /// The cursor to use for the next iteration (0 means iteration is complete).
    pub cursor: u64,
    /// Members with their scores as (member, score) tuples.
    pub members: Vec<(Bytes, f64)>,
}

impl Command for Zscan {
    type Response = ZscanResult;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from_static(b"ZSCAN"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(Bytes::from(self.cursor.to_string()))),
        ];

        if let Some(pattern) = &self.pattern {
            args.push(Frame::BulkString(Some(Bytes::from_static(b"MATCH"))));
            args.push(Frame::BulkString(Some(Bytes::from(pattern.clone()))));
        }

        if let Some(count) = self.count {
            args.push(Frame::BulkString(Some(Bytes::from_static(b"COUNT"))));
            args.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(parts) if parts.len() == 2 => {
                let cursor = match &parts[0] {
                    Frame::BulkString(Some(bytes)) => {
                        let s = String::from_utf8_lossy(bytes);
                        s.parse::<u64>().map_err(|e| {
                            RedisError::Protocol(format!("Invalid cursor value: {}", e))
                        })?
                    }
                    _ => {
                        return Err(RedisError::Protocol(
                            "Expected bulk string for cursor".to_string(),
                        ));
                    }
                };

                let members = match &parts[1] {
                    Frame::Array(items) => {
                        let mut result = Vec::new();
                        let mut i = 0;

                        while i < items.len() {
                            let member = match &items[i] {
                                Frame::BulkString(Some(bytes)) => bytes.clone(),
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "Expected bulk string for member".to_string(),
                                    ));
                                }
                            };

                            let score = if i + 1 < items.len() {
                                match &items[i + 1] {
                                    Frame::BulkString(Some(bytes)) => {
                                        let s = String::from_utf8_lossy(bytes);
                                        s.parse::<f64>().map_err(|e| {
                                            RedisError::Protocol(format!("Invalid float: {}", e))
                                        })?
                                    }
                                    _ => {
                                        return Err(RedisError::Protocol(
                                            "Expected bulk string for score".to_string(),
                                        ));
                                    }
                                }
                            } else {
                                return Err(RedisError::Protocol(
                                    "Missing score for member".to_string(),
                                ));
                            };

                            result.push((member, score));
                            i += 2;
                        }

                        result
                    }
                    _ => {
                        return Err(RedisError::Protocol(
                            "Expected array for members".to_string(),
                        ));
                    }
                };

                Ok(ZscanResult { cursor, members })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol(
                "Expected array with 2 elements".to_string(),
            )),
        }
    }
}

/// ZRANDMEMBER command - Get random member(s) from a sorted set
///
/// Returns one or more random members from the sorted set.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ZRandMember;
///
/// // Get one random member
/// let cmd = ZRandMember::new("myzset");
///
/// // Get 3 random members
/// let cmd = ZRandMember::new("myzset").count(3);
///
/// // Get 3 random members with scores
/// let cmd = ZRandMember::new("myzset").count(3).withscores();
/// ```
#[derive(Debug, Clone)]
pub struct ZRandMember {
    key: String,
    count: Option<i64>,
    withscores: bool,
}

impl ZRandMember {
    /// Create a new ZRANDMEMBER command (returns single member)
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
            withscores: false,
        }
    }

    /// Specify number of members to return
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Return members with their scores
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }
}

impl Command for ZRandMember {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("ZRANDMEMBER"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        if self.withscores {
            frames.push(Frame::BulkString(Some(Bytes::from("WITHSCORES"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                // Single member without count
                Ok(vec![String::from_utf8_lossy(&data).into_owned()])
            }
            Frame::Array(items) => {
                let mut members = Vec::new();
                for item in items {
                    if let Frame::BulkString(Some(data)) = item {
                        members.push(String::from_utf8_lossy(&data).into_owned());
                    }
                }
                Ok(members)
            }
            Frame::Null => Ok(vec![]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZUNIONSTORE command - Union multiple sorted sets and store result
///
/// Computes the union of multiple sorted sets and stores the result in destination.
/// By default, the resulting score is the sum of all scores from the input sets.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ZUnionStore;
///
/// // Simple union
/// let cmd = ZUnionStore::new("dest", vec!["set1".to_string(), "set2".to_string()]);
///
/// // With weights
/// let cmd = ZUnionStore::new("dest", vec!["set1".to_string(), "set2".to_string()])
///     .weights(vec![2.0, 3.0]);
/// ```
#[derive(Debug, Clone)]
pub struct ZUnionStore {
    destination: String,
    keys: Vec<String>,
    weights: Option<Vec<f64>>,
    aggregate: Option<String>,
}

impl ZUnionStore {
    /// Create a new ZUNIONSTORE command
    pub fn new(destination: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            destination: destination.into(),
            keys,
            weights: None,
            aggregate: None,
        }
    }

    /// Set weights for each sorted set
    pub fn weights(mut self, weights: Vec<f64>) -> Self {
        self.weights = Some(weights);
        self
    }

    /// Set aggregation method (SUM, MIN, MAX)
    pub fn aggregate(mut self, aggregate: impl Into<String>) -> Self {
        self.aggregate = Some(aggregate.into());
        self
    }
}

impl Command for ZUnionStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("ZUNIONSTORE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.destination.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        if let Some(weights) = &self.weights {
            frames.push(Frame::BulkString(Some(Bytes::from("WEIGHTS"))));
            for weight in weights {
                frames.push(Frame::BulkString(Some(Bytes::from(weight.to_string()))));
            }
        }

        if let Some(aggregate) = &self.aggregate {
            frames.push(Frame::BulkString(Some(Bytes::from("AGGREGATE"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                aggregate.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZINTERSTORE command - Intersect multiple sorted sets and store result
///
/// Computes the intersection of multiple sorted sets and stores the result in destination.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ZInterStore;
///
/// let cmd = ZInterStore::new("dest", vec!["set1".to_string(), "set2".to_string()]);
/// ```
#[derive(Debug, Clone)]
pub struct ZInterStore {
    destination: String,
    keys: Vec<String>,
    weights: Option<Vec<f64>>,
    aggregate: Option<String>,
}

impl ZInterStore {
    /// Create a new ZINTERSTORE command
    pub fn new(destination: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            destination: destination.into(),
            keys,
            weights: None,
            aggregate: None,
        }
    }

    /// Set weights for each sorted set
    pub fn weights(mut self, weights: Vec<f64>) -> Self {
        self.weights = Some(weights);
        self
    }

    /// Set aggregation method (SUM, MIN, MAX)
    pub fn aggregate(mut self, aggregate: impl Into<String>) -> Self {
        self.aggregate = Some(aggregate.into());
        self
    }
}

impl Command for ZInterStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("ZINTERSTORE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.destination.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        if let Some(weights) = &self.weights {
            frames.push(Frame::BulkString(Some(Bytes::from("WEIGHTS"))));
            for weight in weights {
                frames.push(Frame::BulkString(Some(Bytes::from(weight.to_string()))));
            }
        }

        if let Some(aggregate) = &self.aggregate {
            frames.push(Frame::BulkString(Some(Bytes::from("AGGREGATE"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                aggregate.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZDIFFSTORE command - Diff multiple sorted sets and store result (Redis 6.2+)
///
/// Computes the difference of multiple sorted sets and stores the result in destination.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ZDiffStore;
///
/// let cmd = ZDiffStore::new("dest", vec!["set1".to_string(), "set2".to_string()]);
/// ```
#[derive(Debug, Clone)]
pub struct ZDiffStore {
    destination: String,
    keys: Vec<String>,
}

impl ZDiffStore {
    /// Create a new ZDIFFSTORE command
    pub fn new(destination: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            destination: destination.into(),
            keys,
        }
    }
}

impl Command for ZDiffStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("ZDIFFSTORE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.destination.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZMPOP command - Pop members from sorted sets (Redis 7.0+)
///
/// Pops one or more members with the lowest or highest scores from one or more sorted sets.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ZMPop;
///
/// // Pop lowest score from one set
/// let cmd = ZMPop::new(vec!["myzset"], true); // true = MIN
///
/// // Pop 3 highest scores from multiple sets
/// let cmd = ZMPop::new(vec!["zset1", "zset2"], false).count(3); // false = MAX
/// ```
#[derive(Debug, Clone)]
pub struct ZMPop {
    keys: Vec<String>,
    min: bool,
    count: Option<i64>,
}

impl ZMPop {
    /// Create a new ZMPOP command
    ///
    /// # Arguments
    /// * `keys` - Sorted set keys to pop from
    /// * `min` - If true, pop MIN (lowest scores); if false, pop MAX (highest scores)
    pub fn new(keys: Vec<String>, min: bool) -> Self {
        Self {
            keys,
            min,
            count: None,
        }
    }

    /// Specify number of members to pop
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for ZMPop {
    type Response = Option<(String, Vec<(String, f64)>)>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("ZMPOP"))),
            Frame::BulkString(Some(Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        frames.push(Frame::BulkString(Some(Bytes::from(if self.min {
            "MIN"
        } else {
            "MAX"
        }))));

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(elements) if elements.len() == 2 => {
                // [key, [[member, score], ...]]
                let key = match &elements[0] {
                    Frame::BulkString(Some(k)) => String::from_utf8_lossy(k).to_string(),
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                let members = match &elements[1] {
                    Frame::Array(pairs) => {
                        let mut result = Vec::new();
                        for pair in pairs {
                            if let Frame::Array(ms) = pair
                                && ms.len() == 2
                            {
                                let member = match &ms[0] {
                                    Frame::BulkString(Some(m)) => {
                                        String::from_utf8_lossy(m).to_string()
                                    }
                                    _ => return Err(RedisError::UnexpectedResponse),
                                };
                                let score = match &ms[1] {
                                    Frame::BulkString(Some(s)) => {
                                        String::from_utf8_lossy(s).parse::<f64>().unwrap()
                                    }
                                    _ => return Err(RedisError::UnexpectedResponse),
                                };
                                result.push((member, score));
                            }
                        }
                        result
                    }
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                Ok(Some((key, members)))
            }
            Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// BZMPOP command - Blocking pop from sorted sets (Redis 7.0+)
///
/// Blocking variant of ZMPOP with a timeout.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::BZMPop;
///
/// // Block for up to 5 seconds waiting to pop lowest score
/// let cmd = BZMPop::new(5.0, vec!["myzset"], true);
///
/// // Block for 3 highest scores from multiple sets
/// let cmd = BZMPop::new(3.0, vec!["zset1", "zset2"], false).count(3);
/// ```
#[derive(Debug, Clone)]
pub struct BZMPop {
    timeout: f64,
    keys: Vec<String>,
    min: bool,
    count: Option<i64>,
}

impl BZMPop {
    /// Create a new BZMPOP command
    ///
    /// # Arguments
    /// * `timeout` - Timeout in seconds (0 = block indefinitely)
    /// * `keys` - Sorted set keys to pop from
    /// * `min` - If true, pop MIN (lowest scores); if false, pop MAX (highest scores)
    pub fn new(timeout: f64, keys: Vec<String>, min: bool) -> Self {
        Self {
            timeout,
            keys,
            min,
            count: None,
        }
    }

    /// Specify number of members to pop
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for BZMPop {
    type Response = Option<(String, Vec<(String, f64)>)>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("BZMPOP"))),
            Frame::BulkString(Some(Bytes::from(self.timeout.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        frames.push(Frame::BulkString(Some(Bytes::from(if self.min {
            "MIN"
        } else {
            "MAX"
        }))));

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        // Same parsing as ZMPOP
        ZMPop::parse_response(frame)
    }
}

// Read-only trait implementations for cluster read-from-replica support
use crate::read_preference::ReadOnly;

impl ReadOnly for Zcard {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Zscore {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Zrange {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Zrevrange {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Zrank {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Zrevrank {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Zscan {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for ZRandMember {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands - not read-only
impl ReadOnly for ZUnionStore {}
impl ReadOnly for ZInterStore {}
impl ReadOnly for ZDiffStore {}
impl ReadOnly for ZMPop {}
impl ReadOnly for BZMPop {}

// Write commands - explicitly implement with default (false) for clarity
impl ReadOnly for Zadd {}
impl ReadOnly for Zrem {}
impl ReadOnly for Zincrby {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zadd_frame() {
        let cmd = Zadd::new("myzset").member(1.0, "one").member(2.0, "two");

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 6);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_zadd_with_options() {
        let cmd = Zadd::new("myzset").member(1.0, "one").nx().ch();

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert!(items.len() >= 6);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_zrem_frame() {
        let cmd = Zrem::new("myzset").member("one").member("two");

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 4);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_zcard_frame() {
        let cmd = Zcard::new("myzset");

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 2);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_zscore_response() {
        let frame = Frame::BulkString(Some(Bytes::from("123.45")));
        let result = Zscore::parse_response(frame).unwrap();
        assert_eq!(result, Some(123.45));

        let frame = Frame::Null;
        let result = Zscore::parse_response(frame).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_zrange_frame() {
        let cmd = Zrange::new("myzset", 0, -1).withscores();

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 5);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_zrange_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("one"))),
            Frame::BulkString(Some(Bytes::from("1.0"))),
            Frame::BulkString(Some(Bytes::from("two"))),
            Frame::BulkString(Some(Bytes::from("2.0"))),
        ]);

        let result = Zrange::parse_response(frame).unwrap();
        assert_eq!(result.members.len(), 2);
        assert_eq!(result.members[0].1, 1.0);
        assert_eq!(result.members[1].1, 2.0);
    }

    #[test]
    fn test_zrank_response() {
        let frame = Frame::Integer(3);
        let result = Zrank::parse_response(frame).unwrap();
        assert_eq!(result, Some(3));

        let frame = Frame::Null;
        let result = Zrank::parse_response(frame).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_zincrby_frame() {
        let cmd = Zincrby::new("myzset", 2.5, "one");

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 4);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_zscan_frame() {
        let cmd = Zscan::new("myzset", 0).pattern("one*").count(10);

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 7); // ZSCAN key cursor MATCH pattern COUNT count
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_zscan_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("5"))),
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("member1"))),
                Frame::BulkString(Some(Bytes::from("1.0"))),
                Frame::BulkString(Some(Bytes::from("member2"))),
                Frame::BulkString(Some(Bytes::from("2.0"))),
            ]),
        ]);

        let result = Zscan::parse_response(frame).unwrap();
        assert_eq!(result.cursor, 5);
        assert_eq!(result.members.len(), 2);
        assert_eq!(result.members[0].1, 1.0);
        assert_eq!(result.members[1].1, 2.0);
    }
}

/// ZPOPMIN command - Remove and return members with the lowest scores
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ZPopMin;
///
/// // Pop single member with lowest score
/// let cmd = ZPopMin::new("leaderboard");
///
/// // Pop 3 members with lowest scores
/// let cmd = ZPopMin::new("leaderboard").count(3);
/// ```
#[derive(Debug, Clone)]
pub struct ZPopMin {
    key: String,
    count: Option<i64>,
}

impl ZPopMin {
    /// Create a new ZPOPMIN command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    /// Set number of members to pop
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for ZPopMin {
    type Response = Vec<(String, f64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("ZPOPMIN"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(count) = self.count {
            args.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                let mut i = 0;
                while i < items.len() {
                    if i + 1 < items.len() {
                        let member = match &items[i] {
                            Frame::BulkString(Some(data)) => {
                                String::from_utf8_lossy(data).into_owned()
                            }
                            _ => return Err(RedisError::UnexpectedResponse),
                        };

                        let score = match &items[i + 1] {
                            Frame::BulkString(Some(data)) => String::from_utf8_lossy(data)
                                .parse::<f64>()
                                .map_err(|_| RedisError::UnexpectedResponse)?,
                            _ => return Err(RedisError::UnexpectedResponse),
                        };

                        result.push((member, score));
                        i += 2;
                    } else {
                        break;
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZPOPMAX command - Remove and return members with the highest scores
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ZPopMax;
///
/// // Pop single member with highest score
/// let cmd = ZPopMax::new("leaderboard");
///
/// // Pop 5 members with highest scores
/// let cmd = ZPopMax::new("leaderboard").count(5);
/// ```
#[derive(Debug, Clone)]
pub struct ZPopMax {
    key: String,
    count: Option<i64>,
}

impl ZPopMax {
    /// Create a new ZPOPMAX command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    /// Set number of members to pop
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for ZPopMax {
    type Response = Vec<(String, f64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("ZPOPMAX"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(count) = self.count {
            args.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                let mut i = 0;
                while i < items.len() {
                    if i + 1 < items.len() {
                        let member = match &items[i] {
                            Frame::BulkString(Some(data)) => {
                                String::from_utf8_lossy(data).into_owned()
                            }
                            _ => return Err(RedisError::UnexpectedResponse),
                        };

                        let score = match &items[i + 1] {
                            Frame::BulkString(Some(data)) => String::from_utf8_lossy(data)
                                .parse::<f64>()
                                .map_err(|_| RedisError::UnexpectedResponse)?,
                            _ => return Err(RedisError::UnexpectedResponse),
                        };

                        result.push((member, score));
                        i += 2;
                    } else {
                        break;
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// BZPOPMIN command - Blocking ZPOPMIN
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::BZPopMin;
///
/// // Block for 5 seconds waiting for element
/// let cmd = BZPopMin::new(vec!["queue1", "queue2"], 5.0);
/// ```
#[derive(Debug, Clone)]
pub struct BZPopMin {
    keys: Vec<String>,
    timeout: f64,
}

impl BZPopMin {
    /// Create a new BZPOPMIN command with timeout in seconds
    pub fn new(keys: Vec<impl Into<String>>, timeout: f64) -> Self {
        Self {
            keys: keys.into_iter().map(|k| k.into()).collect(),
            timeout,
        }
    }
}

impl Command for BZPopMin {
    type Response = Option<(String, String, f64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![Frame::BulkString(Some(Bytes::from("BZPOPMIN")))];

        for key in &self.keys {
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        args.push(Frame::BulkString(Some(Bytes::from(
            self.timeout.to_string(),
        ))));

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) if items.len() == 3 => {
                let key = match &items[0] {
                    Frame::BulkString(Some(data)) => String::from_utf8_lossy(data).into_owned(),
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                let member = match &items[1] {
                    Frame::BulkString(Some(data)) => String::from_utf8_lossy(data).into_owned(),
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                let score = match &items[2] {
                    Frame::BulkString(Some(data)) => String::from_utf8_lossy(data)
                        .parse::<f64>()
                        .map_err(|_| RedisError::UnexpectedResponse)?,
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                Ok(Some((key, member, score)))
            }
            Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// BZPOPMAX command - Blocking ZPOPMAX
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::BZPopMax;
///
/// // Block for 10 seconds waiting for element
/// let cmd = BZPopMax::new(vec!["priority_queue"], 10.0);
/// ```
#[derive(Debug, Clone)]
pub struct BZPopMax {
    keys: Vec<String>,
    timeout: f64,
}

impl BZPopMax {
    /// Create a new BZPOPMAX command with timeout in seconds
    pub fn new(keys: Vec<impl Into<String>>, timeout: f64) -> Self {
        Self {
            keys: keys.into_iter().map(|k| k.into()).collect(),
            timeout,
        }
    }
}

impl Command for BZPopMax {
    type Response = Option<(String, String, f64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![Frame::BulkString(Some(Bytes::from("BZPOPMAX")))];

        for key in &self.keys {
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        args.push(Frame::BulkString(Some(Bytes::from(
            self.timeout.to_string(),
        ))));

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) if items.len() == 3 => {
                let key = match &items[0] {
                    Frame::BulkString(Some(data)) => String::from_utf8_lossy(data).into_owned(),
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                let member = match &items[1] {
                    Frame::BulkString(Some(data)) => String::from_utf8_lossy(data).into_owned(),
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                let score = match &items[2] {
                    Frame::BulkString(Some(data)) => String::from_utf8_lossy(data)
                        .parse::<f64>()
                        .map_err(|_| RedisError::UnexpectedResponse)?,
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                Ok(Some((key, member, score)))
            }
            Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZCOUNT command - Count members in a score range
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ZCount;
///
/// // Count members with scores between 1.0 and 5.0 (inclusive)
/// let cmd = ZCount::new("leaderboard", 1.0, 5.0);
/// ```
#[derive(Debug, Clone)]
pub struct ZCount {
    key: String,
    min: f64,
    max: f64,
}

impl ZCount {
    /// Create a new ZCOUNT command
    pub fn new(key: impl Into<String>, min: f64, max: f64) -> Self {
        Self {
            key: key.into(),
            min,
            max,
        }
    }
}

impl Command for ZCount {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("ZCOUNT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.min.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.max.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(count) => Ok(count),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for ZCount {
    fn is_read_only(&self) -> bool {
        true
    }
}

/// ZRANGEBYSCORE command - Return members in a score range
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ZRangeByScore;
///
/// // Get all members with scores 0-100
/// let cmd = ZRangeByScore::new("leaderboard", 0.0, 100.0);
///
/// // With scores and limit
/// let cmd = ZRangeByScore::new("leaderboard", 0.0, 100.0)
///     .withscores()
///     .limit(0, 10);
/// ```
#[derive(Debug, Clone)]
pub struct ZRangeByScore {
    key: String,
    min: f64,
    max: f64,
    withscores: bool,
    offset: Option<i64>,
    count: Option<i64>,
}

impl ZRangeByScore {
    /// Create a new ZRANGEBYSCORE command
    pub fn new(key: impl Into<String>, min: f64, max: f64) -> Self {
        Self {
            key: key.into(),
            min,
            max,
            withscores: false,
            offset: None,
            count: None,
        }
    }

    /// Include scores in the result
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }

    /// Limit results with offset and count
    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.offset = Some(offset);
        self.count = Some(count);
        self
    }
}

impl Command for ZRangeByScore {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("ZRANGEBYSCORE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.min.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.max.to_string()))),
        ];

        if self.withscores {
            args.push(Frame::BulkString(Some(Bytes::from("WITHSCORES"))));
        }

        if let (Some(offset), Some(count)) = (self.offset, self.count) {
            args.push(Frame::BulkString(Some(Bytes::from("LIMIT"))));
            args.push(Frame::BulkString(Some(Bytes::from(offset.to_string()))));
            args.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    if let Frame::BulkString(Some(data)) = item {
                        result.push(String::from_utf8_lossy(&data).into_owned());
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for ZRangeByScore {
    fn is_read_only(&self) -> bool {
        true
    }
}

/// ZMSCORE command - Get scores of multiple members
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ZMScore;
///
/// let cmd = ZMScore::new("leaderboard", vec!["player1", "player2", "player3"]);
/// ```
#[derive(Debug, Clone)]
pub struct ZMScore {
    key: String,
    members: Vec<String>,
}

impl ZMScore {
    /// Create a new ZMSCORE command
    pub fn new(key: impl Into<String>, members: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            members: members.into_iter().map(|m| m.into()).collect(),
        }
    }
}

impl Command for ZMScore {
    type Response = Vec<Option<f64>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("ZMSCORE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for member in &self.members {
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                member.as_bytes(),
            ))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            let score = String::from_utf8_lossy(&data)
                                .parse::<f64>()
                                .map_err(|_| RedisError::UnexpectedResponse)?;
                            result.push(Some(score));
                        }
                        Frame::Null | Frame::BulkString(None) => {
                            result.push(None);
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZREVRANGEBYSCORE - Return range of members by score in reverse order
///
/// Like ZRANGEBYSCORE but returns members in descending score order (high to low).
#[derive(Debug, Clone)]
pub struct ZRevRangeByScore {
    key: String,
    max: f64,
    min: f64,
    withscores: bool,
    offset: Option<i64>,
    count: Option<i64>,
}

impl ZRevRangeByScore {
    /// Create a new ZREVRANGEBYSCORE command
    pub fn new(key: impl Into<String>, max: f64, min: f64) -> Self {
        Self {
            key: key.into(),
            max,
            min,
            withscores: false,
            offset: None,
            count: None,
        }
    }

    /// Include scores in the result
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }

    /// Limit results with offset and count
    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.offset = Some(offset);
        self.count = Some(count);
        self
    }
}

impl Command for ZRevRangeByScore {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("ZREVRANGEBYSCORE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.max.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.min.to_string()))),
        ];

        if self.withscores {
            frames.push(Frame::BulkString(Some(Bytes::from("WITHSCORES"))));
        }

        if let (Some(offset), Some(count)) = (self.offset, self.count) {
            frames.push(Frame::BulkString(Some(Bytes::from("LIMIT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(offset.to_string()))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    if let Frame::BulkString(Some(data)) = item {
                        result.push(String::from_utf8_lossy(&data).into_owned());
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZRANGEBYLEX - Return range of members by lexicographic order
#[derive(Debug, Clone)]
pub struct ZRangeByLex {
    key: String,
    min: String,
    max: String,
    offset: Option<i64>,
    count: Option<i64>,
}

impl ZRangeByLex {
    /// Create a new ZRANGEBYLEX command
    pub fn new(key: impl Into<String>, min: impl Into<String>, max: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            min: min.into(),
            max: max.into(),
            offset: None,
            count: None,
        }
    }

    /// Limit results with offset and count
    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.offset = Some(offset);
        self.count = Some(count);
        self
    }
}

impl Command for ZRangeByLex {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("ZRANGEBYLEX"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.min.clone()))),
            Frame::BulkString(Some(Bytes::from(self.max.clone()))),
        ];

        if let (Some(offset), Some(count)) = (self.offset, self.count) {
            frames.push(Frame::BulkString(Some(Bytes::from("LIMIT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(offset.to_string()))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    if let Frame::BulkString(Some(data)) = item {
                        result.push(String::from_utf8_lossy(&data).into_owned());
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZREVRANGEBYLEX - Return range by lexicographic order in reverse
#[derive(Debug, Clone)]
pub struct ZRevRangeByLex {
    key: String,
    max: String,
    min: String,
    offset: Option<i64>,
    count: Option<i64>,
}

impl ZRevRangeByLex {
    /// Create a new ZREVRANGEBYLEX command
    pub fn new(key: impl Into<String>, max: impl Into<String>, min: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            max: max.into(),
            min: min.into(),
            offset: None,
            count: None,
        }
    }

    /// Limit results with offset and count
    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.offset = Some(offset);
        self.count = Some(count);
        self
    }
}

impl Command for ZRevRangeByLex {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("ZREVRANGEBYLEX"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.max.clone()))),
            Frame::BulkString(Some(Bytes::from(self.min.clone()))),
        ];

        if let (Some(offset), Some(count)) = (self.offset, self.count) {
            frames.push(Frame::BulkString(Some(Bytes::from("LIMIT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(offset.to_string()))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    if let Frame::BulkString(Some(data)) = item {
                        result.push(String::from_utf8_lossy(&data).into_owned());
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZLEXCOUNT - Count members between lexicographic range
#[derive(Debug, Clone)]
pub struct ZLexCount {
    key: String,
    min: String,
    max: String,
}

impl ZLexCount {
    /// Create a new ZLEXCOUNT command
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
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("ZLEXCOUNT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.min.clone()))),
            Frame::BulkString(Some(Bytes::from(self.max.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZREMRANGEBYSCORE - Remove members by score range
#[derive(Debug, Clone)]
pub struct ZRemRangeByScore {
    key: String,
    min: f64,
    max: f64,
}

impl ZRemRangeByScore {
    /// Create a new ZREMRANGEBYSCORE command
    pub fn new(key: impl Into<String>, min: f64, max: f64) -> Self {
        Self {
            key: key.into(),
            min,
            max,
        }
    }
}

impl Command for ZRemRangeByScore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("ZREMRANGEBYSCORE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.min.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.max.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZREMRANGEBYLEX - Remove members by lexicographic range
#[derive(Debug, Clone)]
pub struct ZRemRangeByLex {
    key: String,
    min: String,
    max: String,
}

impl ZRemRangeByLex {
    /// Create a new ZREMRANGEBYLEX command
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
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("ZREMRANGEBYLEX"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.min.clone()))),
            Frame::BulkString(Some(Bytes::from(self.max.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZREMRANGEBYRANK - Remove members by rank range
#[derive(Debug, Clone)]
pub struct ZRemRangeByRank {
    key: String,
    start: i64,
    stop: i64,
}

impl ZRemRangeByRank {
    /// Create a new ZREMRANGEBYRANK command
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
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("ZREMRANGEBYRANK"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.start.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.stop.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for ZMScore {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for ZRevRangeByScore {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for ZRangeByLex {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for ZRevRangeByLex {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for ZLexCount {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands
impl ReadOnly for ZRemRangeByScore {}
impl ReadOnly for ZRemRangeByLex {}
impl ReadOnly for ZRemRangeByRank {}

/// ZINTER command - intersect multiple sorted sets
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Zinter;
///
/// // Simple intersection
/// let cmd = Zinter::new(vec!["zset1", "zset2"]);
///
/// // With weights and aggregate function
/// let cmd = Zinter::new(vec!["zset1", "zset2"])
///     .weights(vec![2.0, 3.0])
///     .aggregate_sum();
/// ```
#[derive(Debug, Clone)]
pub struct Zinter {
    pub(crate) keys: Vec<String>,
    pub(crate) weights: Option<Vec<f64>>,
    pub(crate) aggregate: Option<String>,
    pub(crate) withscores: bool,
}

impl Zinter {
    /// Create a new ZINTER command
    pub fn new(keys: Vec<impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(|k| k.into()).collect(),
            weights: None,
            aggregate: None,
            withscores: false,
        }
    }

    /// Set weights for each input sorted set
    pub fn weights(mut self, weights: Vec<f64>) -> Self {
        self.weights = Some(weights);
        self
    }

    /// Aggregate scores using SUM (default)
    pub fn aggregate_sum(mut self) -> Self {
        self.aggregate = Some("SUM".to_string());
        self
    }

    /// Aggregate scores using MIN
    pub fn aggregate_min(mut self) -> Self {
        self.aggregate = Some("MIN".to_string());
        self
    }

    /// Aggregate scores using MAX
    pub fn aggregate_max(mut self) -> Self {
        self.aggregate = Some("MAX".to_string());
        self
    }

    /// Include scores in the result
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }
}

impl Command for Zinter {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("ZINTER"))),
            Frame::BulkString(Some(Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        if let Some(ref weights) = self.weights {
            frames.push(Frame::BulkString(Some(Bytes::from("WEIGHTS"))));
            for weight in weights {
                frames.push(Frame::BulkString(Some(Bytes::from(weight.to_string()))));
            }
        }

        if let Some(ref agg) = self.aggregate {
            frames.push(Frame::BulkString(Some(Bytes::from("AGGREGATE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(agg.clone()))));
        }

        if self.withscores {
            frames.push(Frame::BulkString(Some(Bytes::from("WITHSCORES"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => result.push(data),
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZUNION command - union multiple sorted sets
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Zunion;
///
/// // Simple union
/// let cmd = Zunion::new(vec!["zset1", "zset2"]);
///
/// // With weights and aggregate function
/// let cmd = Zunion::new(vec!["zset1", "zset2"])
///     .weights(vec![2.0, 3.0])
///     .aggregate_sum();
/// ```
#[derive(Debug, Clone)]
pub struct Zunion {
    pub(crate) keys: Vec<String>,
    pub(crate) weights: Option<Vec<f64>>,
    pub(crate) aggregate: Option<String>,
    pub(crate) withscores: bool,
}

impl Zunion {
    /// Create a new ZUNION command
    pub fn new(keys: Vec<impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(|k| k.into()).collect(),
            weights: None,
            aggregate: None,
            withscores: false,
        }
    }

    /// Set weights for each input sorted set
    pub fn weights(mut self, weights: Vec<f64>) -> Self {
        self.weights = Some(weights);
        self
    }

    /// Aggregate scores using SUM (default)
    pub fn aggregate_sum(mut self) -> Self {
        self.aggregate = Some("SUM".to_string());
        self
    }

    /// Aggregate scores using MIN
    pub fn aggregate_min(mut self) -> Self {
        self.aggregate = Some("MIN".to_string());
        self
    }

    /// Aggregate scores using MAX
    pub fn aggregate_max(mut self) -> Self {
        self.aggregate = Some("MAX".to_string());
        self
    }

    /// Include scores in the result
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }
}

impl Command for Zunion {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("ZUNION"))),
            Frame::BulkString(Some(Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        if let Some(ref weights) = self.weights {
            frames.push(Frame::BulkString(Some(Bytes::from("WEIGHTS"))));
            for weight in weights {
                frames.push(Frame::BulkString(Some(Bytes::from(weight.to_string()))));
            }
        }

        if let Some(ref agg) = self.aggregate {
            frames.push(Frame::BulkString(Some(Bytes::from("AGGREGATE"))));
            frames.push(Frame::BulkString(Some(Bytes::from(agg.clone()))));
        }

        if self.withscores {
            frames.push(Frame::BulkString(Some(Bytes::from("WITHSCORES"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => result.push(data),
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZDIFF command - difference of sorted sets
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Zdiff;
///
/// // Simple difference (elements in first set but not in others)
/// let cmd = Zdiff::new(vec!["zset1", "zset2", "zset3"]);
///
/// // With scores
/// let cmd = Zdiff::new(vec!["zset1", "zset2"]).withscores();
/// ```
#[derive(Debug, Clone)]
pub struct Zdiff {
    pub(crate) keys: Vec<String>,
    pub(crate) withscores: bool,
}

impl Zdiff {
    /// Create a new ZDIFF command
    pub fn new(keys: Vec<impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(|k| k.into()).collect(),
            withscores: false,
        }
    }

    /// Include scores in the result
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }
}

impl Command for Zdiff {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("ZDIFF"))),
            Frame::BulkString(Some(Bytes::from(self.keys.len().to_string()))),
        ];

        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        if self.withscores {
            frames.push(Frame::BulkString(Some(Bytes::from("WITHSCORES"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => result.push(data),
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZRANDMEMBER command - get random member(s) from a sorted set
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Zrandmember;
///
/// // Get single random member
/// let cmd = Zrandmember::new("myzset");
///
/// // Get multiple random members
/// let cmd = Zrandmember::new("myzset").count(3);
///
/// // Get with scores
/// let cmd = Zrandmember::new("myzset").count(3).withscores();
/// ```
#[derive(Debug, Clone)]
pub struct Zrandmember {
    pub(crate) key: String,
    pub(crate) count: Option<i64>,
    pub(crate) withscores: bool,
}

impl Zrandmember {
    /// Create a new ZRANDMEMBER command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
            withscores: false,
        }
    }

    /// Set count of members to return
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Include scores in the result
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }
}

impl Command for Zrandmember {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("ZRANDMEMBER"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        if self.withscores {
            frames.push(Frame::BulkString(Some(Bytes::from("WITHSCORES"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(vec![data]),
            Frame::BulkString(None) | Frame::Null => Ok(Vec::new()),
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => result.push(data),
                        Frame::BulkString(None) | Frame::Null => {}
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod advanced_tests {
    use super::*;

    #[test]
    fn test_zpopmin_frame() {
        let cmd = ZPopMin::new("myzset").count(3);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3); // ZPOPMIN + key + count
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_zpopmax_frame() {
        let cmd = ZPopMax::new("myzset");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 2); // ZPOPMAX + key
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bzpopmin_frame() {
        let cmd = BZPopMin::new(vec!["key1", "key2"], 5.0);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 4); // BZPOPMIN + 2 keys + timeout
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bzpopmax_response_null() {
        let response = BZPopMax::parse_response(Frame::Null).unwrap();
        assert!(response.is_none());
    }

    #[test]
    fn test_zcount_frame() {
        let cmd = ZCount::new("myzset", 1.0, 5.0);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 4); // ZCOUNT + key + min + max
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_zcount_response() {
        let response = ZCount::parse_response(Frame::Integer(42)).unwrap();
        assert_eq!(response, 42);
    }

    #[test]
    fn test_zrangebyscore_frame() {
        let cmd = ZRangeByScore::new("myzset", 0.0, 100.0)
            .withscores()
            .limit(0, 10);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert!(args.len() >= 7); // ZRANGEBYSCORE + key + min + max + WITHSCORES + LIMIT + offset + count
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_zmscore_frame() {
        let cmd = ZMScore::new("myzset", vec!["member1", "member2"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 4); // ZMSCORE + key + 2 members
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_zmscore_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("1.5"))),
            Frame::Null,
            Frame::BulkString(Some(Bytes::from("3.0"))),
        ]);

        let response = ZMScore::parse_response(frame).unwrap();
        assert_eq!(response.len(), 3);
        assert_eq!(response[0], Some(1.5));
        assert_eq!(response[1], None);
        assert_eq!(response[2], Some(3.0));
    }
}
