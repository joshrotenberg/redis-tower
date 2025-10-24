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
                        0.0
                    };

                    members.push((member, score));
                    i += 2;
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

// Read-only trait implementations for cluster read-from-replica support
use crate::cluster::read_preference::ReadOnly;

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
