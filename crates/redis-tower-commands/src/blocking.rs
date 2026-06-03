//! Blocking Redis commands.
//!
//! These commands hold the connection until data arrives or the timeout
//! expires. They should NOT go through caching layers.

use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// BLPOP key \[key ...\] timeout
///
/// Blocking left pop. Waits until an element is available or timeout.
/// Returns `None` on timeout, `Some((key, value))` on success.
pub struct BLPop {
    keys: Vec<String>,
    timeout: f64,
}

impl BLPop {
    /// Block on a single key. Timeout in seconds (0 = block indefinitely).
    pub fn new(key: impl Into<String>, timeout: f64) -> Self {
        Self {
            keys: vec![key.into()],
            timeout,
        }
    }

    /// Block on multiple keys. Returns from the first key that has data.
    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>, timeout: f64) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            timeout,
        }
    }
}

impl Command for BLPop {
    type Response = Option<(Bytes, Bytes)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("BLPOP")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        args.push(bulk(self.timeout.to_string()));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_key_value_or_null(frame)
    }

    fn name(&self) -> &str {
        "BLPOP"
    }
}

/// BRPOP key \[key ...\] timeout
///
/// Blocking right pop. Same as BLPOP but pops from the tail.
pub struct BRPop {
    keys: Vec<String>,
    timeout: f64,
}

impl BRPop {
    pub fn new(key: impl Into<String>, timeout: f64) -> Self {
        Self {
            keys: vec![key.into()],
            timeout,
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>, timeout: f64) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            timeout,
        }
    }
}

impl Command for BRPop {
    type Response = Option<(Bytes, Bytes)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("BRPOP")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        args.push(bulk(self.timeout.to_string()));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_key_value_or_null(frame)
    }

    fn name(&self) -> &str {
        "BRPOP"
    }
}

/// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
///
/// Blocking version of LMOVE.
pub struct BLMove {
    source: String,
    destination: String,
    wherefrom: ListDir,
    whereto: ListDir,
    timeout: f64,
}

/// Direction for blocking list move.
#[derive(Debug, Clone, Copy)]
pub enum ListDir {
    Left,
    Right,
}

impl ListDir {
    fn as_str(&self) -> &str {
        match self {
            ListDir::Left => "LEFT",
            ListDir::Right => "RIGHT",
        }
    }
}

impl BLMove {
    pub fn new(
        source: impl Into<String>,
        destination: impl Into<String>,
        wherefrom: ListDir,
        whereto: ListDir,
        timeout: f64,
    ) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
            wherefrom,
            whereto,
            timeout,
        }
    }
}

impl Command for BLMove {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("BLMOVE"),
            bulk(self.source.as_str()),
            bulk(self.destination.as_str()),
            bulk(self.wherefrom.as_str()),
            bulk(self.whereto.as_str()),
            bulk(self.timeout.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(data)),
            Frame::Null | Frame::BulkString(None) | Frame::Array(None) => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "BLMOVE"
    }
}

/// BZPOPMIN key \[key ...\] timeout
///
/// Blocking pop of the member with the lowest score from sorted sets.
pub struct BZPopMin {
    keys: Vec<String>,
    timeout: f64,
}

impl BZPopMin {
    pub fn new(key: impl Into<String>, timeout: f64) -> Self {
        Self {
            keys: vec![key.into()],
            timeout,
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>, timeout: f64) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            timeout,
        }
    }
}

impl Command for BZPopMin {
    type Response = Option<(Bytes, Bytes, f64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("BZPOPMIN")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        args.push(bulk(self.timeout.to_string()));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_key_member_score_or_null(frame)
    }

    fn name(&self) -> &str {
        "BZPOPMIN"
    }
}

/// BZPOPMAX key \[key ...\] timeout
///
/// Blocking pop of the member with the highest score from sorted sets.
pub struct BZPopMax {
    keys: Vec<String>,
    timeout: f64,
}

impl BZPopMax {
    pub fn new(key: impl Into<String>, timeout: f64) -> Self {
        Self {
            keys: vec![key.into()],
            timeout,
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>, timeout: f64) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            timeout,
        }
    }
}

impl Command for BZPopMax {
    type Response = Option<(Bytes, Bytes, f64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("BZPOPMAX")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        args.push(bulk(self.timeout.to_string()));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_key_member_score_or_null(frame)
    }

    fn name(&self) -> &str {
        "BZPOPMAX"
    }
}

// -- Response parsing --

/// Parse [key, value] or null (for BLPOP/BRPOP).
fn parse_key_value_or_null(frame: Frame) -> Result<Option<(Bytes, Bytes)>, RedisError> {
    match frame {
        Frame::Null | Frame::Array(None) | Frame::BulkString(None) => Ok(None),
        Frame::Array(Some(items)) if items.len() == 2 => {
            let key = extract_bytes(&items[0])?;
            let value = extract_bytes(&items[1])?;
            Ok(Some((key, value)))
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "two-element array or null",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse [key, member, score] or null (for BZPOPMIN/BZPOPMAX).
fn parse_key_member_score_or_null(frame: Frame) -> Result<Option<(Bytes, Bytes, f64)>, RedisError> {
    match frame {
        Frame::Null | Frame::Array(None) | Frame::BulkString(None) => Ok(None),
        Frame::Array(Some(items)) if items.len() == 3 => {
            let key = extract_bytes(&items[0])?;
            let member = extract_bytes(&items[1])?;
            let score = match &items[2] {
                Frame::BulkString(Some(b)) => {
                    let s = std::str::from_utf8(b).map_err(|e| RedisError::UnexpectedResponse {
                        expected: "valid UTF-8 score",
                        actual: e.to_string(),
                    })?;
                    s.parse::<f64>()
                        .map_err(|e| RedisError::UnexpectedResponse {
                            expected: "valid f64 score",
                            actual: e.to_string(),
                        })?
                }
                Frame::Double(d) => *d,
                other => {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "bulk string or double score",
                        actual: format!("{other:?}"),
                    });
                }
            };
            Ok(Some((key, member, score)))
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "three-element array or null",
            actual: format!("{other:?}"),
        }),
    }
}

fn extract_bytes(frame: &Frame) -> Result<Bytes, RedisError> {
    match frame {
        Frame::BulkString(Some(b)) => Ok(b.clone()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "bulk string",
            actual: format!("{other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    // -- BLPop --

    #[test]
    fn blpop_single_key_to_frame() {
        let cmd = BLPop::new("mylist", 5.0);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("BLPOP"), bulk("mylist"), bulk("5")])
        );
    }

    #[test]
    fn blpop_multiple_keys_to_frame() {
        let cmd = BLPop::keys(vec!["list1", "list2"], 0.0);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("BLPOP"), bulk("list1"), bulk("list2"), bulk("0")])
        );
    }

    #[test]
    fn blpop_parse_key_value_array() {
        let cmd = BLPop::new("mylist", 5.0);
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("mylist"))),
            Frame::BulkString(Some(Bytes::from("value1"))),
        ]));
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, Some((Bytes::from("mylist"), Bytes::from("value1"))));
    }

    #[test]
    fn blpop_parse_null_on_timeout() {
        let cmd = BLPop::new("mylist", 1.0);
        let result = cmd.parse_response(Frame::Null).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn blpop_parse_null_array() {
        let cmd = BLPop::new("mylist", 1.0);
        let result = cmd.parse_response(Frame::Array(None)).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn blpop_parse_error_on_wrong_array_size() {
        let cmd = BLPop::new("mylist", 1.0);
        let frame = Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from("only_one")))]));
        assert!(cmd.parse_response(frame).is_err());
    }

    // -- BRPop --

    #[test]
    fn brpop_to_frame() {
        let cmd = BRPop::new("mylist", 10.0);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("BRPOP"), bulk("mylist"), bulk("10")])
        );
    }

    #[test]
    fn brpop_parse_key_value_array() {
        let cmd = BRPop::new("mylist", 5.0);
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("mylist"))),
            Frame::BulkString(Some(Bytes::from("tail"))),
        ]));
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, Some((Bytes::from("mylist"), Bytes::from("tail"))));
    }

    #[test]
    fn brpop_parse_null_on_timeout() {
        let cmd = BRPop::new("mylist", 1.0);
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    // -- BLMove --

    #[test]
    fn blmove_to_frame() {
        let cmd = BLMove::new("src", "dst", ListDir::Left, ListDir::Right, 3.0);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("BLMOVE"),
                bulk("src"),
                bulk("dst"),
                bulk("LEFT"),
                bulk("RIGHT"),
                bulk("3"),
            ])
        );
    }

    #[test]
    fn blmove_parse_value() {
        let cmd = BLMove::new("src", "dst", ListDir::Left, ListDir::Right, 1.0);
        let frame = Frame::BulkString(Some(Bytes::from("moved")));
        assert_eq!(
            cmd.parse_response(frame).unwrap(),
            Some(Bytes::from("moved"))
        );
    }

    #[test]
    fn blmove_parse_null_on_timeout() {
        let cmd = BLMove::new("src", "dst", ListDir::Left, ListDir::Right, 1.0);
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    // -- BZPopMin --

    #[test]
    fn bzpopmin_to_frame() {
        let cmd = BZPopMin::new("myzset", 0.0);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("BZPOPMIN"), bulk("myzset"), bulk("0")])
        );
    }

    #[test]
    fn bzpopmin_parse_key_member_score() {
        let cmd = BZPopMin::new("myzset", 0.0);
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("myzset"))),
            Frame::BulkString(Some(Bytes::from("member1"))),
            Frame::BulkString(Some(Bytes::from("1.5"))),
        ]));
        let result = cmd.parse_response(frame).unwrap().unwrap();
        assert_eq!(result.0, Bytes::from("myzset"));
        assert_eq!(result.1, Bytes::from("member1"));
        assert!((result.2 - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn bzpopmin_parse_null_on_timeout() {
        let cmd = BZPopMin::new("myzset", 1.0);
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    #[test]
    fn bzpopmin_parse_double_score() {
        let cmd = BZPopMin::new("myzset", 0.0);
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("myzset"))),
            Frame::BulkString(Some(Bytes::from("member1"))),
            Frame::Double(2.5),
        ]));
        let result = cmd.parse_response(frame).unwrap().unwrap();
        assert!((result.2 - 2.5).abs() < f64::EPSILON);
    }

    // -- BZPopMax --

    #[test]
    fn bzpopmax_to_frame() {
        let cmd = BZPopMax::keys(vec!["zs1", "zs2"], 5.0);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("BZPOPMAX"), bulk("zs1"), bulk("zs2"), bulk("5"),])
        );
    }

    #[test]
    fn bzpopmax_parse_null_on_timeout() {
        let cmd = BZPopMax::new("myzset", 1.0);
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }
}
