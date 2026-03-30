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
            Frame::Null | Frame::BulkString(None) => Ok(None),
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
