use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// SADD key member \[member ...\]
///
/// Adds the specified members to the set stored at `key`. Returns the number
/// of members that were added (excluding members already present).
pub struct SAdd {
    key: String,
    members: Vec<String>,
}

impl SAdd {
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

impl Command for SAdd {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SADD"), bulk(self.key.as_str())];
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
        "SADD"
    }
}

/// SREM key member \[member ...\]
///
/// Removes the specified members from the set stored at `key`. Returns the
/// number of members that were removed.
pub struct SRem {
    key: String,
    members: Vec<String>,
}

impl SRem {
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

impl Command for SRem {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SREM"), bulk(self.key.as_str())];
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
        "SREM"
    }
}

/// SMEMBERS key
///
/// Returns all the members of the set stored at `key`.
pub struct SMembers {
    key: String,
}

impl SMembers {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for SMembers {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("SMEMBERS"), bulk(self.key.as_str())])
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
            Frame::Set(items) => items
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
                expected: "array or set",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SMEMBERS"
    }
}

/// SISMEMBER key member
///
/// Returns whether `member` is a member of the set stored at `key`.
pub struct SIsMember {
    key: String,
    member: String,
}

impl SIsMember {
    pub fn new(key: impl Into<String>, member: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            member: member.into(),
        }
    }
}

impl Command for SIsMember {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("SISMEMBER"),
            bulk(self.key.as_str()),
            bulk(self.member.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n == 1),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SISMEMBER"
    }
}

/// SCARD key
///
/// Returns the number of members in the set stored at `key`.
pub struct SCard {
    key: String,
}

impl SCard {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for SCard {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("SCARD"), bulk(self.key.as_str())])
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
        "SCARD"
    }
}

/// SINTER key \[key ...\]
///
/// Returns the members of the set resulting from the intersection of all
/// the given sets.
pub struct SInter {
    keys: Vec<String>,
}

impl SInter {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for SInter {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SINTER")];
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
                    Frame::BulkString(Some(data)) => Ok(data),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string",
                        actual: format!("{other:?}"),
                    }),
                })
                .collect(),
            Frame::Set(items) => items
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
                expected: "array or set",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SINTER"
    }
}
