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

/// SRANDMEMBER key \[count\]
///
/// Returns one or more random members from the set stored at `key`. When called
/// without `count`, returns a single member. When `count` is provided, returns
/// up to that many members. A negative count allows duplicates.
pub struct SRandMember {
    key: String,
    count: Option<i64>,
}

impl SRandMember {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for SRandMember {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SRANDMEMBER"), bulk(self.key.as_str())];
        if let Some(count) = self.count {
            args.push(bulk(count.to_string().as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // When called without count, Redis returns a single bulk string.
            Frame::BulkString(Some(data)) => Ok(vec![data]),
            Frame::BulkString(None) => Ok(vec![]),
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
                expected: "bulk string or array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SRANDMEMBER"
    }
}

/// SPOP key \[count\]
///
/// Removes and returns one or more random members from the set stored at `key`.
/// Without `count`, removes and returns a single member. With `count`, removes
/// and returns up to that many members.
pub struct SPop {
    key: String,
    count: Option<u64>,
}

impl SPop {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    pub fn count(mut self, count: u64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for SPop {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SPOP"), bulk(self.key.as_str())];
        if let Some(count) = self.count {
            args.push(bulk(count.to_string().as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // When called without count, Redis returns a single bulk string.
            Frame::BulkString(Some(data)) => Ok(vec![data]),
            Frame::BulkString(None) => Ok(vec![]),
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
                expected: "bulk string or array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SPOP"
    }
}

/// SDIFF key \[key ...\]
///
/// Returns the members of the set resulting from the difference between the
/// first set and all the successive sets.
pub struct SDiff {
    keys: Vec<String>,
}

impl SDiff {
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

impl Command for SDiff {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SDIFF")];
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
        "SDIFF"
    }
}

/// SDIFFSTORE destination key \[key ...\]
///
/// Stores the members of the set resulting from the difference between the
/// first set and all the successive sets into `destination`. Returns the number
/// of elements in the resulting set.
pub struct SDiffStore {
    destination: String,
    keys: Vec<String>,
}

impl SDiffStore {
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

impl Command for SDiffStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SDIFFSTORE"), bulk(self.destination.as_str())];
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
        "SDIFFSTORE"
    }
}

/// SUNION key \[key ...\]
///
/// Returns the members of the set resulting from the union of all the given
/// sets.
pub struct SUnion {
    keys: Vec<String>,
}

impl SUnion {
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

impl Command for SUnion {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SUNION")];
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
        "SUNION"
    }
}

/// SUNIONSTORE destination key \[key ...\]
///
/// Stores the members of the set resulting from the union of all the given
/// sets into `destination`. Returns the number of elements in the resulting set.
pub struct SUnionStore {
    destination: String,
    keys: Vec<String>,
}

impl SUnionStore {
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

impl Command for SUnionStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SUNIONSTORE"), bulk(self.destination.as_str())];
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
        "SUNIONSTORE"
    }
}

/// SMOVE source destination member
///
/// Moves `member` from the set at `source` to the set at `destination`.
/// Returns `true` if the member was moved, `false` if it was not a member of
/// the source set.
pub struct SMove {
    source: String,
    destination: String,
    member: String,
}

impl SMove {
    pub fn new(
        source: impl Into<String>,
        destination: impl Into<String>,
        member: impl Into<String>,
    ) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
            member: member.into(),
        }
    }
}

impl Command for SMove {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("SMOVE"),
            bulk(self.source.as_str()),
            bulk(self.destination.as_str()),
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
        "SMOVE"
    }
}

/// SMISMEMBER key member \[member ...\]
///
/// Returns whether each member is a member of the set stored at `key`. For
/// each member, returns `true` if the member exists, `false` otherwise.
pub struct SMisMember {
    key: String,
    members: Vec<String>,
}

impl SMisMember {
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

impl Command for SMisMember {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SMISMEMBER"), bulk(self.key.as_str())];
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
                    Frame::Integer(n) => Ok(n == 1),
                    Frame::Boolean(b) => Ok(b),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "integer or boolean",
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
        "SMISMEMBER"
    }
}

/// SINTERSTORE destination key \[key ...\]
///
/// Stores the members of the set resulting from the intersection of all the
/// given sets into `destination`. Returns the number of elements in the
/// resulting set.
pub struct SInterStore {
    destination: String,
    keys: Vec<String>,
}

impl SInterStore {
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

impl Command for SInterStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SINTERSTORE"), bulk(self.destination.as_str())];
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
        "SINTERSTORE"
    }
}

/// SINTERCARD numkeys key \[key ...\] \[LIMIT limit\]
///
/// Returns the cardinality of the intersection of the given sets, without
/// actually computing the full intersection. An optional `LIMIT` caps the
/// work done when the cardinality reaches the specified value.
pub struct SInterCard {
    keys: Vec<String>,
    limit: Option<u64>,
}

impl SInterCard {
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            limit: None,
        }
    }

    /// Set the LIMIT option to cap computation early.
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl Command for SInterCard {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SINTERCARD"), bulk(self.keys.len().to_string())];
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
        "SINTERCARD"
    }
}
