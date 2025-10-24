//! Redis Set commands
//!
//! Sets are unordered collections of unique strings. These commands provide
//! type-safe operations for adding, removing, and querying set members,
//! as well as set operations like intersection, union, and difference.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// SADD command - add members to a set
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Sadd;
///
/// // Add single member
/// let cmd = Sadd::new("myset", b"member1".to_vec());
/// // Response: 1 (number of members added)
///
/// // Add multiple members
/// let cmd = Sadd::new("myset", b"member1".to_vec())
///     .member(b"member2".to_vec())
///     .member(b"member3".to_vec());
/// // Response: count of newly added members
/// ```
#[derive(Debug, Clone)]
pub struct Sadd {
    pub(crate) key: String,
    pub(crate) members: Vec<Bytes>,
}

impl Sadd {
    /// Create a new SADD command with the first member
    pub fn new(key: impl Into<String>, member: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            members: vec![member.into()],
        }
    }

    /// Add another member to the set
    pub fn member(mut self, member: impl Into<Bytes>) -> Self {
        self.members.push(member.into());
        self
    }

    /// Add multiple members to the set
    pub fn members<I, B>(mut self, members: I) -> Self
    where
        I: IntoIterator<Item = B>,
        B: Into<Bytes>,
    {
        self.members.extend(members.into_iter().map(Into::into));
        self
    }
}

impl Command for Sadd {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![
            Frame::BulkString(Some(Bytes::from("SADD"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ];

        for member in &self.members {
            parts.push(Frame::BulkString(Some(member.clone())));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(count) => Ok(count),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SREM command - remove members from a set
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Srem;
///
/// // Remove single member
/// let cmd = Srem::new("myset", b"member1".to_vec());
/// // Response: 1 if removed, 0 if member didn't exist
///
/// // Remove multiple members
/// let cmd = Srem::new("myset", b"member1".to_vec())
///     .member(b"member2".to_vec());
/// // Response: count of members actually removed
/// ```
#[derive(Debug, Clone)]
pub struct Srem {
    pub(crate) key: String,
    pub(crate) members: Vec<Bytes>,
}

impl Srem {
    /// Create a new SREM command with the first member
    pub fn new(key: impl Into<String>, member: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            members: vec![member.into()],
        }
    }

    /// Add another member to remove
    pub fn member(mut self, member: impl Into<Bytes>) -> Self {
        self.members.push(member.into());
        self
    }

    /// Add multiple members to remove
    pub fn members<I, B>(mut self, members: I) -> Self
    where
        I: IntoIterator<Item = B>,
        B: Into<Bytes>,
    {
        self.members.extend(members.into_iter().map(Into::into));
        self
    }
}

impl Command for Srem {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![
            Frame::BulkString(Some(Bytes::from("SREM"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ];

        for member in &self.members {
            parts.push(Frame::BulkString(Some(member.clone())));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(count) => Ok(count),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SMEMBERS command - get all members of a set
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Smembers;
///
/// let cmd = Smembers::new("myset");
/// // Response: Vec<Bytes> of all members (order not guaranteed)
/// ```
#[derive(Debug, Clone)]
pub struct Smembers {
    pub(crate) key: String,
}

impl Smembers {
    /// Create a new SMEMBERS command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Smembers {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SMEMBERS"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(elements) => {
                let mut members = Vec::with_capacity(elements.len());
                for element in elements {
                    match element {
                        Frame::BulkString(Some(data)) => members.push(data),
                        Frame::Error(e) => {
                            let err_str = String::from_utf8_lossy(&e).to_string();
                            return Err(RedisError::Redis(err_str));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(members)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SISMEMBER command - check if a value is a member of a set
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Sismember;
///
/// let cmd = Sismember::new("myset", b"member1".to_vec());
/// // Response: true if member exists, false otherwise
/// ```
#[derive(Debug, Clone)]
pub struct Sismember {
    pub(crate) key: String,
    pub(crate) member: Bytes,
}

impl Sismember {
    /// Create a new SISMEMBER command
    pub fn new(key: impl Into<String>, member: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            member: member.into(),
        }
    }
}

impl Command for Sismember {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SISMEMBER"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(self.member.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(result) => Ok(result != 0),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SCARD command - get the number of members in a set
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Scard;
///
/// let cmd = Scard::new("myset");
/// // Response: number of members in the set
/// ```
#[derive(Debug, Clone)]
pub struct Scard {
    pub(crate) key: String,
}

impl Scard {
    /// Create a new SCARD command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Scard {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SCARD"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
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

/// SINTER command - compute the intersection of multiple sets
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Sinter;
///
/// let cmd = Sinter::new("set1")
///     .key("set2")
///     .key("set3");
/// // Response: Vec<Bytes> of members present in all sets
/// ```
#[derive(Debug, Clone)]
pub struct Sinter {
    pub(crate) keys: Vec<String>,
}

impl Sinter {
    /// Create a new SINTER command with the first key
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    /// Add another key to the intersection
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Add multiple keys to the intersection
    pub fn keys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.keys.extend(keys.into_iter().map(Into::into));
        self
    }
}

impl Command for Sinter {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![Frame::BulkString(Some(Bytes::from("SINTER")))];

        for key in &self.keys {
            parts.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(elements) => {
                let mut members = Vec::with_capacity(elements.len());
                for element in elements {
                    match element {
                        Frame::BulkString(Some(data)) => members.push(data),
                        Frame::Error(e) => {
                            let err_str = String::from_utf8_lossy(&e).to_string();
                            return Err(RedisError::Redis(err_str));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(members)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SUNION command - compute the union of multiple sets
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Sunion;
///
/// let cmd = Sunion::new("set1")
///     .key("set2")
///     .key("set3");
/// // Response: Vec<Bytes> of all unique members across all sets
/// ```
#[derive(Debug, Clone)]
pub struct Sunion {
    pub(crate) keys: Vec<String>,
}

impl Sunion {
    /// Create a new SUNION command with the first key
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    /// Add another key to the union
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Add multiple keys to the union
    pub fn keys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.keys.extend(keys.into_iter().map(Into::into));
        self
    }
}

impl Command for Sunion {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![Frame::BulkString(Some(Bytes::from("SUNION")))];

        for key in &self.keys {
            parts.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(elements) => {
                let mut members = Vec::with_capacity(elements.len());
                for element in elements {
                    match element {
                        Frame::BulkString(Some(data)) => members.push(data),
                        Frame::Error(e) => {
                            let err_str = String::from_utf8_lossy(&e).to_string();
                            return Err(RedisError::Redis(err_str));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(members)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SDIFF command - compute the difference of multiple sets
///
/// Returns members in the first set that don't exist in any of the other sets.
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Sdiff;
///
/// let cmd = Sdiff::new("set1")
///     .key("set2")
///     .key("set3");
/// // Response: Vec<Bytes> of members in set1 but not in set2 or set3
/// ```
#[derive(Debug, Clone)]
pub struct Sdiff {
    pub(crate) keys: Vec<String>,
}

impl Sdiff {
    /// Create a new SDIFF command with the first key
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    /// Add another key to subtract from the first set
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Add multiple keys to subtract from the first set
    pub fn keys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.keys.extend(keys.into_iter().map(Into::into));
        self
    }
}

impl Command for Sdiff {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![Frame::BulkString(Some(Bytes::from("SDIFF")))];

        for key in &self.keys {
            parts.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(elements) => {
                let mut members = Vec::with_capacity(elements.len());
                for element in elements {
                    match element {
                        Frame::BulkString(Some(data)) => members.push(data),
                        Frame::Error(e) => {
                            let err_str = String::from_utf8_lossy(&e).to_string();
                            return Err(RedisError::Redis(err_str));
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(members)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SSCAN command - incrementally iterate set members
///
/// # Example
/// ```no_run
/// use redis_tower::commands::Sscan;
///
/// // Basic scan
/// let cmd = Sscan::new("myset", 0);
/// // Response: SscanResult with cursor and members
///
/// // Scan with pattern and count
/// let cmd = Sscan::new("myset", 0)
///     .pattern("prefix:*")
///     .count(100);
/// ```
#[derive(Debug, Clone)]
pub struct Sscan {
    pub(crate) key: String,
    pub(crate) cursor: u64,
    pub(crate) pattern: Option<String>,
    pub(crate) count: Option<usize>,
}

impl Sscan {
    /// Create a new SSCAN command
    pub fn new(key: impl Into<String>, cursor: u64) -> Self {
        Self {
            key: key.into(),
            cursor,
            pattern: None,
            count: None,
        }
    }

    /// Add a MATCH pattern filter
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Add a COUNT hint for the number of elements to return
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }
}

/// Result type for SSCAN command
#[derive(Debug, Clone)]
pub struct SscanResult {
    /// Next cursor position (0 means iteration complete)
    pub cursor: u64,
    /// Members returned in this iteration
    pub members: Vec<Bytes>,
}

impl Command for Sscan {
    type Response = SscanResult;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![
            Frame::BulkString(Some(Bytes::from("SSCAN"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(Bytes::from(self.cursor.to_string()))),
        ];

        if let Some(pattern) = &self.pattern {
            parts.push(Frame::BulkString(Some(Bytes::from("MATCH"))));
            parts.push(Frame::BulkString(Some(Bytes::from(pattern.clone()))));
        }

        if let Some(count) = self.count {
            parts.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            parts.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(mut elements) => {
                if elements.len() != 2 {
                    return Err(RedisError::UnexpectedResponse);
                }

                // Parse cursor
                let cursor = match elements.remove(0) {
                    Frame::BulkString(Some(data)) => {
                        let cursor_str = String::from_utf8_lossy(&data);
                        cursor_str
                            .parse::<u64>()
                            .map_err(|_| RedisError::UnexpectedResponse)?
                    }
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                // Parse members array
                let members = match elements.remove(0) {
                    Frame::Array(member_frames) => {
                        let mut members = Vec::with_capacity(member_frames.len());
                        for member_frame in member_frames {
                            match member_frame {
                                Frame::BulkString(Some(data)) => members.push(data),
                                _ => return Err(RedisError::UnexpectedResponse),
                            }
                        }
                        members
                    }
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                Ok(SscanResult { cursor, members })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sadd_frame() {
        let cmd = Sadd::new("myset", b"member1".to_vec()).member(b"member2".to_vec());
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4); // SADD, key, member1, member2
            }
            _ => panic!("Expected array frame"),
        }
    }

    #[test]
    fn test_sinter_frame() {
        let cmd = Sinter::new("set1").key("set2").key("set3");
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4); // SINTER, set1, set2, set3
            }
            _ => panic!("Expected array frame"),
        }
    }

    #[test]
    fn test_sscan_frame() {
        let cmd = Sscan::new("myset", 0).pattern("prefix:*").count(100);
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 7); // SSCAN, key, cursor, MATCH, pattern, COUNT, count
            }
            _ => panic!("Expected array frame"),
        }
    }
}

/// SPOP command - remove and return random member(s)
#[derive(Debug, Clone)]
pub struct SPop {
    pub(crate) key: String,
    pub(crate) count: Option<i64>,
}

impl SPop {
    /// Create a new SPOP command for single member
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    /// Pop multiple members
    pub fn count(key: impl Into<String>, count: i64) -> Self {
        Self {
            key: key.into(),
            count: Some(count),
        }
    }
}

impl Command for SPop {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("SPOP"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // Single element (no count specified)
            Frame::BulkString(Some(data)) => Ok(vec![data]),
            Frame::BulkString(None) | Frame::Null => Ok(vec![]),
            // Multiple elements (count specified)
            Frame::Array(elements) => {
                let mut members = Vec::with_capacity(elements.len());
                for element in elements {
                    match element {
                        Frame::BulkString(Some(data)) => members.push(data),
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(members)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SRANDMEMBER command - get random member(s) without removing
#[derive(Debug, Clone)]
pub struct SRandMember {
    pub(crate) key: String,
    pub(crate) count: Option<i64>,
}

impl SRandMember {
    /// Create a new SRANDMEMBER command for single member
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    /// Get multiple members (may include duplicates if count is negative)
    pub fn count(key: impl Into<String>, count: i64) -> Self {
        Self {
            key: key.into(),
            count: Some(count),
        }
    }
}

impl Command for SRandMember {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("SRANDMEMBER"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // Single element (no count specified)
            Frame::BulkString(Some(data)) => Ok(vec![data]),
            Frame::BulkString(None) | Frame::Null => Ok(vec![]),
            // Multiple elements (count specified)
            Frame::Array(elements) => {
                let mut members = Vec::with_capacity(elements.len());
                for element in elements {
                    match element {
                        Frame::BulkString(Some(data)) => members.push(data),
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(members)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SMOVE command - move member from one set to another
#[derive(Debug, Clone)]
pub struct SMove {
    pub(crate) source: String,
    pub(crate) destination: String,
    pub(crate) member: Bytes,
}

impl SMove {
    /// Create a new SMOVE command
    pub fn new(
        source: impl Into<String>,
        destination: impl Into<String>,
        member: impl Into<Bytes>,
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
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SMOVE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.source.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.destination.as_bytes()))),
            Frame::BulkString(Some(self.member.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n == 1),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SINTERSTORE command - store intersection in destination
#[derive(Debug, Clone)]
pub struct SInterStore {
    pub(crate) destination: String,
    pub(crate) keys: Vec<String>,
}

impl SInterStore {
    /// Create a new SINTERSTORE command
    pub fn new(destination: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            destination: destination.into(),
            keys,
        }
    }
}

impl Command for SInterStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("SINTERSTORE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.destination.as_bytes()))),
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

// Read-only trait implementations for cluster read-from-replica support
use crate::cluster::read_preference::ReadOnly;

impl ReadOnly for Smembers {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Sismember {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Scard {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Sinter {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Sunion {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Sdiff {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for Sscan {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for SRandMember {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for SMIsMember {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for SInterCard {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands - explicitly implement with default (false) for clarity
impl ReadOnly for Sadd {}
impl ReadOnly for Srem {}
impl ReadOnly for SPop {}
impl ReadOnly for SMove {}
impl ReadOnly for SInterStore {}
impl ReadOnly for SUnionStore {}
impl ReadOnly for SDiffStore {}

/// SUNIONSTORE command - store union in destination
#[derive(Debug, Clone)]
pub struct SUnionStore {
    pub(crate) destination: String,
    pub(crate) keys: Vec<String>,
}

impl SUnionStore {
    /// Create a new SUNIONSTORE command
    pub fn new(destination: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            destination: destination.into(),
            keys,
        }
    }
}

impl Command for SUnionStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("SUNIONSTORE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.destination.as_bytes()))),
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

/// SDIFFSTORE command - store difference in destination
#[derive(Debug, Clone)]
pub struct SDiffStore {
    pub(crate) destination: String,
    pub(crate) keys: Vec<String>,
}

impl SDiffStore {
    /// Create a new SDIFFSTORE command
    pub fn new(destination: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            destination: destination.into(),
            keys,
        }
    }
}

impl Command for SDiffStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("SDIFFSTORE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.destination.as_bytes()))),
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

/// SMISMEMBER command - check multiple members for existence
#[derive(Debug, Clone)]
pub struct SMIsMember {
    pub(crate) key: String,
    pub(crate) members: Vec<Bytes>,
}

impl SMIsMember {
    /// Create a new SMISMEMBER command
    pub fn new(key: impl Into<String>, members: Vec<Bytes>) -> Self {
        Self {
            key: key.into(),
            members,
        }
    }
}

impl Command for SMIsMember {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("SMISMEMBER"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];
        for member in &self.members {
            frames.push(Frame::BulkString(Some(member.clone())));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(elements) => {
                let mut results = Vec::with_capacity(elements.len());
                for element in elements {
                    match element {
                        Frame::Integer(n) => results.push(n == 1),
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(results)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SINTERCARD command - get cardinality of intersection
#[derive(Debug, Clone)]
pub struct SInterCard {
    pub(crate) keys: Vec<String>,
    pub(crate) limit: Option<i64>,
}

impl SInterCard {
    /// Create a new SINTERCARD command
    pub fn new(keys: Vec<String>) -> Self {
        Self { keys, limit: None }
    }

    /// Set limit for early termination
    pub fn limit(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl Command for SInterCard {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("SINTERCARD"))),
            Frame::BulkString(Some(Bytes::from(self.keys.len().to_string()))),
        ];
        for key in &self.keys {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }
        if let Some(limit) = self.limit {
            frames.push(Frame::BulkString(Some(Bytes::from("LIMIT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(limit.to_string()))));
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
