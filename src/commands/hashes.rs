//! Hash commands (HGET, HSET, HDEL, etc.)

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;
use std::collections::HashMap;

/// HGET command - get a field from a hash
#[derive(Debug, Clone)]
pub struct HGet {
    pub(crate) key: String,
    pub(crate) field: String,
}

impl HGet {
    /// Create a new HGET command
    pub fn new(key: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
        }
    }
}

impl Command for HGet {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("HGET"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.field.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(data)),
            Frame::BulkString(None) | Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hget_frame() {
        let cmd = HGet::new("myhash", "field1");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HGET"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myhash"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("field1"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hset_frame() {
        let cmd = HSet::new("myhash", "field1", Bytes::from("value1"));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HSET"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myhash"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("field1"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("value1"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hsetnx_frame() {
        let cmd = HSetNx::new("myhash", "field1", Bytes::from("value1"));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HSETNX"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hrandfield_basic() {
        let cmd = HRandField::new("myhash");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HRANDFIELD"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myhash"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hrandfield_with_count() {
        let cmd = HRandField::new("myhash").count(3);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("3"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hrandfield_with_values() {
        let cmd = HRandField::new("myhash").count(2).with_values();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("WITHVALUES"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hexists_frame() {
        let cmd = HExists::new("myhash", "field1");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HEXISTS"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hlen_frame() {
        let cmd = HLen::new("myhash");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HLEN"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hkeys_frame() {
        let cmd = HKeys::new("myhash");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HKEYS"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hvals_frame() {
        let cmd = HVals::new("myhash");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HVALS"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hmget_frame() {
        let cmd = HMGet::new(
            "myhash",
            vec![
                "field1".to_string(),
                "field2".to_string(),
                "field3".to_string(),
            ],
        );
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HMGET"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myhash"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("field1"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("field2"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("field3"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hincrby_frame() {
        let cmd = HIncrBy::new("myhash", "counter", 5);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HINCRBY"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("5"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hincrbyfloat_frame() {
        let cmd = HIncrByFloat::new("myhash", "score", 2.5);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("HINCRBYFLOAT")))
                );
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("2.5"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hstrlen_frame() {
        let cmd = HStrLen::new("myhash", "field1");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HSTRLEN"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hgetall_frame() {
        let cmd = HGetAll::new("myhash");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HGETALL"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hdel_frame() {
        let cmd = HDel::new("myhash", vec!["field1".to_string(), "field2".to_string()]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HDEL"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("field1"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("field2"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hexpire_frame() {
        let cmd = HExpire::new("myhash", 60, vec!["field1".to_string()]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HEXPIRE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myhash"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("60"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hexpireat_frame() {
        let cmd = HExpireAt::new("myhash", 1234567890, vec!["field1".to_string()]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HEXPIREAT"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hexpiretime_frame() {
        let cmd = HExpireTime::new("myhash", vec!["field1".to_string()]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("HEXPIRETIME")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hpexpire_frame() {
        let cmd = HPExpire::new("myhash", 60000, vec!["field1".to_string()]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HPEXPIRE"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("60000"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hpexpireat_frame() {
        let cmd = HPExpireAt::new("myhash", 1234567890000, vec!["field1".to_string()]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HPEXPIREAT"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hpexpiretime_frame() {
        let cmd = HPExpireTime::new("myhash", vec!["field1".to_string()]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("HPEXPIRETIME")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hpersist_frame() {
        let cmd = HPersist::new("myhash", vec!["field1".to_string(), "field2".to_string()]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HPERSIST"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myhash"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("FIELDS"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("2"))));
                assert_eq!(parts.len(), 6); // HPERSIST key FIELDS 2 field1 field2
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hpttl_frame() {
        let cmd = HPTtl::new("myhash", vec!["field1".to_string()]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HPTTL"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_httl_frame() {
        let cmd = HTtl::new("myhash", vec!["field1".to_string()]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HTTL"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hmset_frame() {
        let fields = vec![
            ("field1".to_string(), Bytes::from("value1")),
            ("field2".to_string(), Bytes::from("value2")),
        ];

        let cmd = HMSet::new("myhash", fields);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HMSET"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myhash"))));
                assert!(parts.len() >= 6); // HMSET key field1 value1 field2 value2
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hsetex_frame() {
        let cmd = HSetEx::new("myhash", 60, "field1", Bytes::from("value1"));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HSETEX"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myhash"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("60"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hgetdel_frame() {
        let cmd = HGetDel::new("myhash", "field1");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HGETDEL"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("myhash"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("field1"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hgetex_basic() {
        let cmd = HGetEx::new("myhash", "field1");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("HGETEX"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_hgetex_with_expiration() {
        let cmd = HGetEx::new("myhash", "field1").ex(60);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("EX"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("60"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }
}

/// HSET command - set a field in a hash
#[derive(Debug, Clone)]
pub struct HSet {
    pub(crate) key: String,
    pub(crate) field: String,
    pub(crate) value: Bytes,
}

impl HSet {
    /// Create a new HSET command
    pub fn new(key: impl Into<String>, field: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
            value: value.into(),
        }
    }
}

impl Command for HSet {
    type Response = i64; // Returns 1 if new field, 0 if updated existing

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("HSET"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.field.as_bytes()))),
            Frame::BulkString(Some(self.value.clone())),
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

/// HSETNX command - Set hash field only if it doesn't exist
///
/// Sets field in the hash to value, only if field does not yet exist.
/// Returns true if field was set, false if it already existed.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::HSetNx;
///
/// let cmd = HSetNx::new("myhash", "field1", b"value1");
/// ```
#[derive(Debug, Clone)]
pub struct HSetNx {
    key: String,
    field: String,
    value: Bytes,
}

impl HSetNx {
    /// Create a new HSETNX command
    pub fn new(key: impl Into<String>, field: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
            value: value.into(),
        }
    }
}

impl Command for HSetNx {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("HSETNX"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.field.as_bytes()))),
            Frame::BulkString(Some(self.value.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),
            Frame::Integer(0) => Ok(false),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// HRANDFIELD command - Get random field(s) from a hash
///
/// Returns one or more random fields from the hash.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::HRandField;
///
/// // Get one random field
/// let cmd = HRandField::new("myhash");
///
/// // Get 3 random fields
/// let cmd = HRandField::new("myhash").count(3);
///
/// // Get 3 random fields with values
/// let cmd = HRandField::new("myhash").count(3).with_values();
/// ```
#[derive(Debug, Clone)]
pub struct HRandField {
    key: String,
    count: Option<i64>,
    with_values: bool,
}

impl HRandField {
    /// Create a new HRANDFIELD command (returns single field)
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
            with_values: false,
        }
    }

    /// Specify number of fields to return
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Return fields with their values
    pub fn with_values(mut self) -> Self {
        self.with_values = true;
        self
    }
}

impl Command for HRandField {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HRANDFIELD"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        if self.with_values {
            frames.push(Frame::BulkString(Some(Bytes::from("WITHVALUES"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                // Single field without count
                Ok(vec![String::from_utf8_lossy(&data).into_owned()])
            }
            Frame::Array(items) => {
                let mut fields = Vec::new();
                for item in items {
                    if let Frame::BulkString(Some(data)) = item {
                        fields.push(String::from_utf8_lossy(&data).into_owned());
                    }
                }
                Ok(fields)
            }
            Frame::Null => Ok(vec![]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// Read-only trait implementations for cluster read-from-replica support
use crate::read_preference::ReadOnly;

impl ReadOnly for HGet {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for HExists {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for HLen {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for HKeys {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for HVals {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for HMGet {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for HStrLen {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for HGetAll {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for HRandField {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands - explicitly implement with default (false) for clarity
impl ReadOnly for HSet {}
impl ReadOnly for HDel {}
impl ReadOnly for HIncrBy {}
impl ReadOnly for HIncrByFloat {}
impl ReadOnly for HSetNx {}

/// HEXISTS command - check if field exists
#[derive(Debug, Clone)]
pub struct HExists {
    pub(crate) key: String,
    pub(crate) field: String,
}

impl HExists {
    /// Create a new HEXISTS command
    pub fn new(key: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
        }
    }
}

impl Command for HExists {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("HEXISTS"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.field.as_bytes()))),
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

/// HLEN command - get number of fields
#[derive(Debug, Clone)]
pub struct HLen {
    pub(crate) key: String,
}

impl HLen {
    /// Create a new HLEN command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for HLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("HLEN"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
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

/// HKEYS command - get all field names
#[derive(Debug, Clone)]
pub struct HKeys {
    pub(crate) key: String,
}

impl HKeys {
    /// Create a new HKEYS command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for HKeys {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("HKEYS"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(elements) => {
                let mut keys = Vec::with_capacity(elements.len());
                for element in elements {
                    match element {
                        Frame::BulkString(Some(data)) => {
                            keys.push(String::from_utf8_lossy(&data).to_string());
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(keys)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// HVALS command - get all values
#[derive(Debug, Clone)]
pub struct HVals {
    pub(crate) key: String,
}

impl HVals {
    /// Create a new HVALS command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for HVals {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("HVALS"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(elements) => {
                let mut values = Vec::with_capacity(elements.len());
                for element in elements {
                    match element {
                        Frame::BulkString(Some(data)) => {
                            values.push(data);
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(values)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// HMGET command - get multiple field values
#[derive(Debug, Clone)]
pub struct HMGet {
    pub(crate) key: String,
    pub(crate) fields: Vec<String>,
}

impl HMGet {
    /// Create a new HMGET command
    pub fn new(key: impl Into<String>, fields: Vec<String>) -> Self {
        Self {
            key: key.into(),
            fields,
        }
    }

    /// Convenience method for getting a single field
    pub fn single(key: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            fields: vec![field.into()],
        }
    }
}

impl Command for HMGet {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HMGET"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];
        for field in &self.fields {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                field.as_bytes(),
            ))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(elements) => {
                let mut results = Vec::with_capacity(elements.len());
                for element in elements {
                    match element {
                        Frame::BulkString(Some(data)) => results.push(Some(data)),
                        Frame::BulkString(None) | Frame::Null => results.push(None),
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

/// HINCRBY command - increment hash field by integer
#[derive(Debug, Clone)]
pub struct HIncrBy {
    pub(crate) key: String,
    pub(crate) field: String,
    pub(crate) increment: i64,
}

impl HIncrBy {
    /// Create a new HINCRBY command
    pub fn new(key: impl Into<String>, field: impl Into<String>, increment: i64) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
            increment,
        }
    }
}

impl Command for HIncrBy {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("HINCRBY"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.field.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.increment.to_string()))),
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

/// HINCRBYFLOAT command - increment hash field by float
#[derive(Debug, Clone)]
pub struct HIncrByFloat {
    pub(crate) key: String,
    pub(crate) field: String,
    pub(crate) increment: f64,
}

impl HIncrByFloat {
    /// Create a new HINCRBYFLOAT command
    pub fn new(key: impl Into<String>, field: impl Into<String>, increment: f64) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
            increment,
        }
    }
}

impl Command for HIncrByFloat {
    type Response = f64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("HINCRBYFLOAT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.field.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.increment.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(&data);
                s.parse::<f64>()
                    .map_err(|_| RedisError::Protocol("Invalid float response".to_string()))
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// HSTRLEN command - get string length of field value
#[derive(Debug, Clone)]
pub struct HStrLen {
    pub(crate) key: String,
    pub(crate) field: String,
}

impl HStrLen {
    /// Create a new HSTRLEN command
    pub fn new(key: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
        }
    }
}

impl Command for HStrLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("HSTRLEN"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.field.as_bytes()))),
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

/// HGETALL command - get all fields and values from a hash
#[derive(Debug, Clone)]
pub struct HGetAll {
    pub(crate) key: String,
}

impl HGetAll {
    /// Create a new HGETALL command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for HGetAll {
    type Response = HashMap<String, Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("HGETALL"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(elements) => {
                if elements.len() % 2 != 0 {
                    return Err(RedisError::Protocol(
                        "HGETALL returned odd number of elements".to_string(),
                    ));
                }

                let mut map = HashMap::new();
                let mut iter = elements.into_iter();

                while let Some(key_frame) = iter.next() {
                    let value_frame = iter.next().unwrap(); // Safe because we checked length

                    match (key_frame, value_frame) {
                        (Frame::BulkString(Some(key_bytes)), Frame::BulkString(Some(value))) => {
                            let key = String::from_utf8_lossy(&key_bytes).to_string();
                            map.insert(key, value);
                        }
                        _ => {
                            return Err(RedisError::Protocol(
                                "HGETALL unexpected frame type".to_string(),
                            ));
                        }
                    }
                }

                Ok(map)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// HDEL command - delete fields from a hash
#[derive(Debug, Clone)]
pub struct HDel {
    pub(crate) key: String,
    pub(crate) fields: Vec<String>,
}

impl HDel {
    /// Create a new HDEL command
    pub fn new(key: impl Into<String>, fields: Vec<String>) -> Self {
        Self {
            key: key.into(),
            fields,
        }
    }

    /// Convenience method for deleting a single field
    pub fn single(key: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            fields: vec![field.into()],
        }
    }
}

impl Command for HDel {
    type Response = i64; // Number of fields removed

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HDEL"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];
        for field in &self.fields {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                field.as_bytes(),
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

/// HEXPIRE command - Set expiration for hash fields (Redis 7.4+)
///
/// Sets expiration time in seconds for one or more hash fields.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::HExpire;
///
/// let cmd = HExpire::new("myhash", 60, vec!["field1", "field2"]);
/// ```
#[derive(Debug, Clone)]
pub struct HExpire {
    key: String,
    seconds: i64,
    fields: Vec<String>,
}

impl HExpire {
    /// Create a new HEXPIRE command
    pub fn new(key: impl Into<String>, seconds: i64, fields: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            seconds,
            fields: fields.into_iter().map(|f| f.into()).collect(),
        }
    }
}

impl Command for HExpire {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HEXPIRE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.seconds.to_string()))),
            Frame::BulkString(Some(Bytes::from("FIELDS"))),
            Frame::BulkString(Some(Bytes::from(self.fields.len().to_string()))),
        ];
        for field in &self.fields {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                field.as_bytes(),
            ))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::Integer(n) => result.push(n),
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

/// HEXPIREAT command - Set expiration timestamp for hash fields (Redis 7.4+)
///
/// Sets expiration time as Unix timestamp in seconds for one or more hash fields.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::HExpireAt;
///
/// let cmd = HExpireAt::new("myhash", 1735689600, vec!["field1"]);
/// ```
#[derive(Debug, Clone)]
pub struct HExpireAt {
    key: String,
    timestamp: i64,
    fields: Vec<String>,
}

impl HExpireAt {
    /// Create a new HEXPIREAT command
    pub fn new(key: impl Into<String>, timestamp: i64, fields: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            timestamp,
            fields: fields.into_iter().map(|f| f.into()).collect(),
        }
    }
}

impl Command for HExpireAt {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HEXPIREAT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.timestamp.to_string()))),
            Frame::BulkString(Some(Bytes::from("FIELDS"))),
            Frame::BulkString(Some(Bytes::from(self.fields.len().to_string()))),
        ];
        for field in &self.fields {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                field.as_bytes(),
            ))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::Integer(n) => result.push(n),
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

/// HEXPIRETIME command - Get expiration timestamp for hash fields (Redis 7.4+)
///
/// Returns the absolute Unix timestamp in seconds at which the given field will expire.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::HExpireTime;
///
/// let cmd = HExpireTime::new("myhash", vec!["field1", "field2"]);
/// ```
#[derive(Debug, Clone)]
pub struct HExpireTime {
    key: String,
    fields: Vec<String>,
}

impl HExpireTime {
    /// Create a new HEXPIRETIME command
    pub fn new(key: impl Into<String>, fields: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            fields: fields.into_iter().map(|f| f.into()).collect(),
        }
    }
}

impl Command for HExpireTime {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HEXPIRETIME"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from("FIELDS"))),
            Frame::BulkString(Some(Bytes::from(self.fields.len().to_string()))),
        ];
        for field in &self.fields {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                field.as_bytes(),
            ))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::Integer(n) => result.push(n),
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

/// HPEXPIRE command - Set expiration for hash fields in milliseconds (Redis 7.4+)
///
/// Sets expiration time in milliseconds for one or more hash fields.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::HPExpire;
///
/// let cmd = HPExpire::new("myhash", 60000, vec!["field1"]);
/// ```
#[derive(Debug, Clone)]
pub struct HPExpire {
    key: String,
    milliseconds: i64,
    fields: Vec<String>,
}

impl HPExpire {
    /// Create a new HPEXPIRE command
    pub fn new(key: impl Into<String>, milliseconds: i64, fields: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            milliseconds,
            fields: fields.into_iter().map(|f| f.into()).collect(),
        }
    }
}

impl Command for HPExpire {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HPEXPIRE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.milliseconds.to_string()))),
            Frame::BulkString(Some(Bytes::from("FIELDS"))),
            Frame::BulkString(Some(Bytes::from(self.fields.len().to_string()))),
        ];
        for field in &self.fields {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                field.as_bytes(),
            ))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::Integer(n) => result.push(n),
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

/// HPEXPIREAT command - Set expiration timestamp in milliseconds for hash fields (Redis 7.4+)
///
/// Sets expiration time as Unix timestamp in milliseconds for one or more hash fields.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::HPExpireAt;
///
/// let cmd = HPExpireAt::new("myhash", 1735689600000, vec!["field1"]);
/// ```
#[derive(Debug, Clone)]
pub struct HPExpireAt {
    key: String,
    timestamp: i64,
    fields: Vec<String>,
}

impl HPExpireAt {
    /// Create a new HPEXPIREAT command
    pub fn new(key: impl Into<String>, timestamp: i64, fields: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            timestamp,
            fields: fields.into_iter().map(|f| f.into()).collect(),
        }
    }
}

impl Command for HPExpireAt {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HPEXPIREAT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.timestamp.to_string()))),
            Frame::BulkString(Some(Bytes::from("FIELDS"))),
            Frame::BulkString(Some(Bytes::from(self.fields.len().to_string()))),
        ];
        for field in &self.fields {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                field.as_bytes(),
            ))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::Integer(n) => result.push(n),
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

/// HPEXPIRETIME command - Get expiration timestamp in milliseconds for hash fields (Redis 7.4+)
///
/// Returns the absolute Unix timestamp in milliseconds at which the given field will expire.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::HPExpireTime;
///
/// let cmd = HPExpireTime::new("myhash", vec!["field1", "field2"]);
/// ```
#[derive(Debug, Clone)]
pub struct HPExpireTime {
    key: String,
    fields: Vec<String>,
}

impl HPExpireTime {
    /// Create a new HPEXPIRETIME command
    pub fn new(key: impl Into<String>, fields: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            fields: fields.into_iter().map(|f| f.into()).collect(),
        }
    }
}

impl Command for HPExpireTime {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HPEXPIRETIME"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from("FIELDS"))),
            Frame::BulkString(Some(Bytes::from(self.fields.len().to_string()))),
        ];
        for field in &self.fields {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                field.as_bytes(),
            ))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::Integer(n) => result.push(n),
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

/// HPERSIST command - Remove expiration from hash fields (Redis 7.4+)
///
/// Removes the expiration from one or more hash fields.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::HPersist;
///
/// let cmd = HPersist::new("myhash", vec!["field1", "field2"]);
/// ```
#[derive(Debug, Clone)]
pub struct HPersist {
    key: String,
    fields: Vec<String>,
}

impl HPersist {
    /// Create a new HPERSIST command
    pub fn new(key: impl Into<String>, fields: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            fields: fields.into_iter().map(|f| f.into()).collect(),
        }
    }
}

impl Command for HPersist {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HPERSIST"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from("FIELDS"))),
            Frame::BulkString(Some(Bytes::from(self.fields.len().to_string()))),
        ];
        for field in &self.fields {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                field.as_bytes(),
            ))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::Integer(n) => result.push(n),
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

/// HPTTL command - Get TTL in milliseconds for hash fields (Redis 7.4+)
///
/// Returns the remaining time to live in milliseconds for hash fields.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::HPTtl;
///
/// let cmd = HPTtl::new("myhash", vec!["field1", "field2"]);
/// ```
#[derive(Debug, Clone)]
pub struct HPTtl {
    key: String,
    fields: Vec<String>,
}

impl HPTtl {
    /// Create a new HPTTL command
    pub fn new(key: impl Into<String>, fields: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            fields: fields.into_iter().map(|f| f.into()).collect(),
        }
    }
}

impl Command for HPTtl {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HPTTL"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from("FIELDS"))),
            Frame::BulkString(Some(Bytes::from(self.fields.len().to_string()))),
        ];
        for field in &self.fields {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                field.as_bytes(),
            ))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::Integer(n) => result.push(n),
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

/// HTTL command - Get TTL in seconds for hash fields (Redis 7.4+)
///
/// Returns the remaining time to live in seconds for hash fields.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::HTtl;
///
/// let cmd = HTtl::new("myhash", vec!["field1", "field2"]);
/// ```
#[derive(Debug, Clone)]
pub struct HTtl {
    key: String,
    fields: Vec<String>,
}

impl HTtl {
    /// Create a new HTTL command
    pub fn new(key: impl Into<String>, fields: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            fields: fields.into_iter().map(|f| f.into()).collect(),
        }
    }
}

impl Command for HTtl {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HTTL"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from("FIELDS"))),
            Frame::BulkString(Some(Bytes::from(self.fields.len().to_string()))),
        ];
        for field in &self.fields {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                field.as_bytes(),
            ))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    match item {
                        Frame::Integer(n) => result.push(n),
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

/// HMSET command - Set multiple hash fields (deprecated, use HSET)
///
/// This command is deprecated. Use HSET with multiple field-value pairs instead.
///
/// Available since Redis 2.0.0. Deprecated in Redis 4.0.0.
#[derive(Debug, Clone)]
pub struct HMSet {
    key: String,
    fields: Vec<(String, Bytes)>,
}

impl HMSet {
    /// Create a new HMSET command
    pub fn new(key: impl Into<String>, fields: Vec<(impl Into<String>, impl Into<Bytes>)>) -> Self {
        Self {
            key: key.into(),
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }
}

impl Command for HMSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HMSET"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];
        for (field, value) in &self.fields {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                field.as_bytes(),
            ))));
            frames.push(Frame::BulkString(Some(value.clone())));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// HSETEX command - Set hash field with expiration (Redis 7.4+, not yet released)
///
/// Sets a hash field with an expiration time.
/// Note: This command may not be available in all Redis versions.
#[derive(Debug, Clone)]
pub struct HSetEx {
    key: String,
    seconds: i64,
    field: String,
    value: Bytes,
}

impl HSetEx {
    /// Create a new HSETEX command
    pub fn new(
        key: impl Into<String>,
        seconds: i64,
        field: impl Into<String>,
        value: impl Into<Bytes>,
    ) -> Self {
        Self {
            key: key.into(),
            seconds,
            field: field.into(),
            value: value.into(),
        }
    }
}

impl Command for HSetEx {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("HSETEX"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.seconds.to_string()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.field.as_bytes()))),
            Frame::BulkString(Some(self.value.clone())),
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

/// HGETDEL command - Get and delete a hash field (Redis 7.4+, not yet released)
///
/// Gets the value of a hash field and deletes it atomically.
/// Note: This command may not be available in all Redis versions.
#[derive(Debug, Clone)]
pub struct HGetDel {
    key: String,
    field: String,
}

impl HGetDel {
    /// Create a new HGETDEL command
    pub fn new(key: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
        }
    }
}

impl Command for HGetDel {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("HGETDEL"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.field.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(data)),
            Frame::BulkString(None) | Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// HGETEX command - Get hash field with expiration update (Redis 7.4+, not yet released)
///
/// Gets the value of a hash field and optionally updates its expiration.
/// Note: This command may not be available in all Redis versions.
#[derive(Debug, Clone)]
pub struct HGetEx {
    key: String,
    field: String,
    expiration: Option<i64>,
}

impl HGetEx {
    /// Create a new HGETEX command
    pub fn new(key: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
            expiration: None,
        }
    }

    /// Set expiration in seconds
    pub fn ex(mut self, seconds: i64) -> Self {
        self.expiration = Some(seconds);
        self
    }
}

impl Command for HGetEx {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HGETEX"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.field.as_bytes()))),
        ];
        if let Some(exp) = self.expiration {
            frames.push(Frame::BulkString(Some(Bytes::from("EX"))));
            frames.push(Frame::BulkString(Some(Bytes::from(exp.to_string()))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(data)),
            Frame::BulkString(None) | Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}
