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

// Write commands - explicitly implement with default (false) for clarity
impl ReadOnly for HSet {}
impl ReadOnly for HDel {}
impl ReadOnly for HIncrBy {}
impl ReadOnly for HIncrByFloat {}

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
