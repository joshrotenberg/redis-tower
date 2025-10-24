//! Dynamic Redis value types for script responses

use crate::codec::Frame;
use crate::types::RedisError;
use bytes::Bytes;
use std::collections::HashMap;

/// A dynamic Redis value that can represent any Redis type.
///
/// This is primarily used for EVAL/EVALSHA where the return type
/// is determined by the Lua script at runtime.
#[derive(Debug, Clone, PartialEq)]
pub enum RedisValue {
    /// Null/nil value
    Nil,

    /// Simple string (status reply)
    Status(String),

    /// Integer value
    Integer(i64),

    /// Bulk string (bytes)
    BulkString(Bytes),

    /// Array of values (recursive)
    Array(Vec<RedisValue>),

    /// RESP3: Map of key-value pairs
    Map(HashMap<RedisValue, RedisValue>),

    /// RESP3: Set of unique values
    Set(Vec<RedisValue>),

    /// RESP3: Double precision float
    Double(f64),

    /// RESP3: Boolean
    Boolean(bool),

    /// Error response
    Error(String),
}

impl RedisValue {
    /// Convert to Option<Bytes>, returning None for Nil
    pub fn as_bytes(&self) -> Result<Option<Bytes>, RedisError> {
        match self {
            RedisValue::Nil => Ok(None),
            RedisValue::BulkString(bytes) => Ok(Some(bytes.clone())),
            RedisValue::Status(s) => Ok(Some(Bytes::from(s.clone()))),
            _ => Err(RedisError::TypeMismatch {
                expected: "bulk string or status",
                actual: format!("{:?}", self),
            }),
        }
    }

    /// Convert to integer
    pub fn as_i64(&self) -> Result<i64, RedisError> {
        match self {
            RedisValue::Integer(i) => Ok(*i),
            _ => Err(RedisError::TypeMismatch {
                expected: "integer",
                actual: format!("{:?}", self),
            }),
        }
    }

    /// Convert to double
    pub fn as_f64(&self) -> Result<f64, RedisError> {
        match self {
            RedisValue::Double(d) => Ok(*d),
            RedisValue::Integer(i) => Ok(*i as f64),
            _ => Err(RedisError::TypeMismatch {
                expected: "double",
                actual: format!("{:?}", self),
            }),
        }
    }

    /// Convert to boolean
    pub fn as_bool(&self) -> Result<bool, RedisError> {
        match self {
            RedisValue::Boolean(b) => Ok(*b),
            RedisValue::Integer(i) => Ok(*i != 0),
            _ => Err(RedisError::TypeMismatch {
                expected: "boolean",
                actual: format!("{:?}", self),
            }),
        }
    }

    /// Convert to array
    pub fn as_array(&self) -> Result<&[RedisValue], RedisError> {
        match self {
            RedisValue::Array(arr) => Ok(arr),
            _ => Err(RedisError::TypeMismatch {
                expected: "array",
                actual: format!("{:?}", self),
            }),
        }
    }

    /// Convert to map
    pub fn as_map(&self) -> Result<&HashMap<RedisValue, RedisValue>, RedisError> {
        match self {
            RedisValue::Map(map) => Ok(map),
            _ => Err(RedisError::TypeMismatch {
                expected: "map",
                actual: format!("{:?}", self),
            }),
        }
    }

    /// Check if value is nil
    pub fn is_nil(&self) -> bool {
        matches!(self, RedisValue::Nil)
    }

    /// Check if value is an error
    pub fn is_error(&self) -> bool {
        matches!(self, RedisValue::Error(_))
    }
}

/// Trait for converting Frame to RedisValue
pub trait FromFrame: Sized {
    /// Convert a frame to this type
    fn from_frame(frame: Frame) -> Result<Self, RedisError>;
}

impl FromFrame for RedisValue {
    fn from_frame(frame: Frame) -> Result<Self, RedisError> {
        match frame {
            Frame::SimpleString(s) => {
                Ok(RedisValue::Status(String::from_utf8_lossy(&s).to_string()))
            }
            Frame::Error(e) => Ok(RedisValue::Error(String::from_utf8_lossy(&e).to_string())),
            Frame::Integer(i) => Ok(RedisValue::Integer(i)),
            Frame::BulkString(Some(bytes)) => Ok(RedisValue::BulkString(bytes)),
            Frame::BulkString(None) | Frame::Null => Ok(RedisValue::Nil),
            Frame::Array(items) => {
                let values: Result<Vec<_>, _> =
                    items.into_iter().map(RedisValue::from_frame).collect();
                Ok(RedisValue::Array(values?))
            }
            Frame::Map(pairs) => {
                let mut map = HashMap::new();
                for (k, v) in pairs {
                    let key = RedisValue::from_frame(k)?;
                    let value = RedisValue::from_frame(v)?;
                    map.insert(key, value);
                }
                Ok(RedisValue::Map(map))
            }
            Frame::Set(items) => {
                let values: Result<Vec<_>, _> =
                    items.into_iter().map(RedisValue::from_frame).collect();
                Ok(RedisValue::Set(values?))
            }
            Frame::Double(d) => Ok(RedisValue::Double(d)),
            Frame::Boolean(b) => Ok(RedisValue::Boolean(b)),
            Frame::Push(items) => {
                // Push frames are treated as arrays
                let values: Result<Vec<_>, _> =
                    items.into_iter().map(RedisValue::from_frame).collect();
                Ok(RedisValue::Array(values?))
            }
        }
    }
}

// Implement Hash and Eq for RedisValue to use as HashMap key
impl std::hash::Hash for RedisValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            RedisValue::Nil => 0.hash(state),
            RedisValue::Status(s) => {
                1.hash(state);
                s.hash(state);
            }
            RedisValue::Integer(i) => {
                2.hash(state);
                i.hash(state);
            }
            RedisValue::BulkString(b) => {
                3.hash(state);
                b.hash(state);
            }
            RedisValue::Boolean(b) => {
                4.hash(state);
                b.hash(state);
            }
            RedisValue::Error(e) => {
                5.hash(state);
                e.hash(state);
            }
            // Arrays, Maps, Sets, Doubles are not hashable
            // This is a limitation for using them as map keys
            RedisValue::Array(_) => panic!("Cannot hash array"),
            RedisValue::Map(_) => panic!("Cannot hash map"),
            RedisValue::Set(_) => panic!("Cannot hash set"),
            RedisValue::Double(_) => panic!("Cannot hash double"),
        }
    }
}

impl Eq for RedisValue {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nil_conversion() {
        let value = RedisValue::Nil;
        assert!(value.is_nil());
        assert_eq!(value.as_bytes().unwrap(), None);
    }

    #[test]
    fn test_integer_conversion() {
        let value = RedisValue::Integer(42);
        assert_eq!(value.as_i64().unwrap(), 42);
        assert_eq!(value.as_f64().unwrap(), 42.0);
    }

    #[test]
    fn test_bulk_string_conversion() {
        let value = RedisValue::BulkString(Bytes::from("hello"));
        assert_eq!(value.as_bytes().unwrap(), Some(Bytes::from("hello")));
    }

    #[test]
    fn test_array_conversion() {
        let value = RedisValue::Array(vec![RedisValue::Integer(1), RedisValue::Integer(2)]);
        let arr = value.as_array().unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_from_frame_simple_string() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let value = RedisValue::from_frame(frame).unwrap();
        assert_eq!(value, RedisValue::Status("OK".to_string()));
    }

    #[test]
    fn test_from_frame_bulk_string() {
        let frame = Frame::BulkString(Some(Bytes::from("hello")));
        let value = RedisValue::from_frame(frame).unwrap();
        assert_eq!(value, RedisValue::BulkString(Bytes::from("hello")));
    }

    #[test]
    fn test_from_frame_null() {
        let frame = Frame::BulkString(None);
        let value = RedisValue::from_frame(frame).unwrap();
        assert_eq!(value, RedisValue::Nil);
    }

    #[test]
    fn test_from_frame_array() {
        let frame = Frame::Array(vec![
            Frame::Integer(1),
            Frame::Integer(2),
            Frame::Integer(3),
        ]);
        let value = RedisValue::from_frame(frame).unwrap();
        if let RedisValue::Array(arr) = value {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RedisValue::Integer(1));
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_from_frame_nested_array() {
        let frame = Frame::Array(vec![
            Frame::Integer(1),
            Frame::Array(vec![Frame::Integer(2), Frame::Integer(3)]),
        ]);
        let value = RedisValue::from_frame(frame).unwrap();
        if let RedisValue::Array(arr) = value {
            assert_eq!(arr.len(), 2);
            if let RedisValue::Array(nested) = &arr[1] {
                assert_eq!(nested.len(), 2);
            } else {
                panic!("Expected nested array");
            }
        } else {
            panic!("Expected array");
        }
    }
}
