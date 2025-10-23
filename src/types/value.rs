//! Redis value types

use bytes::Bytes;

/// Redis value types
#[derive(Debug, Clone, PartialEq)]
pub enum RedisValue {
    /// Null value
    Null,
    /// String value
    String(Bytes),
    /// Integer value
    Integer(i64),
    /// Array of values
    Array(Vec<RedisValue>),
    /// Error value
    Error(String),
}

impl RedisValue {
    /// Convert to Option<String>
    pub fn as_string(&self) -> Option<String> {
        match self {
            RedisValue::String(bytes) => String::from_utf8(bytes.to_vec()).ok(),
            _ => None,
        }
    }

    /// Convert to Option<i64>
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            RedisValue::Integer(n) => Some(*n),
            _ => None,
        }
    }

    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, RedisValue::Null)
    }
}
