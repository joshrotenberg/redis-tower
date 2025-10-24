//! Bitmap commands for bit-level operations on strings
//!
//! Redis bitmaps are not an actual data type, but a set of bit-oriented operations
//! defined on the String type. Useful for real-time analytics, user tracking, etc.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// SETBIT command - Set or clear a bit at a given offset
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::SetBit;
///
/// // Set bit at offset 100 to 1
/// let cmd = SetBit::new("bitmap", 100, true);
///
/// // Clear bit at offset 200
/// let cmd = SetBit::new("bitmap", 200, false);
/// ```
#[derive(Debug, Clone)]
pub struct SetBit {
    key: String,
    offset: i64,
    value: bool,
}

impl SetBit {
    /// Create a new SETBIT command
    pub fn new(key: impl Into<String>, offset: i64, value: bool) -> Self {
        Self {
            key: key.into(),
            offset,
            value,
        }
    }
}

impl Command for SetBit {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SETBIT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.offset.to_string()))),
            Frame::BulkString(Some(Bytes::from(if self.value { "1" } else { "0" }))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(0) => Ok(false),
            Frame::Integer(1) => Ok(true),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// GETBIT command - Get the value of a bit at a given offset
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::GetBit;
///
/// let cmd = GetBit::new("bitmap", 100);
/// ```
#[derive(Debug, Clone)]
pub struct GetBit {
    key: String,
    offset: i64,
}

impl GetBit {
    /// Create a new GETBIT command
    pub fn new(key: impl Into<String>, offset: i64) -> Self {
        Self {
            key: key.into(),
            offset,
        }
    }
}

impl Command for GetBit {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("GETBIT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.offset.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(0) => Ok(false),
            Frame::Integer(1) => Ok(true),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// BITCOUNT command - Count the number of set bits in a string
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::BitCount;
///
/// // Count all bits
/// let cmd = BitCount::new("bitmap");
///
/// // Count bits in a byte range
/// let cmd = BitCount::new("bitmap").range(0, 10);
///
/// // Count bits in a bit range (Redis 7.0+)
/// let cmd = BitCount::new("bitmap").bit_range(100, 200);
/// ```
#[derive(Debug, Clone)]
pub struct BitCount {
    key: String,
    start: Option<i64>,
    end: Option<i64>,
    use_bit_index: bool, // BIT option (Redis 7.0+)
}

impl BitCount {
    /// Create a new BITCOUNT command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            start: None,
            end: None,
            use_bit_index: false,
        }
    }

    /// Set byte range for counting (default interpretation)
    pub fn range(mut self, start: i64, end: i64) -> Self {
        self.start = Some(start);
        self.end = Some(end);
        self.use_bit_index = false;
        self
    }

    /// Set bit range for counting (Redis 7.0+)
    pub fn bit_range(mut self, start: i64, end: i64) -> Self {
        self.start = Some(start);
        self.end = Some(end);
        self.use_bit_index = true;
        self
    }
}

impl Command for BitCount {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("BITCOUNT"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        if let (Some(start), Some(end)) = (self.start, self.end) {
            args.push(Frame::BulkString(Some(Bytes::from(start.to_string()))));
            args.push(Frame::BulkString(Some(Bytes::from(end.to_string()))));

            if self.use_bit_index {
                args.push(Frame::BulkString(Some(Bytes::from("BIT"))));
            }
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(count) => Ok(count),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Bitwise operation type for BITOP
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOp {
    /// AND operation
    And,
    /// OR operation
    Or,
    /// XOR operation
    Xor,
    /// NOT operation (only one source key allowed)
    Not,
}

impl BitOp {
    fn as_str(&self) -> &'static str {
        match self {
            BitOp::And => "AND",
            BitOp::Or => "OR",
            BitOp::Xor => "XOR",
            BitOp::Not => "NOT",
        }
    }
}

/// BITOP command - Perform bitwise operations between strings
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::{BitOp as BitOpType, BitOp};
///
/// // AND operation
/// let cmd = BitOp::new(BitOpType::And, "result", vec!["key1", "key2"]);
///
/// // NOT operation (single key)
/// let cmd = BitOp::new(BitOpType::Not, "result", vec!["key1"]);
/// ```
#[derive(Debug, Clone)]
pub struct BitOpCmd {
    operation: BitOp,
    dest_key: String,
    keys: Vec<String>,
}

impl BitOpCmd {
    /// Create a new BITOP command
    pub fn new(
        operation: BitOp,
        dest_key: impl Into<String>,
        keys: Vec<impl Into<String>>,
    ) -> Self {
        Self {
            operation,
            dest_key: dest_key.into(),
            keys: keys.into_iter().map(|k| k.into()).collect(),
        }
    }
}

impl Command for BitOpCmd {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("BITOP"))),
            Frame::BulkString(Some(Bytes::from(self.operation.as_str()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.dest_key.as_bytes()))),
        ];

        for key in &self.keys {
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(size) => Ok(size),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// BITPOS command - Find the first bit set to 1 or 0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::BitPos;
///
/// // Find first bit set to 1
/// let cmd = BitPos::new("bitmap", true);
///
/// // Find first bit set to 0 in byte range
/// let cmd = BitPos::new("bitmap", false).range(0, 10);
///
/// // Find first bit set to 1 in bit range (Redis 7.0+)
/// let cmd = BitPos::new("bitmap", true).bit_range(100, 200);
/// ```
#[derive(Debug, Clone)]
pub struct BitPos {
    key: String,
    bit: bool,
    start: Option<i64>,
    end: Option<i64>,
    use_bit_index: bool,
}

impl BitPos {
    /// Create a new BITPOS command
    pub fn new(key: impl Into<String>, bit: bool) -> Self {
        Self {
            key: key.into(),
            bit,
            start: None,
            end: None,
            use_bit_index: false,
        }
    }

    /// Set byte range for search
    pub fn range(mut self, start: i64, end: i64) -> Self {
        self.start = Some(start);
        self.end = Some(end);
        self.use_bit_index = false;
        self
    }

    /// Set bit range for search (Redis 7.0+)
    pub fn bit_range(mut self, start: i64, end: i64) -> Self {
        self.start = Some(start);
        self.end = Some(end);
        self.use_bit_index = true;
        self
    }
}

impl Command for BitPos {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("BITPOS"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(if self.bit { "1" } else { "0" }))),
        ];

        if let Some(start) = self.start {
            args.push(Frame::BulkString(Some(Bytes::from(start.to_string()))));

            if let Some(end) = self.end {
                args.push(Frame::BulkString(Some(Bytes::from(end.to_string()))));

                if self.use_bit_index {
                    args.push(Frame::BulkString(Some(Bytes::from("BIT"))));
                }
            }
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(pos) => Ok(pos),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setbit_frame() {
        let cmd = SetBit::new("key", 100, true);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 4);
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("SETBIT")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_setbit_response() {
        assert!(!SetBit::parse_response(Frame::Integer(0)).unwrap());
        assert!(SetBit::parse_response(Frame::Integer(1)).unwrap());
    }

    #[test]
    fn test_getbit_frame() {
        let cmd = GetBit::new("key", 100);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3);
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("GETBIT")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bitcount_no_range() {
        let cmd = BitCount::new("key");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 2); // BITCOUNT + key
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bitcount_with_range() {
        let cmd = BitCount::new("key").range(0, 10);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 4); // BITCOUNT + key + start + end
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bitcount_with_bit_range() {
        let cmd = BitCount::new("key").bit_range(0, 10);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 5); // BITCOUNT + key + start + end + BIT
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bitop_and() {
        let cmd = BitOpCmd::new(BitOp::And, "dest", vec!["key1", "key2"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 5); // BITOP + AND + dest + 2 keys
                assert!(matches!(
                    &args[1],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("AND")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bitop_not() {
        let cmd = BitOpCmd::new(BitOp::Not, "dest", vec!["key1"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 4); // BITOP + NOT + dest + 1 key
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bitpos_no_range() {
        let cmd = BitPos::new("key", true);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3); // BITPOS + key + bit
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bitpos_with_range() {
        let cmd = BitPos::new("key", false).range(5, 10);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 5); // BITPOS + key + bit + start + end
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_bitpos_response() {
        let response = BitPos::parse_response(Frame::Integer(42)).unwrap();
        assert_eq!(response, 42);
    }
}
