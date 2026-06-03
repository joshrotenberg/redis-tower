use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// SETBIT key offset value
///
/// Sets or clears the bit at `offset` in the string value stored at `key`.
/// Returns the original bit value stored at `offset`.
///
/// See: <https://redis.io/commands/setbit>
pub struct SetBit {
    key: String,
    offset: u64,
    value: u8,
}

impl SetBit {
    /// Creates a new [`SetBit`] command.
    pub fn new(key: impl Into<String>, offset: u64, value: u8) -> Self {
        Self {
            key: key.into(),
            offset,
            value,
        }
    }
}

impl Command for SetBit {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("SETBIT"),
            bulk(self.key.as_str()),
            bulk(self.offset.to_string()),
            bulk(self.value.to_string()),
        ])
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
        "SETBIT"
    }
}

/// GETBIT key offset
///
/// Returns the bit value at `offset` in the string value stored at `key`.
///
/// See: <https://redis.io/commands/getbit>
pub struct GetBit {
    key: String,
    offset: u64,
}

impl GetBit {
    /// Creates a new [`GetBit`] command.
    pub fn new(key: impl Into<String>, offset: u64) -> Self {
        Self {
            key: key.into(),
            offset,
        }
    }
}

impl Command for GetBit {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("GETBIT"),
            bulk(self.key.as_str()),
            bulk(self.offset.to_string()),
        ])
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
        "GETBIT"
    }
}

/// BITCOUNT key \[start end \[BYTE|BIT\]\]
///
/// Counts the number of set bits (population counting) in a string.
/// By default counts all bytes; use `.range()` to limit and `.bit_mode()`
/// to interpret the range as bit offsets instead of byte offsets.
///
/// See: <https://redis.io/commands/bitcount>
pub struct BitCount {
    key: String,
    range: Option<(i64, i64)>,
    bit_mode: bool,
}

impl BitCount {
    /// Creates a new [`BitCount`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            range: None,
            bit_mode: false,
        }
    }

    /// Limit the count to the byte range `[start, end]`.
    #[must_use]
    pub fn range(mut self, start: i64, end: i64) -> Self {
        self.range = Some((start, end));
        self
    }

    /// Interpret the range as bit offsets instead of byte offsets.
    #[must_use]
    pub fn bit_mode(mut self) -> Self {
        self.bit_mode = true;
        self
    }
}

impl Command for BitCount {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("BITCOUNT"), bulk(self.key.as_str())];
        if let Some((start, end)) = self.range {
            args.push(bulk(start.to_string()));
            args.push(bulk(end.to_string()));
            if self.bit_mode {
                args.push(bulk("BIT"));
            }
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
        "BITCOUNT"
    }
}

/// BITPOS key bit \[start \[end \[BYTE|BIT\]\]\]
///
/// Returns the position of the first bit set to `bit` (0 or 1) in the string
/// stored at `key`. Use `.range()` to limit the search and `.bit_mode()` to
/// interpret the range as bit offsets instead of byte offsets.
///
/// See: <https://redis.io/commands/bitpos>
pub struct BitPos {
    key: String,
    bit: u8,
    range: Option<(i64, i64)>,
    bit_mode: bool,
}

impl BitPos {
    /// Creates a new [`BitPos`] command.
    pub fn new(key: impl Into<String>, bit: u8) -> Self {
        Self {
            key: key.into(),
            bit,
            range: None,
            bit_mode: false,
        }
    }

    /// Limit the search to the byte range `[start, end]`.
    #[must_use]
    pub fn range(mut self, start: i64, end: i64) -> Self {
        self.range = Some((start, end));
        self
    }

    /// Interpret the range as bit offsets instead of byte offsets.
    #[must_use]
    pub fn bit_mode(mut self) -> Self {
        self.bit_mode = true;
        self
    }
}

impl Command for BitPos {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("BITPOS"),
            bulk(self.key.as_str()),
            bulk(self.bit.to_string()),
        ];
        if let Some((start, end)) = self.range {
            args.push(bulk(start.to_string()));
            args.push(bulk(end.to_string()));
            if self.bit_mode {
                args.push(bulk("BIT"));
            }
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
        "BITPOS"
    }
}

/// Bitwise operation for BITOP.
pub enum BitOperation {
    /// Bitwise AND.
    And,
    /// Bitwise OR.
    Or,
    /// Bitwise XOR.
    Xor,
    /// Bitwise NOT (single source key only).
    Not,
}

impl BitOperation {
    fn as_str(&self) -> &str {
        match self {
            BitOperation::And => "AND",
            BitOperation::Or => "OR",
            BitOperation::Xor => "XOR",
            BitOperation::Not => "NOT",
        }
    }
}

/// BITOP AND|OR|XOR|NOT destkey key \[key ...\]
///
/// Performs a bitwise operation between strings and stores the result in `destkey`.
/// Returns the size of the string stored in the destination key (the longest
/// input string length).
///
/// See: <https://redis.io/commands/bitop>
pub struct BitOp {
    operation: BitOperation,
    destkey: String,
    keys: Vec<String>,
}

impl BitOp {
    /// Creates a new [`BitOp`] command.
    pub fn new(
        operation: BitOperation,
        destkey: impl Into<String>,
        keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            operation,
            destkey: destkey.into(),
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for BitOp {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("BITOP"),
            bulk(self.operation.as_str()),
            bulk(self.destkey.as_str()),
        ];
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
        "BITOP"
    }
}
