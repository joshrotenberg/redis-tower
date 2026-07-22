use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// SETBIT key offset value
///
/// Sets or clears the bit at `offset` in the string value stored at `key`.
/// Returns the original bit value stored at `offset`.
#[derive(Clone)]
pub struct SetBit {
    key: String,
    offset: u64,
    value: u8,
}

impl SetBit {
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
#[derive(Clone)]
pub struct GetBit {
    key: String,
    offset: u64,
}

impl GetBit {
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
#[derive(Clone)]
pub struct BitCount {
    key: String,
    range: Option<(i64, i64)>,
    bit_mode: bool,
}

impl BitCount {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            range: None,
            bit_mode: false,
        }
    }

    /// Limit the count to the byte range `[start, end]`.
    pub fn range(mut self, start: i64, end: i64) -> Self {
        self.range = Some((start, end));
        self
    }

    /// Interpret the range as bit offsets instead of byte offsets.
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
#[derive(Clone)]
pub struct BitPos {
    key: String,
    bit: u8,
    range: Option<(i64, i64)>,
    bit_mode: bool,
}

impl BitPos {
    pub fn new(key: impl Into<String>, bit: u8) -> Self {
        Self {
            key: key.into(),
            bit,
            range: None,
            bit_mode: false,
        }
    }

    /// Limit the search to the byte range `[start, end]`.
    pub fn range(mut self, start: i64, end: i64) -> Self {
        self.range = Some((start, end));
        self
    }

    /// Interpret the range as bit offsets instead of byte offsets.
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
#[derive(Clone)]
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
#[derive(Clone)]
pub struct BitOp {
    operation: BitOperation,
    destkey: String,
    keys: Vec<String>,
}

impl BitOp {
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

/// OVERFLOW behavior for `BITFIELD` `INCRBY`/`SET` operations.
///
/// Controls how the server handles overflow on signed/unsigned integers:
/// - `Wrap` -- wrap around (the default for both signed and unsigned).
/// - `Sat` -- saturate at the min/max value of the field type.
/// - `Fail` -- do not perform the operation and return nil for that sub-command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitfieldOverflow {
    /// Wrap around on overflow.
    Wrap,
    /// Saturate at the type's min/max on overflow.
    Sat,
    /// Fail (return nil) on overflow.
    Fail,
}

impl BitfieldOverflow {
    fn as_str(&self) -> &str {
        match self {
            BitfieldOverflow::Wrap => "WRAP",
            BitfieldOverflow::Sat => "SAT",
            BitfieldOverflow::Fail => "FAIL",
        }
    }
}

/// A single sub-operation within a `BITFIELD` command.
#[derive(Clone)]
enum BitfieldOp {
    Get {
        encoding: String,
        offset: String,
    },
    Set {
        encoding: String,
        offset: String,
        value: i64,
    },
    IncrBy {
        encoding: String,
        offset: String,
        increment: i64,
    },
    Overflow(BitfieldOverflow),
}

/// BITFIELD key \[GET enc offset\] \[SET enc offset value\]
/// \[INCRBY enc offset increment\] \[OVERFLOW WRAP|SAT|FAIL\] ...
///
/// Performs a series of arbitrary-width integer operations on the string stored
/// at `key`, treating it as an array of packed bit fields. Sub-operations are
/// applied in the order they are added. Each `GET`, `SET`, and `INCRBY` produces
/// one entry in the response array; `OVERFLOW` produces no entry and instead
/// controls the behavior of subsequent `INCRBY`/`SET` ops.
///
/// Returns one `Option<i64>` per `GET`/`SET`/`INCRBY`. The value is `None` only
/// when a preceding `OVERFLOW FAIL` caused that operation to be skipped.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_commands::{Bitfield, BitfieldOverflow};
/// use redis_tower_core::RedisConnection;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let results = conn
///     .execute(
///         Bitfield::new("mykey")
///             .set("u8", "0", 255)
///             .overflow(BitfieldOverflow::Sat)
///             .incr_by("u8", "0", 10),
///     )
///     .await?;
/// # let _ = results;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Bitfield {
    key: String,
    ops: Vec<BitfieldOp>,
}

impl Bitfield {
    /// Create a new `BITFIELD` command targeting `key`.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            ops: Vec::new(),
        }
    }

    /// Add a `GET` sub-operation.
    ///
    /// `encoding` is a type like `"u8"` or `"i16"`; `offset` is either an
    /// absolute bit offset like `"0"` or a type-relative offset like `"#2"`.
    pub fn get(mut self, encoding: impl Into<String>, offset: impl Into<String>) -> Self {
        self.ops.push(BitfieldOp::Get {
            encoding: encoding.into(),
            offset: offset.into(),
        });
        self
    }

    /// Add a `SET` sub-operation that writes `value` and returns the old value.
    pub fn set(
        mut self,
        encoding: impl Into<String>,
        offset: impl Into<String>,
        value: i64,
    ) -> Self {
        self.ops.push(BitfieldOp::Set {
            encoding: encoding.into(),
            offset: offset.into(),
            value,
        });
        self
    }

    /// Add an `INCRBY` sub-operation and return the new value.
    pub fn incr_by(
        mut self,
        encoding: impl Into<String>,
        offset: impl Into<String>,
        increment: i64,
    ) -> Self {
        self.ops.push(BitfieldOp::IncrBy {
            encoding: encoding.into(),
            offset: offset.into(),
            increment,
        });
        self
    }

    /// Set the overflow behavior for subsequent `INCRBY`/`SET` operations.
    pub fn overflow(mut self, overflow: BitfieldOverflow) -> Self {
        self.ops.push(BitfieldOp::Overflow(overflow));
        self
    }
}

/// Parse a `BITFIELD`/`BITFIELD_RO` response array into one entry per op.
fn parse_bitfield_response(frame: Frame) -> Result<Vec<Option<i64>>, RedisError> {
    match frame {
        Frame::Array(Some(frames)) => frames
            .into_iter()
            .map(|f| match f {
                Frame::Integer(n) => Ok(Some(n)),
                Frame::Null => Ok(None),
                Frame::BulkString(None) => Ok(None),
                Frame::Array(None) => Ok(None),
                other => Err(RedisError::UnexpectedResponse {
                    expected: "integer or nil",
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

impl Command for Bitfield {
    type Response = Vec<Option<i64>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("BITFIELD"), bulk(self.key.as_str())];
        for op in &self.ops {
            match op {
                BitfieldOp::Get { encoding, offset } => {
                    args.push(bulk("GET"));
                    args.push(bulk(encoding.as_str()));
                    args.push(bulk(offset.as_str()));
                }
                BitfieldOp::Set {
                    encoding,
                    offset,
                    value,
                } => {
                    args.push(bulk("SET"));
                    args.push(bulk(encoding.as_str()));
                    args.push(bulk(offset.as_str()));
                    args.push(bulk(value.to_string()));
                }
                BitfieldOp::IncrBy {
                    encoding,
                    offset,
                    increment,
                } => {
                    args.push(bulk("INCRBY"));
                    args.push(bulk(encoding.as_str()));
                    args.push(bulk(offset.as_str()));
                    args.push(bulk(increment.to_string()));
                }
                BitfieldOp::Overflow(overflow) => {
                    args.push(bulk("OVERFLOW"));
                    args.push(bulk(overflow.as_str()));
                }
            }
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_bitfield_response(frame)
    }

    fn name(&self) -> &str {
        "BITFIELD"
    }
}

/// BITFIELD_RO key GET enc offset \[GET enc offset ...\]
///
/// The read-only variant of `BITFIELD`. Supports only `GET` sub-operations,
/// which makes it safe to run on replicas. Returns one `Option<i64>` per `GET`.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_commands::BitfieldRo;
/// use redis_tower_core::RedisConnection;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let values = conn
///     .execute(BitfieldRo::new("mykey").get("u8", "0").get("u8", "8"))
///     .await?;
/// # let _ = values;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct BitfieldRo {
    key: String,
    gets: Vec<(String, String)>,
}

impl BitfieldRo {
    /// Create a new `BITFIELD_RO` command targeting `key`.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            gets: Vec::new(),
        }
    }

    /// Add a `GET` sub-operation.
    ///
    /// `encoding` is a type like `"u8"` or `"i16"`; `offset` is either an
    /// absolute bit offset like `"0"` or a type-relative offset like `"#2"`.
    pub fn get(mut self, encoding: impl Into<String>, offset: impl Into<String>) -> Self {
        self.gets.push((encoding.into(), offset.into()));
        self
    }
}

impl Command for BitfieldRo {
    type Response = Vec<Option<i64>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("BITFIELD_RO"), bulk(self.key.as_str())];
        for (encoding, offset) in &self.gets {
            args.push(bulk("GET"));
            args.push(bulk(encoding.as_str()));
            args.push(bulk(offset.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_bitfield_response(frame)
    }

    fn name(&self) -> &str {
        "BITFIELD_RO"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    #[test]
    fn bitfield_get_set_incrby_overflow_to_frame() {
        let cmd = Bitfield::new("mykey")
            .get("u8", "0")
            .set("u8", "0", 255)
            .overflow(BitfieldOverflow::Sat)
            .incr_by("u8", "0", 10);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("BITFIELD"),
                bulk("mykey"),
                bulk("GET"),
                bulk("u8"),
                bulk("0"),
                bulk("SET"),
                bulk("u8"),
                bulk("0"),
                bulk("255"),
                bulk("OVERFLOW"),
                bulk("SAT"),
                bulk("INCRBY"),
                bulk("u8"),
                bulk("0"),
                bulk("10"),
            ])
        );
    }

    #[test]
    fn bitfield_overflow_fail_to_frame() {
        let cmd = Bitfield::new("k")
            .overflow(BitfieldOverflow::Fail)
            .incr_by("u8", "#0", 1);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("BITFIELD"),
                bulk("k"),
                bulk("OVERFLOW"),
                bulk("FAIL"),
                bulk("INCRBY"),
                bulk("u8"),
                bulk("#0"),
                bulk("1"),
            ])
        );
    }

    #[test]
    fn bitfield_parses_integer_and_nil() {
        let cmd = Bitfield::new("k");
        let frame = Frame::Array(Some(vec![Frame::Integer(7), Frame::Null]));
        assert_eq!(cmd.parse_response(frame).unwrap(), vec![Some(7), None]);
    }

    #[test]
    fn bitfield_not_idempotent() {
        assert!(!Bitfield::new("k").idempotent());
    }

    #[test]
    fn bitfield_ro_two_gets_to_frame() {
        let cmd = BitfieldRo::new("mykey").get("u8", "0").get("i16", "#1");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("BITFIELD_RO"),
                bulk("mykey"),
                bulk("GET"),
                bulk("u8"),
                bulk("0"),
                bulk("GET"),
                bulk("i16"),
                bulk("#1"),
            ])
        );
    }

    #[test]
    fn bitfield_ro_is_idempotent() {
        assert!(BitfieldRo::new("k").get("u8", "0").idempotent());
    }
}
