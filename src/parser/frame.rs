//! RESP frame types and implementations

// Note: RespError and Result imports removed as they're not used in this module
use std::fmt;

/// RESP frame types as defined in the Redis protocol specification
#[derive(Debug, Clone, PartialEq)]
pub enum RespFrame {
    /// Simple string (+OK\r\n)
    SimpleString(String),
    /// Error (-ERR message\r\n)
    Error(String),
    /// Integer (:123\r\n)
    Integer(i64),
    /// Bulk string ($6\r\nfoobar\r\n)
    BulkString(Vec<u8>),
    /// Array (*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n)
    Array(Vec<RespFrame>),
    /// Null bulk string ($-1\r\n)
    NullBulkString,
    /// Null array (*-1\r\n)
    NullArray,
}

/// Count the number of digits in an i64
#[inline]
fn count_digits_i64(n: i64) -> usize {
    if n == 0 {
        return 1;
    }

    let abs_val = n.unsigned_abs();
    count_digits_u64(abs_val)
}

/// Count the number of digits in a u64
#[inline]
fn count_digits_u64(mut n: u64) -> usize {
    if n == 0 {
        return 1;
    }

    let mut digits = 0;
    while n > 0 {
        digits += 1;
        n /= 10;
    }
    digits
}

/// Count the number of digits in a usize
#[inline]
fn count_digits_usize(n: usize) -> usize {
    count_digits_u64(n as u64)
}

/// Write an integer directly to a buffer as ASCII bytes without allocating a String
#[inline]
fn write_integer_bytes(buf: &mut Vec<u8>, n: i64) {
    if n == 0 {
        buf.push(b'0');
        return;
    }

    // Handle negative numbers, including i64::MIN which can't be negated
    let (is_negative, mut abs_value) = if n < 0 {
        if n == i64::MIN {
            // Special case: i64::MIN cannot be negated
            buf.extend_from_slice(b"-9223372036854775808");
            return;
        }
        (true, -n as u64)
    } else {
        (false, n as u64)
    };

    if is_negative {
        buf.push(b'-');
    }

    // Calculate number of digits
    let mut divisor = 1u64;
    let mut temp = abs_value;
    while temp >= 10 {
        temp /= 10;
        divisor *= 10;
    }

    // Write each digit
    while divisor > 0 {
        let digit = (abs_value / divisor) as u8;
        buf.push(b'0' + digit);
        abs_value %= divisor;
        divisor /= 10;
    }
}

/// Write an unsigned integer directly to a buffer as ASCII bytes without allocating a String
#[inline]
fn write_usize_bytes(buf: &mut Vec<u8>, mut n: usize) {
    if n == 0 {
        buf.push(b'0');
        return;
    }

    // Calculate number of digits
    let mut divisor = 1usize;
    let mut temp = n;
    while temp >= 10 {
        temp /= 10;
        divisor *= 10;
    }

    // Write each digit
    while divisor > 0 {
        let digit = (n / divisor) as u8;
        buf.push(b'0' + digit);
        n %= divisor;
        divisor /= 10;
    }
}

impl RespFrame {
    /// Check if this frame represents a successful response
    #[inline]
    pub fn is_successful(&self) -> bool {
        !matches!(self, RespFrame::Error(_))
    }

    /// Check if this frame is null
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, RespFrame::NullBulkString | RespFrame::NullArray)
    }

    /// Extract the integer value if this is an Integer frame
    ///
    /// # Example
    /// ```
    /// use redis_tower::parser::RespFrame;
    ///
    /// let frame = RespFrame::Integer(42);
    /// assert_eq!(frame.as_integer(), Some(42));
    ///
    /// let frame = RespFrame::SimpleString("OK".to_string());
    /// assert_eq!(frame.as_integer(), None);
    /// ```
    #[inline]
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            RespFrame::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Extract the bulk string value if this is a BulkString frame
    ///
    /// Returns a reference to the byte slice without copying.
    ///
    /// # Example
    /// ```
    /// use redis_tower::parser::RespFrame;
    ///
    /// let frame = RespFrame::BulkString(b"hello".to_vec());
    /// assert_eq!(frame.as_bulk_string(), Some(&b"hello"[..]));
    ///
    /// let frame = RespFrame::NullBulkString;
    /// assert_eq!(frame.as_bulk_string(), None);
    /// ```
    #[inline]
    pub fn as_bulk_string(&self) -> Option<&[u8]> {
        match self {
            RespFrame::BulkString(data) => Some(data),
            _ => None,
        }
    }

    /// Extract the simple string value if this is a SimpleString frame
    ///
    /// # Example
    /// ```
    /// use redis_tower::parser::RespFrame;
    ///
    /// let frame = RespFrame::SimpleString("OK".to_string());
    /// assert_eq!(frame.as_simple_string(), Some("OK"));
    ///
    /// let frame = RespFrame::Integer(42);
    /// assert_eq!(frame.as_simple_string(), None);
    /// ```
    #[inline]
    pub fn as_simple_string(&self) -> Option<&str> {
        match self {
            RespFrame::SimpleString(s) => Some(s),
            _ => None,
        }
    }

    /// Extract the error message if this is an Error frame
    ///
    /// # Example
    /// ```
    /// use redis_tower::parser::RespFrame;
    ///
    /// let frame = RespFrame::Error("ERR something went wrong".to_string());
    /// assert_eq!(frame.as_error(), Some("ERR something went wrong"));
    ///
    /// let frame = RespFrame::SimpleString("OK".to_string());
    /// assert_eq!(frame.as_error(), None);
    /// ```
    #[inline]
    pub fn as_error(&self) -> Option<&str> {
        match self {
            RespFrame::Error(s) => Some(s),
            _ => None,
        }
    }

    /// Extract the array elements if this is an Array frame
    ///
    /// Returns a reference to the array slice without copying.
    ///
    /// # Example
    /// ```
    /// use redis_tower::parser::RespFrame;
    ///
    /// let frame = RespFrame::Array(vec![
    ///     RespFrame::BulkString(b"foo".to_vec()),
    ///     RespFrame::BulkString(b"bar".to_vec()),
    /// ]);
    /// assert_eq!(frame.as_array().map(|a| a.len()), Some(2));
    ///
    /// let frame = RespFrame::NullArray;
    /// assert_eq!(frame.as_array(), None);
    /// ```
    #[inline]
    pub fn as_array(&self) -> Option<&[RespFrame]> {
        match self {
            RespFrame::Array(frames) => Some(frames),
            _ => None,
        }
    }

    /// Get an element from an Array frame by index
    ///
    /// Returns `None` if this is not an Array or the index is out of bounds.
    ///
    /// # Example
    /// ```
    /// use redis_tower::parser::RespFrame;
    ///
    /// let frame = RespFrame::Array(vec![
    ///     RespFrame::Integer(1),
    ///     RespFrame::Integer(2),
    ///     RespFrame::Integer(3),
    /// ]);
    ///
    /// assert_eq!(frame.get_array_element(0).and_then(|f| f.as_integer()), Some(1));
    /// assert_eq!(frame.get_array_element(1).and_then(|f| f.as_integer()), Some(2));
    /// assert_eq!(frame.get_array_element(10), None);
    /// ```
    #[inline]
    pub fn get_array_element(&self, index: usize) -> Option<&RespFrame> {
        match self {
            RespFrame::Array(frames) => frames.get(index),
            _ => None,
        }
    }

    /// Convert frame to bytes for transmission
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.write_to(&mut buf);
        buf
    }

    /// Write frame to a buffer
    pub fn write_to(&self, buf: &mut Vec<u8>) {
        match self {
            RespFrame::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespFrame::Error(s) => {
                buf.push(b'-');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespFrame::Integer(i) => {
                buf.push(b':');
                write_integer_bytes(buf, *i);
                buf.extend_from_slice(b"\r\n");
            }
            RespFrame::BulkString(data) => {
                buf.push(b'$');
                write_usize_bytes(buf, data.len());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            RespFrame::Array(frames) => {
                buf.push(b'*');
                write_usize_bytes(buf, frames.len());
                buf.extend_from_slice(b"\r\n");
                for frame in frames {
                    frame.write_to(buf);
                }
            }
            RespFrame::NullBulkString => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            RespFrame::NullArray => {
                buf.extend_from_slice(b"*-1\r\n");
            }
        }
    }

    /// Get the approximate size in bytes when serialized
    pub fn size_hint(&self) -> usize {
        match self {
            RespFrame::SimpleString(s) => s.len() + 3, // +\r\n
            RespFrame::Error(s) => s.len() + 3,        // -\r\n
            RespFrame::Integer(i) => {
                // :number\r\n - calculate actual digits needed
                let digits = count_digits_i64(*i);
                let sign_bytes = if *i < 0 { 1 } else { 0 };
                1 + sign_bytes + digits + 2 // : + sign + digits + \r\n
            }
            RespFrame::BulkString(data) => {
                // $len\r\n + data + \r\n
                let len_digits = count_digits_usize(data.len());
                1 + len_digits + 2 + data.len() + 2
            }
            RespFrame::Array(frames) => {
                // *count\r\n + sum of frame sizes
                let count_digits = count_digits_usize(frames.len());
                1 + count_digits + 2 + frames.iter().map(|f| f.size_hint()).sum::<usize>()
            }
            RespFrame::NullBulkString => 5, // $-1\r\n
            RespFrame::NullArray => 5,      // *-1\r\n
        }
    }
}

impl fmt::Display for RespFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespFrame::SimpleString(s) => write!(f, "SimpleString({s})"),
            RespFrame::Error(s) => write!(f, "Error({s})"),
            RespFrame::Integer(i) => write!(f, "Integer({i})"),
            RespFrame::BulkString(data) => {
                if let Ok(s) = String::from_utf8(data.clone()) {
                    write!(f, "BulkString({s})")
                } else {
                    write!(f, "BulkString({} bytes)", data.len())
                }
            }
            RespFrame::Array(frames) => {
                write!(f, "Array({})", frames.len())
            }
            RespFrame::NullBulkString => write!(f, "NullBulkString"),
            RespFrame::NullArray => write!(f, "NullArray"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization() {
        let frame = RespFrame::SimpleString("OK".to_string());
        assert_eq!(frame.serialize(), b"+OK\r\n");

        let frame = RespFrame::Error("ERR test".to_string());
        assert_eq!(frame.serialize(), b"-ERR test\r\n");

        let frame = RespFrame::Integer(42);
        assert_eq!(frame.serialize(), b":42\r\n");

        let frame = RespFrame::BulkString(b"hello".to_vec());
        assert_eq!(frame.serialize(), b"$5\r\nhello\r\n");

        let frame = RespFrame::NullBulkString;
        assert_eq!(frame.serialize(), b"$-1\r\n");
    }

    #[test]
    fn test_frame_properties() {
        assert!(RespFrame::SimpleString("OK".to_string()).is_successful());
        assert!(!RespFrame::Error("ERR".to_string()).is_successful());

        assert!(RespFrame::NullBulkString.is_null());
        assert!(RespFrame::NullArray.is_null());
        assert!(!RespFrame::SimpleString("OK".to_string()).is_null());
    }

    #[test]
    fn test_array_serialization() {
        let frame = RespFrame::Array(vec![
            RespFrame::BulkString(b"foo".to_vec()),
            RespFrame::BulkString(b"bar".to_vec()),
        ]);
        assert_eq!(frame.serialize(), b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }
}
