//! Shared decoders for reply shapes that differ between RESP2 and RESP3.
//!
//! `connect()` negotiates RESP3 by default, so every typed `parse_response`
//! must accept both wire forms of the values it decodes. These helpers hold
//! the per-value rules in one place instead of repeating them per command.

use redis_tower_core::{Frame, RedisError};

/// Decode a double reply sent in either protocol's form.
///
/// RESP2 sends doubles as bulk strings (`$3\r\n2.5\r\n`, `$3\r\ninf\r\n`).
/// RESP3 sends a double frame, which the codec decodes to [`Frame::Double`]
/// for finite values and to [`Frame::SpecialFloat`] for `inf`, `-inf`, and
/// `nan`. All of them are accepted, as are the simple-string replies some
/// module commands use for numeric values. Integer frames are rejected: Redis
/// never sends a double that way.
pub(crate) fn double(frame: &Frame) -> Result<f64, RedisError> {
    match frame {
        Frame::Double(d) => Ok(*d),
        Frame::SpecialFloat(b) | Frame::BulkString(Some(b)) | Frame::SimpleString(b) => {
            std::str::from_utf8(b)
                .ok()
                .and_then(|s| s.trim().parse::<f64>().ok())
                .ok_or_else(|| RedisError::UnexpectedResponse {
                    expected: "float",
                    actual: format!("{frame:?}"),
                })
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "double, special float, or float string",
            actual: format!("{other:?}"),
        }),
    }
}

/// Decode a double reply that may be null (`$-1` in RESP2, `_` in RESP3).
pub(crate) fn optional_double(frame: &Frame) -> Result<Option<f64>, RedisError> {
    match frame {
        Frame::Null | Frame::BulkString(None) => Ok(None),
        other => double(other).map(Some),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn double_accepts_both_protocols() {
        // RESP2 bulk strings, including the special values.
        assert_eq!(
            double(&Frame::BulkString(Some(Bytes::from("2.5")))).unwrap(),
            2.5
        );
        assert_eq!(
            double(&Frame::BulkString(Some(Bytes::from("inf")))).unwrap(),
            f64::INFINITY
        );
        assert_eq!(
            double(&Frame::BulkString(Some(Bytes::from("-inf")))).unwrap(),
            f64::NEG_INFINITY
        );
        // RESP3 doubles and special floats.
        assert_eq!(double(&Frame::Double(-3.0)).unwrap(), -3.0);
        assert_eq!(
            double(&Frame::SpecialFloat(Bytes::from("inf"))).unwrap(),
            f64::INFINITY
        );
        assert_eq!(
            double(&Frame::SpecialFloat(Bytes::from("-inf"))).unwrap(),
            f64::NEG_INFINITY
        );
        assert!(
            double(&Frame::SpecialFloat(Bytes::from("nan")))
                .unwrap()
                .is_nan()
        );
        assert_eq!(
            double(&Frame::SimpleString(Bytes::from("1.5"))).unwrap(),
            1.5
        );
    }

    #[test]
    fn double_rejects_non_numeric() {
        assert!(double(&Frame::Integer(4)).is_err());
        assert!(double(&Frame::BulkString(Some(Bytes::from("abc")))).is_err());
        assert!(double(&Frame::Null).is_err());
        assert!(double(&Frame::Array(Some(vec![]))).is_err());
    }

    #[test]
    fn optional_double_maps_both_nulls() {
        assert_eq!(optional_double(&Frame::Null).unwrap(), None);
        assert_eq!(optional_double(&Frame::BulkString(None)).unwrap(), None);
        assert_eq!(
            optional_double(&Frame::SpecialFloat(Bytes::from("-inf"))).unwrap(),
            Some(f64::NEG_INFINITY)
        );
    }
}
