//! Traits for converting Redis response values into Rust types.
//!
//! Commands return raw types like `Option<Bytes>` or `i64`. These traits
//! provide ergonomic conversion to user types.
//!
//! # Example
//!
//! The values below are what command execution hands back: a `GET` reply is an
//! `Option<Bytes>`, so `.parse_into()` is called directly on it.
//!
//! ```no_run
//! use bytes::Bytes;
//! use redis_tower_core::RedisValueExt;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Convert Option<Bytes> to String
//! let reply: Option<Bytes> = Some(Bytes::from("alice"));
//! let name: String = reply.parse_into()?;
//! assert_eq!(name, "alice");
//!
//! // Convert Option<Bytes> to integer (Redis stores numbers as strings)
//! let reply: Option<Bytes> = Some(Bytes::from("42"));
//! let count: u32 = reply.parse_into()?;
//! assert_eq!(count, 42);
//!
//! // Handle missing keys with Option
//! let reply: Option<Bytes> = None;
//! let missing: Option<String> = reply.parse_into()?;
//! assert_eq!(missing, None);
//! # Ok(())
//! # }
//! ```

use bytes::Bytes;

use crate::error::RedisError;

/// Convert a Redis bulk string (Bytes) into a Rust type.
///
/// Implement this trait for your own types to enable `.parse_into::<T>()`
/// on command responses that return `Option<Bytes>` or `Bytes`.
///
/// # Example
///
/// ```no_run
/// use bytes::Bytes;
/// use redis_tower_core::{FromRedisBytes, RedisError};
///
/// struct UserId(u64);
///
/// impl FromRedisBytes for UserId {
///     fn from_redis_bytes(bytes: Bytes) -> Result<Self, RedisError> {
///         let s = std::str::from_utf8(&bytes).map_err(|_| RedisError::TypeMismatch {
///             expected: "valid UTF-8 for UserId",
///         })?;
///         let id = s.parse::<u64>().map_err(|_| RedisError::TypeMismatch {
///             expected: "u64 for UserId",
///         })?;
///         Ok(UserId(id))
///     }
/// }
/// # let id = UserId::from_redis_bytes(Bytes::from("7")).unwrap();
/// # assert_eq!(id.0, 7);
/// ```
pub trait FromRedisBytes: Sized {
    /// Parse raw bytes from a Redis bulk string into this type.
    fn from_redis_bytes(bytes: Bytes) -> Result<Self, RedisError>;
}

/// Extension trait for converting Redis response values to typed results.
///
/// Provides `.parse_into::<T>()` on common response types like
/// `Option<Bytes>`, `i64`, `f64`, `bool`, `Vec<Bytes>`, and more.
///
/// # Example
///
/// ```no_run
/// use bytes::Bytes;
/// use redis_tower_core::RedisValueExt;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // The GET reply for a counter Redis stores as the string "42"
/// let reply: Option<Bytes> = Some(Bytes::from("42"));
/// let count: u32 = reply.parse_into()?;
/// assert_eq!(count, 42);
/// # Ok(())
/// # }
/// ```
pub trait RedisValueExt<T> {
    /// Convert this Redis response value into the target type `U`.
    fn parse_into<U>(self) -> Result<U, RedisError>
    where
        U: RedisConvert<T>;
}

/// Trait for converting from a Redis response type to a user type.
///
/// This is the core conversion trait. `From` is the response type produced by
/// a command (`Option<Bytes>`, `i64`, `f64`, `Bytes`, `Vec<Bytes>`, etc.)
/// and `Self` is the target Rust type. Blanket implementations are provided
/// for common conversions; implement this trait for custom conversions.
pub trait RedisConvert<From>: Sized {
    /// Perform the conversion from a Redis response value to this type.
    fn redis_convert(value: From) -> Result<Self, RedisError>;
}

// -- RedisValueExt implementations --

impl RedisValueExt<Option<Bytes>> for Option<Bytes> {
    fn parse_into<U: RedisConvert<Option<Bytes>>>(self) -> Result<U, RedisError> {
        U::redis_convert(self)
    }
}

impl RedisValueExt<Bytes> for Bytes {
    fn parse_into<U: RedisConvert<Bytes>>(self) -> Result<U, RedisError> {
        U::redis_convert(self)
    }
}

impl RedisValueExt<i64> for i64 {
    fn parse_into<U: RedisConvert<i64>>(self) -> Result<U, RedisError> {
        U::redis_convert(self)
    }
}

impl RedisValueExt<f64> for f64 {
    fn parse_into<U: RedisConvert<f64>>(self) -> Result<U, RedisError> {
        U::redis_convert(self)
    }
}

impl RedisValueExt<bool> for bool {
    fn parse_into<U: RedisConvert<bool>>(self) -> Result<U, RedisError> {
        U::redis_convert(self)
    }
}

impl RedisValueExt<Vec<Bytes>> for Vec<Bytes> {
    fn parse_into<U: RedisConvert<Vec<Bytes>>>(self) -> Result<U, RedisError> {
        U::redis_convert(self)
    }
}

impl RedisValueExt<Vec<Option<Bytes>>> for Vec<Option<Bytes>> {
    fn parse_into<U: RedisConvert<Vec<Option<Bytes>>>>(self) -> Result<U, RedisError> {
        U::redis_convert(self)
    }
}

impl RedisValueExt<Vec<(Bytes, Bytes)>> for Vec<(Bytes, Bytes)> {
    fn parse_into<U: RedisConvert<Vec<(Bytes, Bytes)>>>(self) -> Result<U, RedisError> {
        U::redis_convert(self)
    }
}

// -- FromRedisBytes implementations --

impl FromRedisBytes for String {
    fn from_redis_bytes(bytes: Bytes) -> Result<Self, RedisError> {
        String::from_utf8(bytes.to_vec()).map_err(|_| RedisError::TypeMismatch {
            expected: "valid UTF-8 string",
        })
    }
}

impl FromRedisBytes for Bytes {
    fn from_redis_bytes(bytes: Bytes) -> Result<Self, RedisError> {
        Ok(bytes)
    }
}

impl FromRedisBytes for Vec<u8> {
    fn from_redis_bytes(bytes: Bytes) -> Result<Self, RedisError> {
        Ok(bytes.to_vec())
    }
}

macro_rules! impl_from_redis_bytes_parse {
    ($($t:ty),+) => {
        $(
            impl FromRedisBytes for $t {
                fn from_redis_bytes(bytes: Bytes) -> Result<Self, RedisError> {
                    let s = std::str::from_utf8(&bytes).map_err(|_| RedisError::TypeMismatch {
                        expected: concat!("parseable as ", stringify!($t)),
                    })?;
                    s.parse::<$t>().map_err(|_| RedisError::TypeMismatch {
                        expected: concat!("parseable as ", stringify!($t)),
                    })
                }
            }
        )+
    };
}

impl_from_redis_bytes_parse!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, f32, f64);

impl FromRedisBytes for bool {
    fn from_redis_bytes(bytes: Bytes) -> Result<Self, RedisError> {
        match bytes.as_ref() {
            b"1" | b"true" | b"TRUE" => Ok(true),
            b"0" | b"false" | b"FALSE" => Ok(false),
            _ => Err(RedisError::TypeMismatch {
                expected: "boolean (1/0 or true/false)",
            }),
        }
    }
}

// -- RedisConvert from Option<Bytes> --

// Option<Bytes> -> T where T: FromRedisBytes (requires Some, errors on None)
impl<T: FromRedisBytes> RedisConvert<Option<Bytes>> for T {
    fn redis_convert(value: Option<Bytes>) -> Result<Self, RedisError> {
        match value {
            Some(b) => T::from_redis_bytes(b),
            None => Err(RedisError::TypeMismatch {
                expected: "non-null value",
            }),
        }
    }
}

// Option<Bytes> -> Option<T> where T: FromRedisBytes (None maps to None)
impl<T: FromRedisBytes> RedisConvert<Option<Bytes>> for Option<T> {
    fn redis_convert(value: Option<Bytes>) -> Result<Self, RedisError> {
        match value {
            Some(b) => T::from_redis_bytes(b).map(Some),
            None => Ok(None),
        }
    }
}

// -- RedisConvert from Bytes --

impl<T: FromRedisBytes> RedisConvert<Bytes> for T {
    fn redis_convert(value: Bytes) -> Result<Self, RedisError> {
        T::from_redis_bytes(value)
    }
}

// -- RedisConvert from i64 --

macro_rules! impl_redis_convert_from_i64 {
    ($($t:ty),+) => {
        $(
            impl RedisConvert<i64> for $t {
                fn redis_convert(value: i64) -> Result<Self, RedisError> {
                    <$t>::try_from(value).map_err(|_| RedisError::TypeMismatch {
                        expected: concat!(stringify!($t), " (value out of range)"),
                    })
                }
            }
        )+
    };
}

impl_redis_convert_from_i64!(
    i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, isize, usize
);

impl RedisConvert<i64> for f64 {
    fn redis_convert(value: i64) -> Result<Self, RedisError> {
        Ok(value as f64)
    }
}

impl RedisConvert<i64> for f32 {
    fn redis_convert(value: i64) -> Result<Self, RedisError> {
        Ok(value as f32)
    }
}

impl RedisConvert<i64> for bool {
    fn redis_convert(value: i64) -> Result<Self, RedisError> {
        Ok(value != 0)
    }
}

impl RedisConvert<i64> for String {
    fn redis_convert(value: i64) -> Result<Self, RedisError> {
        Ok(value.to_string())
    }
}

// -- RedisConvert from f64 --

impl RedisConvert<f64> for f64 {
    fn redis_convert(value: f64) -> Result<Self, RedisError> {
        Ok(value)
    }
}

impl RedisConvert<f64> for f32 {
    fn redis_convert(value: f64) -> Result<Self, RedisError> {
        Ok(value as f32)
    }
}

impl RedisConvert<f64> for String {
    fn redis_convert(value: f64) -> Result<Self, RedisError> {
        Ok(value.to_string())
    }
}

// -- RedisConvert from bool --

impl RedisConvert<bool> for bool {
    fn redis_convert(value: bool) -> Result<Self, RedisError> {
        Ok(value)
    }
}

impl RedisConvert<bool> for i64 {
    fn redis_convert(value: bool) -> Result<Self, RedisError> {
        Ok(if value { 1 } else { 0 })
    }
}

impl RedisConvert<bool> for String {
    fn redis_convert(value: bool) -> Result<Self, RedisError> {
        Ok(value.to_string())
    }
}

// -- RedisConvert from Vec<Bytes> --

/// Blanket impl: any `Vec<T>` where `T: FromRedisBytes` can be parsed from a
/// `Vec<Bytes>` Redis response.  This covers `Vec<Bytes>`, `Vec<String>`,
/// `Vec<u32>`, etc. without requiring a separate impl for each type.
impl<T: FromRedisBytes> RedisConvert<Vec<Bytes>> for Vec<T> {
    fn redis_convert(value: Vec<Bytes>) -> Result<Self, RedisError> {
        value.into_iter().map(T::from_redis_bytes).collect()
    }
}

// -- RedisConvert from Vec<Option<Bytes>> --

impl RedisConvert<Vec<Option<Bytes>>> for Vec<Option<Bytes>> {
    fn redis_convert(value: Vec<Option<Bytes>>) -> Result<Self, RedisError> {
        Ok(value)
    }
}

impl RedisConvert<Vec<Option<Bytes>>> for Vec<Option<String>> {
    fn redis_convert(value: Vec<Option<Bytes>>) -> Result<Self, RedisError> {
        value
            .into_iter()
            .map(|opt| {
                opt.map(|b| {
                    String::from_utf8(b.to_vec()).map_err(|_| RedisError::TypeMismatch {
                        expected: "valid UTF-8 string",
                    })
                })
                .transpose()
            })
            .collect()
    }
}

// -- RedisConvert from Vec<(Bytes, Bytes)> --

impl RedisConvert<Vec<(Bytes, Bytes)>> for Vec<(Bytes, Bytes)> {
    fn redis_convert(value: Vec<(Bytes, Bytes)>) -> Result<Self, RedisError> {
        Ok(value)
    }
}

impl RedisConvert<Vec<(Bytes, Bytes)>> for Vec<(String, String)> {
    fn redis_convert(value: Vec<(Bytes, Bytes)>) -> Result<Self, RedisError> {
        value
            .into_iter()
            .map(|(k, v)| {
                let ks = String::from_utf8(k.to_vec()).map_err(|_| RedisError::TypeMismatch {
                    expected: "valid UTF-8 key",
                })?;
                let vs = String::from_utf8(v.to_vec()).map_err(|_| RedisError::TypeMismatch {
                    expected: "valid UTF-8 value",
                })?;
                Ok((ks, vs))
            })
            .collect()
    }
}

/// Blanket impl: `HashMap<String, V>` where `V: FromRedisBytes` can be parsed
/// from a `Vec<(Bytes, Bytes)>` Redis response (e.g. HGETALL results).
/// This covers `HashMap<String, String>`, `HashMap<String, f64>`, etc.
impl<V: FromRedisBytes> RedisConvert<Vec<(Bytes, Bytes)>> for std::collections::HashMap<String, V> {
    fn redis_convert(value: Vec<(Bytes, Bytes)>) -> Result<Self, RedisError> {
        value
            .into_iter()
            .map(|(k, v)| {
                let key = String::from_utf8(k.to_vec()).map_err(|_| RedisError::TypeMismatch {
                    expected: "valid UTF-8 key",
                })?;
                let val = V::from_redis_bytes(v)?;
                Ok((key, val))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // -- FromRedisBytes tests --

    #[test]
    fn bytes_to_string() {
        let b = Bytes::from("hello");
        let s: String = FromRedisBytes::from_redis_bytes(b).unwrap();
        assert_eq!(s, "hello");
    }

    #[test]
    fn bytes_to_bytes() {
        let b = Bytes::from("data");
        let r: Bytes = FromRedisBytes::from_redis_bytes(b.clone()).unwrap();
        assert_eq!(r, b);
    }

    #[test]
    fn bytes_to_vec_u8() {
        let b = Bytes::from("abc");
        let v: Vec<u8> = FromRedisBytes::from_redis_bytes(b).unwrap();
        assert_eq!(v, b"abc");
    }

    #[test]
    fn bytes_to_i64() {
        let b = Bytes::from("42");
        let n: i64 = FromRedisBytes::from_redis_bytes(b).unwrap();
        assert_eq!(n, 42);
    }

    #[test]
    fn bytes_to_u32() {
        let b = Bytes::from("100");
        let n: u32 = FromRedisBytes::from_redis_bytes(b).unwrap();
        assert_eq!(n, 100);
    }

    #[test]
    fn bytes_to_f64() {
        let b = Bytes::from("2.5");
        let n: f64 = FromRedisBytes::from_redis_bytes(b).unwrap();
        assert!((n - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn bytes_to_bool() {
        assert!(bool::from_redis_bytes(Bytes::from("1")).unwrap());
        assert!(!bool::from_redis_bytes(Bytes::from("0")).unwrap());
        assert!(bool::from_redis_bytes(Bytes::from("true")).unwrap());
        assert!(!bool::from_redis_bytes(Bytes::from("false")).unwrap());
    }

    #[test]
    fn invalid_utf8_string() {
        let b = Bytes::from(vec![0xff, 0xfe]);
        assert!(String::from_redis_bytes(b).is_err());
    }

    #[test]
    fn invalid_number() {
        let b = Bytes::from("not_a_number");
        assert!(i64::from_redis_bytes(b).is_err());
    }

    // -- RedisConvert from Option<Bytes> tests --

    #[test]
    fn option_bytes_some_to_string() {
        let v: Option<Bytes> = Some(Bytes::from("hello"));
        let s: String = v.parse_into().unwrap();
        assert_eq!(s, "hello");
    }

    #[test]
    fn option_bytes_none_to_string_fails() {
        let v: Option<Bytes> = None;
        assert!(v.parse_into::<String>().is_err());
    }

    #[test]
    fn option_bytes_none_to_option_string() {
        let v: Option<Bytes> = None;
        let s: Option<String> = v.parse_into().unwrap();
        assert_eq!(s, None);
    }

    #[test]
    fn option_bytes_some_to_option_string() {
        let v: Option<Bytes> = Some(Bytes::from("hello"));
        let s: Option<String> = v.parse_into().unwrap();
        assert_eq!(s, Some("hello".to_string()));
    }

    #[test]
    fn option_bytes_to_u32() {
        let v: Option<Bytes> = Some(Bytes::from("42"));
        let n: u32 = v.parse_into().unwrap();
        assert_eq!(n, 42);
    }

    // -- RedisConvert from i64 tests --

    #[test]
    fn i64_to_u32() {
        let n: u32 = 42i64.parse_into().unwrap();
        assert_eq!(n, 42);
    }

    #[test]
    fn i64_negative_to_u32_fails() {
        assert!((-1i64).parse_into::<u32>().is_err());
    }

    #[test]
    fn i64_to_bool() {
        assert!(1i64.parse_into::<bool>().unwrap());
        assert!(!0i64.parse_into::<bool>().unwrap());
    }

    #[test]
    fn i64_to_string() {
        let s: String = 42i64.parse_into().unwrap();
        assert_eq!(s, "42");
    }

    // -- RedisConvert from f64 tests --

    #[test]
    fn f64_to_f32() {
        let n: f32 = 2.5f64.parse_into().unwrap();
        assert!((n - 2.5).abs() < f32::EPSILON);
    }

    // -- RedisConvert from Vec tests --

    #[test]
    fn vec_bytes_to_vec_string() {
        let v = vec![Bytes::from("a"), Bytes::from("b")];
        let s: Vec<String> = v.parse_into().unwrap();
        assert_eq!(s, vec!["a", "b"]);
    }

    #[test]
    fn vec_pairs_to_hashmap() {
        let v = vec![
            (Bytes::from("k1"), Bytes::from("v1")),
            (Bytes::from("k2"), Bytes::from("v2")),
        ];
        let m: std::collections::HashMap<String, String> = v.parse_into().unwrap();
        assert_eq!(m.get("k1").unwrap(), "v1");
        assert_eq!(m.get("k2").unwrap(), "v2");
    }

    // -- Blanket impl tests --

    #[test]
    fn vec_bytes_to_vec_u32() {
        // blanket impl: Vec<u32> via FromRedisBytes
        let v = vec![Bytes::from("1"), Bytes::from("2"), Bytes::from("42")];
        let ns: Vec<u32> = v.parse_into().unwrap();
        assert_eq!(ns, vec![1u32, 2, 42]);
    }

    #[test]
    fn vec_bytes_to_vec_bytes_blanket() {
        // blanket impl also covers Vec<Bytes> (Bytes: FromRedisBytes)
        let v = vec![Bytes::from("a"), Bytes::from("b")];
        let r: Vec<Bytes> = v.parse_into().unwrap();
        assert_eq!(r, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    #[test]
    fn vec_pairs_to_hashmap_f64() {
        // blanket HashMap<String, V> impl: V=f64
        let v = vec![
            (Bytes::from("score1"), Bytes::from("1.5")),
            (Bytes::from("score2"), Bytes::from("2.5")),
        ];
        let m: std::collections::HashMap<String, f64> = v.parse_into().unwrap();
        assert!((m["score1"] - 1.5).abs() < f64::EPSILON);
        assert!((m["score2"] - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn vec_pairs_to_hashmap_u64() {
        // blanket HashMap<String, V> impl: V=u64
        let v = vec![(Bytes::from("count"), Bytes::from("42"))];
        let m: std::collections::HashMap<String, u64> = v.parse_into().unwrap();
        assert_eq!(m["count"], 42u64);
    }

    #[test]
    fn vec_pairs_to_hashmap_invalid_value_fails() {
        // blanket HashMap<String, V> impl: invalid V parse returns error
        let v = vec![(Bytes::from("key"), Bytes::from("not_a_number"))];
        let r: Result<std::collections::HashMap<String, u32>, _> = v.parse_into();
        assert!(r.is_err());
    }

    // -- Edge cases: numeric bounds --

    #[test]
    fn bytes_to_i64_max() {
        let b = Bytes::from(i64::MAX.to_string());
        let n: i64 = FromRedisBytes::from_redis_bytes(b).unwrap();
        assert_eq!(n, i64::MAX);
    }

    #[test]
    fn bytes_to_i64_min() {
        let b = Bytes::from(i64::MIN.to_string());
        let n: i64 = FromRedisBytes::from_redis_bytes(b).unwrap();
        assert_eq!(n, i64::MIN);
    }

    #[test]
    fn i64_max_to_u32_fails() {
        // i64::MAX overflows u32
        assert!(i64::MAX.parse_into::<u32>().is_err());
    }

    #[test]
    fn i64_max_to_i64_succeeds() {
        let n: i64 = i64::MAX.parse_into().unwrap();
        assert_eq!(n, i64::MAX);
    }

    #[test]
    fn i64_min_to_i64_succeeds() {
        let n: i64 = i64::MIN.parse_into().unwrap();
        assert_eq!(n, i64::MIN);
    }

    #[test]
    fn large_i64_fits_u64() {
        // Value larger than u32::MAX but fits u64
        let val: i64 = (u32::MAX as i64) + 1;
        let n: u64 = val.parse_into().unwrap();
        assert_eq!(n, (u32::MAX as u64) + 1);
    }

    // -- Edge cases: empty and special strings --

    #[test]
    fn empty_bytes_to_string_succeeds() {
        let b = Bytes::from("");
        let s: String = FromRedisBytes::from_redis_bytes(b).unwrap();
        assert_eq!(s, "");
    }

    #[test]
    fn empty_bytes_to_i64_fails() {
        let b = Bytes::from("");
        assert!(i64::from_redis_bytes(b).is_err());
    }

    #[test]
    fn bytes_to_bool_invalid_value() {
        let b = Bytes::from("yes");
        assert!(bool::from_redis_bytes(b).is_err());
    }

    #[test]
    fn bytes_to_bool_true_uppercase() {
        assert!(bool::from_redis_bytes(Bytes::from("TRUE")).unwrap());
    }

    #[test]
    fn bytes_to_bool_false_uppercase() {
        assert!(!bool::from_redis_bytes(Bytes::from("FALSE")).unwrap());
    }

    // -- Edge cases: Vec conversions --

    #[test]
    fn vec_bytes_with_invalid_utf8_fails() {
        let v = vec![Bytes::from("valid"), Bytes::from(vec![0xff, 0xfe])];
        let result: Result<Vec<String>, _> = v.parse_into();
        assert!(result.is_err());
    }

    #[test]
    fn empty_vec_bytes_to_vec_string() {
        let v: Vec<Bytes> = vec![];
        let s: Vec<String> = v.parse_into().unwrap();
        assert!(s.is_empty());
    }

    #[test]
    fn vec_option_bytes_mixed_some_none() {
        let v: Vec<Option<Bytes>> =
            vec![Some(Bytes::from("hello")), None, Some(Bytes::from("world"))];
        let s: Vec<Option<String>> = v.parse_into().unwrap();
        assert_eq!(s[0], Some("hello".to_string()));
        assert_eq!(s[1], None);
        assert_eq!(s[2], Some("world".to_string()));
    }

    #[test]
    fn vec_option_bytes_with_invalid_utf8_fails() {
        let v: Vec<Option<Bytes>> = vec![Some(Bytes::from(vec![0xff]))];
        let result: Result<Vec<Option<String>>, _> = v.parse_into();
        assert!(result.is_err());
    }

    #[test]
    fn vec_pairs_with_invalid_utf8_key_fails() {
        let v = vec![(Bytes::from(vec![0xff]), Bytes::from("v"))];
        let result: Result<Vec<(String, String)>, _> = v.parse_into();
        assert!(result.is_err());
    }

    #[test]
    fn vec_pairs_with_invalid_utf8_value_fails() {
        let v = vec![(Bytes::from("k"), Bytes::from(vec![0xff]))];
        let result: Result<Vec<(String, String)>, _> = v.parse_into();
        assert!(result.is_err());
    }

    // -- Edge cases: i64 to other numeric types --

    #[test]
    fn i64_to_f64_preserves_value() {
        let n: f64 = 42i64.parse_into().unwrap();
        assert!((n - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn i64_to_f32() {
        let n: f32 = 42i64.parse_into().unwrap();
        assert!((n - 42.0).abs() < f32::EPSILON);
    }

    #[test]
    fn i64_zero_to_bool_is_false() {
        let b: bool = 0i64.parse_into().unwrap();
        assert!(!b);
    }

    #[test]
    fn i64_negative_to_bool_is_true() {
        let b: bool = (-1i64).parse_into().unwrap();
        assert!(b);
    }

    // -- Edge cases: f64 conversions --

    #[test]
    fn f64_to_string() {
        let s: String = 1.5f64.parse_into().unwrap();
        assert_eq!(s, "1.5");
    }

    #[test]
    fn f64_identity() {
        let n: f64 = 1.5f64.parse_into().unwrap();
        assert!((n - 1.5).abs() < f64::EPSILON);
    }

    // -- Edge cases: bool conversions --

    #[test]
    fn bool_to_i64() {
        let n: i64 = true.parse_into().unwrap();
        assert_eq!(n, 1);
        let n: i64 = false.parse_into().unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn bool_to_string() {
        let s: String = true.parse_into().unwrap();
        assert_eq!(s, "true");
        let s: String = false.parse_into().unwrap();
        assert_eq!(s, "false");
    }

    #[test]
    fn bool_identity() {
        let b: bool = true.parse_into().unwrap();
        assert!(b);
    }

    // -- Edge cases: Option<Bytes> with numeric parsing --

    #[test]
    fn option_bytes_none_to_option_i64() {
        let v: Option<Bytes> = None;
        let n: Option<i64> = v.parse_into().unwrap();
        assert_eq!(n, None);
    }

    #[test]
    fn option_bytes_some_to_option_i64() {
        let v: Option<Bytes> = Some(Bytes::from("99"));
        let n: Option<i64> = v.parse_into().unwrap();
        assert_eq!(n, Some(99));
    }

    // -- Edge cases: Bytes direct conversions --

    #[test]
    fn bytes_direct_to_string() {
        let b = Bytes::from("direct");
        let s: String = b.parse_into().unwrap();
        assert_eq!(s, "direct");
    }

    #[test]
    fn bytes_direct_to_bytes_identity() {
        let b = Bytes::from("data");
        let r: Bytes = b.parse_into().unwrap();
        assert_eq!(r, Bytes::from("data"));
    }
}
