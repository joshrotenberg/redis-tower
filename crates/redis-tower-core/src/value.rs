//! Traits for converting Redis response values into Rust types.
//!
//! Commands return raw types like `Option<Bytes>` or `i64`. These traits
//! provide ergonomic conversion to user types.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::{RedisConnection, RedisValueExt};
//! use redis_tower::commands::*;
//!
//! let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! conn.execute(Set::new("name", "alice")).await?;
//!
//! // Convert Option<Bytes> to String
//! let name: String = conn.execute(Get::new("name")).await?.parse_into()?;
//!
//! // Convert Option<Bytes> to integer (Redis stores numbers as strings)
//! conn.execute(Set::new("counter", "42")).await?;
//! let count: u32 = conn.execute(Get::new("counter")).await?.parse_into()?;
//!
//! // Handle missing keys with Option
//! let missing: Option<String> = conn.execute(Get::new("nope")).await?.parse_into()?;
//! assert_eq!(missing, None);
//! ```

use bytes::Bytes;

use crate::error::RedisError;

/// Convert a Redis bulk string (Bytes) into a Rust type.
///
/// Implement this trait for your own types to enable `.parse_into::<T>()`
/// on command responses that return `Option<Bytes>` or `Bytes`.
pub trait FromRedisBytes: Sized {
    fn from_redis_bytes(bytes: Bytes) -> Result<Self, RedisError>;
}

/// Extension trait for converting Redis response values to typed results.
///
/// Provides `.parse_into::<T>()` on common response types.
pub trait RedisValueExt<T> {
    fn parse_into<U>(self) -> Result<U, RedisError>
    where
        U: RedisConvert<T>;
}

/// Trait for converting from a Redis response type to a user type.
///
/// This is the core conversion trait. `T` is the response type from a command
/// (`Option<Bytes>`, `i64`, `f64`, `Bytes`, `Vec<Bytes>`, etc.).
pub trait RedisConvert<From>: Sized {
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

impl_redis_convert_from_i64!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, isize, usize);

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

impl RedisConvert<Vec<Bytes>> for Vec<Bytes> {
    fn redis_convert(value: Vec<Bytes>) -> Result<Self, RedisError> {
        Ok(value)
    }
}

impl RedisConvert<Vec<Bytes>> for Vec<String> {
    fn redis_convert(value: Vec<Bytes>) -> Result<Self, RedisError> {
        value
            .into_iter()
            .map(|b| {
                String::from_utf8(b.to_vec()).map_err(|_| RedisError::TypeMismatch {
                    expected: "valid UTF-8 string",
                })
            })
            .collect()
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

impl RedisConvert<Vec<(Bytes, Bytes)>> for std::collections::HashMap<String, String> {
    fn redis_convert(value: Vec<(Bytes, Bytes)>) -> Result<Self, RedisError> {
        let pairs: Vec<(String, String)> = RedisConvert::redis_convert(value)?;
        Ok(pairs.into_iter().collect())
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
}
