//! Typed decoding of raw RESP frames into Rust types.
//!
//! [`RedisConvert`](crate::RedisConvert) converts the *already-typed* response a
//! command produces (`Option<Bytes>`, `i64`, ...). [`FromFrame`] works one level
//! lower: it decodes a raw [`Frame`] straight into a Rust type, which is what
//! dynamic callers need when they run an arbitrary command via
//! [`RawCommand`](https://docs.rs/redis-tower-commands) and get back a `Frame`
//! with no typed response shape attached.
//!
//! Impls are provided for the common scalar types, `Bytes`, `Option<T>`,
//! `Vec<T>`, fixed-size tuples, and `HashMap<K, V>`, covering the bulk of the
//! reply shapes a CLI or tooling layer decodes. `Frame` implements `FromFrame`
//! as an identity, so `query::<Frame>()` is the raw passthrough.
//!
//! # Example
//!
//! Decoding the frames a dynamic command would return. `RawCommand::query::<T>()`
//! calls `T::from_frame` on the reply, so these are the shapes it handles.
//!
//! ```no_run
//! use std::collections::HashMap;
//!
//! use redis_tower_core::FromFrame;
//! use redis_tower_protocol::Frame;
//! use redis_tower_protocol::helpers::{array, bulk};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Scalar -- the INCR reply
//! let n = i64::from_frame(Frame::Integer(1))?;
//!
//! // Array -- the SMEMBERS reply
//! let members = Vec::<String>::from_frame(array(vec![bulk("a"), bulk("b")]))?;
//!
//! // Map (RESP3 map or RESP2 flat array) -- the HGETALL reply
//! let fields = HashMap::<String, String>::from_frame(array(vec![bulk("f"), bulk("v")]))?;
//! # let _ = (n, members, fields);
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::hash::Hash;

use bytes::Bytes;

use crate::Frame;
use crate::error::RedisError;

/// Decode a raw RESP [`Frame`] into a Rust type.
///
/// Implement this trait for custom types to enable
/// [`RawCommand::query::<T>()`](https://docs.rs/redis-tower-commands) typed
/// decoding of dynamic commands.
pub trait FromFrame: Sized {
    /// Decode a response frame into this type.
    ///
    /// Error frames (`-ERR ...` / blob errors) are surfaced as
    /// [`RedisError::Redis`] before any type matching, so every impl can assume
    /// a non-error frame.
    fn from_frame(frame: Frame) -> Result<Self, RedisError>;
}

/// Reject error frames up front so each impl only handles value frames.
fn check_error(frame: Frame) -> Result<Frame, RedisError> {
    match frame {
        Frame::Error(b) | Frame::BlobError(b) => {
            Err(RedisError::Redis(String::from_utf8_lossy(&b).into_owned()))
        }
        other => Ok(other),
    }
}

fn unexpected(expected: &'static str, frame: &Frame) -> RedisError {
    RedisError::UnexpectedResponse {
        expected,
        actual: format!("{frame:?}"),
    }
}

/// Extract the bytes from a string-like frame (simple/bulk/verbatim/big-number).
fn frame_to_bytes(frame: Frame) -> Result<Bytes, RedisError> {
    match frame {
        Frame::SimpleString(b) | Frame::BigNumber(b) => Ok(b),
        Frame::BulkString(Some(b)) => Ok(b),
        Frame::VerbatimString(_, b) => Ok(b),
        other => Err(unexpected("string-like frame", &other)),
    }
}

/// Extract the element vector from an array-like frame.
fn frame_to_vec(frame: Frame) -> Result<Vec<Frame>, RedisError> {
    match frame {
        Frame::Array(Some(v)) => Ok(v),
        Frame::Array(None) => Ok(Vec::new()),
        Frame::Set(v)
        | Frame::Push(v)
        | Frame::StreamedArray(v)
        | Frame::StreamedSet(v)
        | Frame::StreamedPush(v) => Ok(v),
        other => Err(unexpected("array-like frame", &other)),
    }
}

// -- Identity --

impl FromFrame for Frame {
    fn from_frame(frame: Frame) -> Result<Self, RedisError> {
        Ok(frame)
    }
}

// -- String-like scalars --

impl FromFrame for Bytes {
    fn from_frame(frame: Frame) -> Result<Self, RedisError> {
        frame_to_bytes(check_error(frame)?)
    }
}

// Note: there is intentionally no dedicated `Vec<u8>` impl. Raw bulk-string
// bytes decode to `Bytes` (use `.to_vec()` for an owned `Vec<u8>`); `Vec<u8>`
// itself is handled by the generic `Vec<T>` impl as an array of `u8` elements.

impl FromFrame for String {
    fn from_frame(frame: Frame) -> Result<Self, RedisError> {
        let b = frame_to_bytes(check_error(frame)?)?;
        String::from_utf8(b.to_vec()).map_err(|_| RedisError::TypeMismatch {
            expected: "valid UTF-8 string",
        })
    }
}

// -- Integer scalars --

macro_rules! impl_from_frame_int {
    ($($t:ty),+) => {
        $(
            impl FromFrame for $t {
                fn from_frame(frame: Frame) -> Result<Self, RedisError> {
                    match check_error(frame)? {
                        Frame::Integer(i) => <$t>::try_from(i).map_err(|_| {
                            RedisError::TypeMismatch {
                                expected: concat!(stringify!($t), " (value out of range)"),
                            }
                        }),
                        other => {
                            let b = frame_to_bytes(other)?;
                            let s = std::str::from_utf8(&b).map_err(|_| {
                                RedisError::TypeMismatch {
                                    expected: concat!("parseable as ", stringify!($t)),
                                }
                            })?;
                            s.trim().parse::<$t>().map_err(|_| RedisError::TypeMismatch {
                                expected: concat!("parseable as ", stringify!($t)),
                            })
                        }
                    }
                }
            }
        )+
    };
}

impl_from_frame_int!(
    i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, isize, usize
);

// -- Float scalars --

fn parse_float<T: std::str::FromStr>(b: &[u8]) -> Result<T, RedisError> {
    let s = std::str::from_utf8(b).map_err(|_| RedisError::TypeMismatch {
        expected: "parseable as float",
    })?;
    s.trim().parse::<T>().map_err(|_| RedisError::TypeMismatch {
        expected: "parseable as float",
    })
}

impl FromFrame for f64 {
    fn from_frame(frame: Frame) -> Result<Self, RedisError> {
        match check_error(frame)? {
            Frame::Double(d) => Ok(d),
            Frame::Integer(i) => Ok(i as f64),
            Frame::SpecialFloat(b) => parse_float::<f64>(&b),
            other => parse_float::<f64>(&frame_to_bytes(other)?),
        }
    }
}

impl FromFrame for f32 {
    fn from_frame(frame: Frame) -> Result<Self, RedisError> {
        match check_error(frame)? {
            Frame::Double(d) => Ok(d as f32),
            Frame::Integer(i) => Ok(i as f32),
            Frame::SpecialFloat(b) => parse_float::<f32>(&b),
            other => parse_float::<f32>(&frame_to_bytes(other)?),
        }
    }
}

// -- Boolean --

impl FromFrame for bool {
    fn from_frame(frame: Frame) -> Result<Self, RedisError> {
        match check_error(frame)? {
            Frame::Boolean(b) => Ok(b),
            Frame::Integer(i) => Ok(i != 0),
            other => match frame_to_bytes(other)?.as_ref() {
                b"1" | b"true" | b"TRUE" => Ok(true),
                b"0" | b"false" | b"FALSE" => Ok(false),
                _ => Err(RedisError::TypeMismatch {
                    expected: "boolean (1/0 or true/false)",
                }),
            },
        }
    }
}

// -- Option --

impl<T: FromFrame> FromFrame for Option<T> {
    fn from_frame(frame: Frame) -> Result<Self, RedisError> {
        match check_error(frame)? {
            Frame::Null | Frame::BulkString(None) | Frame::Array(None) => Ok(None),
            other => T::from_frame(other).map(Some),
        }
    }
}

// -- Vec --

impl<T: FromFrame> FromFrame for Vec<T> {
    fn from_frame(frame: Frame) -> Result<Self, RedisError> {
        frame_to_vec(check_error(frame)?)?
            .into_iter()
            .map(T::from_frame)
            .collect()
    }
}

// -- HashMap --

impl<K, V> FromFrame for HashMap<K, V>
where
    K: FromFrame + Eq + Hash,
    V: FromFrame,
{
    fn from_frame(frame: Frame) -> Result<Self, RedisError> {
        let pairs = match check_error(frame)? {
            Frame::Map(pairs) | Frame::StreamedMap(pairs) | Frame::Attribute(pairs) => pairs,
            Frame::Array(None) => Vec::new(),
            // RESP2 returns maps as a flat array [k, v, k, v, ...].
            Frame::Array(Some(flat)) => {
                if flat.len() % 2 != 0 {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "flat array with an even number of elements",
                        actual: format!("array of {} elements", flat.len()),
                    });
                }
                let mut it = flat.into_iter();
                let mut pairs = Vec::with_capacity(it.len() / 2);
                while let (Some(k), Some(v)) = (it.next(), it.next()) {
                    pairs.push((k, v));
                }
                pairs
            }
            other => return Err(unexpected("map or flat array", &other)),
        };
        pairs
            .into_iter()
            .map(|(k, v)| Ok((K::from_frame(k)?, V::from_frame(v)?)))
            .collect()
    }
}

// -- Tuples --

macro_rules! impl_from_frame_tuple {
    ($len:expr; $($name:ident),+) => {
        impl<$($name: FromFrame),+> FromFrame for ($($name,)+) {
            fn from_frame(frame: Frame) -> Result<Self, RedisError> {
                let items = frame_to_vec(check_error(frame)?)?;
                if items.len() != $len {
                    return Err(RedisError::UnexpectedResponse {
                        expected: concat!("array of ", stringify!($len), " elements"),
                        actual: format!("array of {} elements", items.len()),
                    });
                }
                let mut it = items.into_iter();
                Ok((
                    $( $name::from_frame(it.next().expect("length checked above"))?, )+
                ))
            }
        }
    };
}

impl_from_frame_tuple!(1; A);
impl_from_frame_tuple!(2; A, B);
impl_from_frame_tuple!(3; A, B, C);
impl_from_frame_tuple!(4; A, B, C, D);
impl_from_frame_tuple!(5; A, B, C, D, E);
impl_from_frame_tuple!(6; A, B, C, D, E, F);

#[cfg(test)]
mod tests {
    use super::*;

    // -- Identity --

    #[test]
    fn frame_identity() {
        let f = Frame::Integer(7);
        let out: Frame = FromFrame::from_frame(f.clone()).unwrap();
        assert_eq!(out, f);
    }

    #[test]
    fn frame_identity_preserves_errors() {
        // The identity impl is a raw passthrough and does not reject errors.
        let f = Frame::Error(Bytes::from("ERR boom"));
        let out: Frame = FromFrame::from_frame(f.clone()).unwrap();
        assert_eq!(out, f);
    }

    // -- Error frames --

    #[test]
    fn error_frame_maps_to_redis_error() {
        let f = Frame::Error(Bytes::from("ERR wrong type"));
        let r: Result<String, _> = FromFrame::from_frame(f);
        match r {
            Err(RedisError::Redis(m)) => assert_eq!(m, "ERR wrong type"),
            other => panic!("expected Redis error, got {other:?}"),
        }
    }

    #[test]
    fn blob_error_maps_to_redis_error() {
        let f = Frame::BlobError(Bytes::from("SYNTAX bad"));
        let r: Result<i64, _> = FromFrame::from_frame(f);
        assert!(matches!(r, Err(RedisError::Redis(_))));
    }

    // -- String-like scalars --

    #[test]
    fn string_from_bulk() {
        let s: String = FromFrame::from_frame(Frame::BulkString(Some(Bytes::from("hi")))).unwrap();
        assert_eq!(s, "hi");
    }

    #[test]
    fn string_from_simple() {
        let s: String = FromFrame::from_frame(Frame::SimpleString(Bytes::from("OK"))).unwrap();
        assert_eq!(s, "OK");
    }

    #[test]
    fn string_from_verbatim() {
        let f = Frame::VerbatimString(Bytes::from("txt"), Bytes::from("hello"));
        let s: String = FromFrame::from_frame(f).unwrap();
        assert_eq!(s, "hello");
    }

    #[test]
    fn bytes_from_bulk() {
        let b: Bytes = FromFrame::from_frame(Frame::BulkString(Some(Bytes::from("raw")))).unwrap();
        assert_eq!(b, Bytes::from("raw"));
    }

    #[test]
    fn vec_u8_from_integer_array() {
        // Vec<u8> is an array of u8 elements (the generic Vec<T> impl), not raw
        // bulk-string bytes -- use Bytes for that.
        let f = Frame::Array(Some(vec![Frame::Integer(1), Frame::Integer(2)]));
        let v: Vec<u8> = FromFrame::from_frame(f).unwrap();
        assert_eq!(v, vec![1u8, 2]);
    }

    #[test]
    fn string_invalid_utf8_fails() {
        let f = Frame::BulkString(Some(Bytes::from(vec![0xff, 0xfe])));
        let r: Result<String, _> = FromFrame::from_frame(f);
        assert!(matches!(r, Err(RedisError::TypeMismatch { .. })));
    }

    #[test]
    fn string_from_integer_fails() {
        let r: Result<String, _> = FromFrame::from_frame(Frame::Integer(1));
        assert!(matches!(r, Err(RedisError::UnexpectedResponse { .. })));
    }

    // -- Integer scalars --

    #[test]
    fn i64_from_integer() {
        let n: i64 = FromFrame::from_frame(Frame::Integer(42)).unwrap();
        assert_eq!(n, 42);
    }

    #[test]
    fn u32_from_integer() {
        let n: u32 = FromFrame::from_frame(Frame::Integer(7)).unwrap();
        assert_eq!(n, 7);
    }

    #[test]
    fn u32_from_negative_integer_fails() {
        let r: Result<u32, _> = FromFrame::from_frame(Frame::Integer(-1));
        assert!(matches!(r, Err(RedisError::TypeMismatch { .. })));
    }

    #[test]
    fn i64_from_bulk_string() {
        // Redis often returns numbers as bulk strings.
        let n: i64 = FromFrame::from_frame(Frame::BulkString(Some(Bytes::from("123")))).unwrap();
        assert_eq!(n, 123);
    }

    #[test]
    fn i64_from_unparseable_string_fails() {
        let f = Frame::BulkString(Some(Bytes::from("nope")));
        let r: Result<i64, _> = FromFrame::from_frame(f);
        assert!(matches!(r, Err(RedisError::TypeMismatch { .. })));
    }

    // -- Float scalars --

    #[test]
    fn f64_from_double() {
        let n: f64 = FromFrame::from_frame(Frame::Double(2.5)).unwrap();
        assert!((n - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn f64_from_integer() {
        let n: f64 = FromFrame::from_frame(Frame::Integer(3)).unwrap();
        assert!((n - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn f64_from_bulk_string() {
        let n: f64 = FromFrame::from_frame(Frame::BulkString(Some(Bytes::from("1.25")))).unwrap();
        assert!((n - 1.25).abs() < 1e-9);
    }

    #[test]
    fn f64_from_special_float_inf() {
        let n: f64 = FromFrame::from_frame(Frame::SpecialFloat(Bytes::from("inf"))).unwrap();
        assert!(n.is_infinite() && n.is_sign_positive());
    }

    #[test]
    fn f32_from_double() {
        let n: f32 = FromFrame::from_frame(Frame::Double(1.5)).unwrap();
        assert!((n - 1.5).abs() < f32::EPSILON);
    }

    // -- Boolean --

    #[test]
    fn bool_from_boolean() {
        let b: bool = FromFrame::from_frame(Frame::Boolean(true)).unwrap();
        assert!(b);
    }

    #[test]
    fn bool_from_integer() {
        assert!(<bool as FromFrame>::from_frame(Frame::Integer(1)).unwrap());
        assert!(!<bool as FromFrame>::from_frame(Frame::Integer(0)).unwrap());
    }

    #[test]
    fn bool_from_string() {
        let f = Frame::BulkString(Some(Bytes::from("true")));
        assert!(<bool as FromFrame>::from_frame(f).unwrap());
    }

    #[test]
    fn bool_from_invalid_string_fails() {
        let f = Frame::BulkString(Some(Bytes::from("maybe")));
        let r: Result<bool, _> = FromFrame::from_frame(f);
        assert!(matches!(r, Err(RedisError::TypeMismatch { .. })));
    }

    // -- Option --

    #[test]
    fn option_from_null() {
        let v: Option<String> = FromFrame::from_frame(Frame::Null).unwrap();
        assert_eq!(v, None);
    }

    #[test]
    fn option_from_nil_bulk() {
        let v: Option<String> = FromFrame::from_frame(Frame::BulkString(None)).unwrap();
        assert_eq!(v, None);
    }

    #[test]
    fn option_from_nil_array() {
        let v: Option<Vec<String>> = FromFrame::from_frame(Frame::Array(None)).unwrap();
        assert_eq!(v, None);
    }

    #[test]
    fn option_some() {
        let f = Frame::BulkString(Some(Bytes::from("x")));
        let v: Option<String> = FromFrame::from_frame(f).unwrap();
        assert_eq!(v, Some("x".to_string()));
    }

    // -- Vec --

    #[test]
    fn vec_string_from_array() {
        let f = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::BulkString(Some(Bytes::from("b"))),
        ]));
        let v: Vec<String> = FromFrame::from_frame(f).unwrap();
        assert_eq!(v, vec!["a", "b"]);
    }

    #[test]
    fn vec_from_set() {
        let f = Frame::Set(vec![Frame::Integer(1), Frame::Integer(2)]);
        let v: Vec<i64> = FromFrame::from_frame(f).unwrap();
        assert_eq!(v, vec![1, 2]);
    }

    #[test]
    fn vec_from_empty_array() {
        let v: Vec<String> = FromFrame::from_frame(Frame::Array(None)).unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn vec_of_options_mixed() {
        let f = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::Null,
        ]));
        let v: Vec<Option<String>> = FromFrame::from_frame(f).unwrap();
        assert_eq!(v, vec![Some("a".to_string()), None]);
    }

    #[test]
    fn vec_from_non_array_fails() {
        let r: Result<Vec<String>, _> = FromFrame::from_frame(Frame::Integer(1));
        assert!(matches!(r, Err(RedisError::UnexpectedResponse { .. })));
    }

    // -- HashMap --

    #[test]
    fn hashmap_from_map() {
        let f = Frame::Map(vec![
            (
                Frame::BulkString(Some(Bytes::from("k1"))),
                Frame::BulkString(Some(Bytes::from("v1"))),
            ),
            (
                Frame::BulkString(Some(Bytes::from("k2"))),
                Frame::BulkString(Some(Bytes::from("v2"))),
            ),
        ]);
        let m: HashMap<String, String> = FromFrame::from_frame(f).unwrap();
        assert_eq!(m.get("k1").unwrap(), "v1");
        assert_eq!(m.get("k2").unwrap(), "v2");
    }

    #[test]
    fn hashmap_from_flat_array() {
        // RESP2 shape: HGETALL returns a flat array.
        let f = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("field"))),
            Frame::BulkString(Some(Bytes::from("42"))),
        ]));
        let m: HashMap<String, i64> = FromFrame::from_frame(f).unwrap();
        assert_eq!(m.get("field").copied(), Some(42));
    }

    #[test]
    fn hashmap_from_odd_flat_array_fails() {
        let f = Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from("lonely")))]));
        let r: Result<HashMap<String, String>, _> = FromFrame::from_frame(f);
        assert!(matches!(r, Err(RedisError::UnexpectedResponse { .. })));
    }

    #[test]
    fn hashmap_from_empty_array() {
        let m: HashMap<String, String> = FromFrame::from_frame(Frame::Array(None)).unwrap();
        assert!(m.is_empty());
    }

    // -- Tuples --

    #[test]
    fn tuple_pair() {
        let f = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("name"))),
            Frame::Integer(30),
        ]));
        let (name, age): (String, i64) = FromFrame::from_frame(f).unwrap();
        assert_eq!(name, "name");
        assert_eq!(age, 30);
    }

    #[test]
    fn tuple_triple_mixed() {
        let f = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::Integer(1),
            Frame::Double(2.5),
        ]));
        let (a, b, c): (String, i64, f64) = FromFrame::from_frame(f).unwrap();
        assert_eq!(a, "a");
        assert_eq!(b, 1);
        assert!((c - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn tuple_wrong_arity_fails() {
        let f = Frame::Array(Some(vec![Frame::Integer(1)]));
        let r: Result<(i64, i64), _> = FromFrame::from_frame(f);
        assert!(matches!(r, Err(RedisError::UnexpectedResponse { .. })));
    }

    // -- Nested --

    #[test]
    fn nested_vec_of_pairs() {
        let f = Frame::Array(Some(vec![
            Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from("a"))),
                Frame::Integer(1),
            ])),
            Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from("b"))),
                Frame::Integer(2),
            ])),
        ]));
        let v: Vec<(String, i64)> = FromFrame::from_frame(f).unwrap();
        assert_eq!(v, vec![("a".to_string(), 1), ("b".to_string(), 2)]);
    }
}
