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
//! # RESP2 and RESP3
//!
//! The same command can reply with different frame types depending on the
//! negotiated protocol, and every impl decodes both forms to the same value:
//!
//! - Strings accept RESP3 doubles (`,2.5` and `,inf`), which RESP2 sends as
//!   bulk strings. A double renders in Redis's own notation (`3`, `0.00001`,
//!   `1e+20`), so the text matches the RESP2 reply for the same value.
//! - `Vec<T>` accepts a RESP3 map, flattened to the `[k, v, k, v, ...]` order
//!   of the RESP2 array.
//! - `HashMap<K, V>` and `Vec<(K, V)>` accept a RESP3 map, a RESP3 array of
//!   `[k, v]` pairs (for example `ZRANGE ... WITHSCORES`), and the RESP2 flat
//!   `[k, v, k, v, ...]` array. An array whose every element is a two-element
//!   array is read as pairs.
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

    /// Decode the elements of an array-like reply into a `Vec<Self>`.
    ///
    /// `Vec<T>` calls this rather than decoding each element on its own, so a
    /// type can read several elements as one value. The default decodes each
    /// element with [`FromFrame::from_frame`]. Two-element tuples override it
    /// to read a RESP2 flat `[k, v, k, v, ...]` array as pairs, matching the
    /// `[[k, v], ...]` array RESP3 sends for the same reply.
    fn from_frame_elements(elements: Vec<Frame>) -> Result<Vec<Self>, RedisError> {
        elements.into_iter().map(Self::from_frame).collect()
    }

    /// Decode the entries of a RESP3 map reply into a `Vec<Self>`.
    ///
    /// The default flattens the map to `[k, v, k, v, ...]`, the order RESP2
    /// sends the same reply in, and decodes it with
    /// [`FromFrame::from_frame_elements`]. Two-element tuples override it to
    /// decode each entry as a pair.
    fn from_frame_map(entries: Vec<(Frame, Frame)>) -> Result<Vec<Self>, RedisError> {
        Self::from_frame_elements(entries.into_iter().flat_map(|(k, v)| [k, v]).collect())
    }
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
///
/// RESP3 doubles are accepted because RESP2 sends the same values as bulk
/// strings (`ZSCORE`, `INCRBYFLOAT`, `HINCRBYFLOAT`, ...).
fn frame_to_bytes(frame: Frame) -> Result<Bytes, RedisError> {
    match frame {
        Frame::SimpleString(b) | Frame::BigNumber(b) | Frame::SpecialFloat(b) => Ok(b),
        Frame::BulkString(Some(b)) => Ok(b),
        Frame::VerbatimString(_, b) => Ok(b),
        Frame::Double(d) => Ok(Bytes::from(redis_double_text(d))),
        other => Err(unexpected("string-like frame", &other)),
    }
}

/// Render a double as Redis writes it in a RESP2 reply.
///
/// This follows Redis's `d2string`: whole values within ±2^62 print as
/// integers, and everything else uses the shortest round-trip digits laid out
/// as fpconv does: plain up to six leading fractional zeros or seven trailing
/// integer zeros, scientific with a signed exponent beyond that (`0.00001`,
/// `1.5e-7`, `12345678901234567000`, `1e+20`).
fn redis_double_text(d: f64) -> String {
    if d.is_nan() {
        return "nan".to_string();
    }
    if d.is_infinite() {
        return if d > 0.0 { "inf" } else { "-inf" }.to_string();
    }
    if d == 0.0 {
        return if d.is_sign_negative() { "-0" } else { "0" }.to_string();
    }
    // Redis's `double2ll` bound: LLONG_MAX / 2.
    const INTEGER_LIMIT: f64 = (i64::MAX / 2) as f64;
    if d.fract() == 0.0 && d.abs() <= INTEGER_LIMIT {
        return (d as i64).to_string();
    }

    let (digits, last) = shortest_digits(d);
    let len = digits.len() as i32;
    let sign = if d < 0.0 { "-" } else { "" };
    let exponent = last + len - 1;
    if last >= 0 && exponent < len + 7 {
        return format!("{sign}{digits}{}", "0".repeat(last as usize));
    }
    if last < 0 && (last > -7 || exponent.abs() < 4) {
        let point = len + last;
        return if point <= 0 {
            format!("{sign}0.{}{digits}", "0".repeat(-point as usize))
        } else {
            let (whole, fraction) = digits.split_at(point as usize);
            format!("{sign}{whole}.{fraction}")
        };
    }
    let (first, rest) = digits.split_at(1);
    let point = if rest.is_empty() { "" } else { "." };
    let exponent_sign = if exponent < 0 { '-' } else { '+' };
    format!(
        "{sign}{first}{point}{rest}e{exponent_sign}{}",
        exponent.abs()
    )
}

/// The shortest round-trip decimal digits of a finite, non-zero `d`, and the
/// power of ten of the last digit: `digits * 10^last == |d|`.
fn shortest_digits(d: f64) -> (String, i32) {
    // `{:e}` writes the shortest round-trip digits as `d.ddde<exp>`.
    let scientific = format!("{:e}", d.abs());
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("LowerExp always writes an exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let exponent: i32 = exponent
        .parse()
        .expect("LowerExp writes an integer exponent");
    let last = exponent - (digits.len() as i32 - 1);
    (digits, last)
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

/// Whether `frame` is a two-element array.
fn is_pair(frame: &Frame) -> bool {
    matches!(frame, Frame::Array(Some(v)) if v.len() == 2)
}

/// Read array elements as key/value pairs.
///
/// RESP3 sends pair replies such as `ZRANGE ... WITHSCORES` as an array of
/// `[k, v]` arrays, and RESP2 as a flat `[k, v, k, v, ...]` array. An array
/// whose every element is a two-element array is read as the former.
fn elements_to_pairs(elements: Vec<Frame>) -> Result<Vec<(Frame, Frame)>, RedisError> {
    if !elements.is_empty() && elements.iter().all(is_pair) {
        return Ok(elements
            .into_iter()
            .filter_map(|pair| match pair {
                Frame::Array(Some(kv)) => <[Frame; 2]>::try_from(kv).ok().map(|[k, v]| (k, v)),
                _ => None,
            })
            .collect());
    }
    if !elements.len().is_multiple_of(2) {
        return Err(RedisError::UnexpectedResponse {
            expected: "flat array with an even number of elements, or an array of pairs",
            actual: format!("array of {} elements", elements.len()),
        });
    }
    let mut it = elements.into_iter();
    let mut pairs = Vec::with_capacity(it.len() / 2);
    while let (Some(k), Some(v)) = (it.next(), it.next()) {
        pairs.push((k, v));
    }
    Ok(pairs)
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
        match check_error(frame)? {
            Frame::Map(entries) | Frame::StreamedMap(entries) => T::from_frame_map(entries),
            other => T::from_frame_elements(frame_to_vec(other)?),
        }
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
            // RESP2 flat [k, v, k, v, ...] or RESP3 [[k, v], ...].
            Frame::Array(Some(elements)) => elements_to_pairs(elements)?,
            other => return Err(unexpected("map, flat array, or array of pairs", &other)),
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
impl_from_frame_tuple!(3; A, B, C);
impl_from_frame_tuple!(4; A, B, C, D);
impl_from_frame_tuple!(5; A, B, C, D, E);
impl_from_frame_tuple!(6; A, B, C, D, E, F);

/// Pairs also decode from the reply shapes that carry a run of pairs, so
/// `Vec<(K, V)>` reads a RESP2 flat array, a RESP3 array of pairs, and a RESP3
/// map alike.
impl<A: FromFrame, B: FromFrame> FromFrame for (A, B) {
    fn from_frame(frame: Frame) -> Result<Self, RedisError> {
        let items = frame_to_vec(check_error(frame)?)?;
        match <[Frame; 2]>::try_from(items) {
            Ok([a, b]) => Ok((A::from_frame(a)?, B::from_frame(b)?)),
            Err(items) => Err(RedisError::UnexpectedResponse {
                expected: "array of 2 elements",
                actual: format!("array of {} elements", items.len()),
            }),
        }
    }

    fn from_frame_elements(elements: Vec<Frame>) -> Result<Vec<Self>, RedisError> {
        Self::from_frame_map(elements_to_pairs(elements)?)
    }

    fn from_frame_map(entries: Vec<(Frame, Frame)>) -> Result<Vec<Self>, RedisError> {
        entries
            .into_iter()
            .map(|(a, b)| Ok((A::from_frame(a)?, B::from_frame(b)?)))
            .collect()
    }
}

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

    // -- RESP2 / RESP3 parity --
    //
    // Each test decodes the RESP2 and RESP3 forms of one reply and expects
    // the same value from both.

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Some(Bytes::from(s.to_string())))
    }

    #[test]
    fn string_from_resp2_bulk_and_resp3_double() {
        // ZSCORE: RESP2 `$3 2.5`, RESP3 `,2.5`.
        let resp2: String = FromFrame::from_frame(bulk("2.5")).unwrap();
        let resp3: String = FromFrame::from_frame(Frame::Double(2.5)).unwrap();
        assert_eq!(resp2, resp3);
        // Whole doubles render without a fractional part, like RESP2.
        let resp3: String = FromFrame::from_frame(Frame::Double(3.0)).unwrap();
        assert_eq!(resp3, "3");
    }

    #[test]
    fn string_from_resp3_double_uses_redis_notation() {
        // Pairs of the RESP2 bulk text and RESP3 double Redis 8.10.2 sent for
        // the same ZSCORE.
        for text in [
            "1e+20",
            "-1e+20",
            "1e+300",
            "1.5e+21",
            "12345678901234567000",
            "1234567890123456768",
            "123456789012345680",
            "10000000000000000",
            "0.00001",
            "-0.00001",
            "0.000002",
            "0.000123",
            "1.5e-7",
            "1.2e-6",
            "1.2345e-4",
            "5e-324",
            "1234567.125",
            "123.456",
            "0.1",
            "3",
            "-3",
            "0",
        ] {
            let resp2: String = FromFrame::from_frame(bulk(text)).unwrap();
            let resp3: String =
                FromFrame::from_frame(Frame::Double(text.parse().unwrap())).unwrap();
            assert_eq!(resp2, resp3, "{text}");
        }
    }

    #[test]
    fn string_from_resp3_special_float() {
        // ZSCORE of an infinite score: RESP2 `$3 inf`, RESP3 `,inf`.
        let resp2: String = FromFrame::from_frame(bulk("inf")).unwrap();
        let resp3: String = FromFrame::from_frame(Frame::SpecialFloat(Bytes::from("inf"))).unwrap();
        assert_eq!(resp2, resp3);
        let bytes: Bytes = FromFrame::from_frame(Frame::SpecialFloat(Bytes::from("-inf"))).unwrap();
        assert_eq!(bytes, Bytes::from("-inf"));
    }

    #[test]
    fn vec_from_resp2_flat_array_and_resp3_map() {
        // HGETALL: RESP2 flat array, RESP3 map.
        let resp2: Vec<String> =
            FromFrame::from_frame(Frame::Array(Some(vec![bulk("f"), bulk("v")]))).unwrap();
        let resp3: Vec<String> =
            FromFrame::from_frame(Frame::Map(vec![(bulk("f"), bulk("v"))])).unwrap();
        assert_eq!(resp2, vec!["f", "v"]);
        assert_eq!(resp2, resp3);
    }

    #[test]
    fn hashmap_from_resp3_nested_pairs() {
        // ZRANGE ... WITHSCORES: RESP2 flat, RESP3 array of [member, score].
        let resp2: HashMap<String, f64> = FromFrame::from_frame(Frame::Array(Some(vec![
            bulk("a"),
            bulk("1"),
            bulk("b"),
            bulk("inf"),
        ])))
        .unwrap();
        let resp3: HashMap<String, f64> = FromFrame::from_frame(Frame::Array(Some(vec![
            Frame::Array(Some(vec![bulk("a"), Frame::Double(1.0)])),
            Frame::Array(Some(vec![
                bulk("b"),
                Frame::SpecialFloat(Bytes::from("inf")),
            ])),
        ])))
        .unwrap();
        assert_eq!(resp2, resp3);
        assert_eq!(resp3.get("b").copied(), Some(f64::INFINITY));

        // A single RESP3 pair is an odd-length outer array.
        let one: HashMap<String, f64> = FromFrame::from_frame(Frame::Array(Some(vec![
            Frame::Array(Some(vec![bulk("a"), Frame::Double(1.0)])),
        ])))
        .unwrap();
        assert_eq!(one.get("a").copied(), Some(1.0));
    }

    #[test]
    fn vec_of_pairs_from_every_pair_shape() {
        let expected = vec![("a".to_string(), 1i64), ("b".to_string(), 2)];
        // RESP2 flat array.
        let flat: Vec<(String, i64)> = FromFrame::from_frame(Frame::Array(Some(vec![
            bulk("a"),
            Frame::Integer(1),
            bulk("b"),
            Frame::Integer(2),
        ])))
        .unwrap();
        // RESP3 array of pairs.
        let nested: Vec<(String, i64)> = FromFrame::from_frame(Frame::Array(Some(vec![
            Frame::Array(Some(vec![bulk("a"), Frame::Integer(1)])),
            Frame::Array(Some(vec![bulk("b"), Frame::Integer(2)])),
        ])))
        .unwrap();
        // RESP3 map.
        let map: Vec<(String, i64)> = FromFrame::from_frame(Frame::Map(vec![
            (bulk("a"), Frame::Integer(1)),
            (bulk("b"), Frame::Integer(2)),
        ]))
        .unwrap();
        assert_eq!(flat, expected);
        assert_eq!(nested, expected);
        assert_eq!(map, expected);
    }

    #[test]
    fn vec_of_pairs_from_odd_flat_array_fails() {
        let r: Result<Vec<(String, i64)>, _> =
            FromFrame::from_frame(Frame::Array(Some(vec![bulk("a")])));
        assert!(matches!(r, Err(RedisError::UnexpectedResponse { .. })));
    }
}
