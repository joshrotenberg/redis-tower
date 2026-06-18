//! Convenience constructors and formatting helpers for RESP3 frames.
//!
//! In addition to the frame constructors ([`bulk`], [`array()`], [`null_bulk`]),
//! this module provides:
//!
//! - [`display`] / [`FrameDisplay`]: a `Display` adapter that renders a frame in
//!   a `redis-cli`-style, human-readable layout.
//! - [`frame_to_json`] (behind the `serde` feature): converts a frame into a
//!   [`serde_json::Value`] for logging, inspection, or serialization.

use core::fmt;

use bytes::Bytes;

use crate::Frame;

/// Create a bulk string frame from anything that can be represented as bytes.
pub fn bulk(data: impl AsRef<[u8]>) -> Frame {
    Frame::BulkString(Some(Bytes::copy_from_slice(data.as_ref())))
}

/// Create an array frame from a vec of frames.
pub fn array(frames: Vec<Frame>) -> Frame {
    Frame::Array(Some(frames))
}

/// Create a null bulk string frame.
pub fn null_bulk() -> Frame {
    Frame::BulkString(None)
}

/// A [`Display`](fmt::Display) adapter that renders a [`Frame`] in a
/// `redis-cli`-style, human-readable layout.
///
/// Because [`Frame`] is defined in an upstream crate, the rendering lives in a
/// wrapper rather than a direct `Display` impl. Construct one with [`display`]
/// (or the tuple field directly) and format it with `{}`:
///
/// ```
/// use redis_tower_protocol::helpers::{array, bulk, display};
/// use redis_tower_protocol::Frame;
///
/// let frame = array(vec![bulk("hello"), Frame::Integer(42)]);
/// let rendered = display(&frame).to_string();
/// assert_eq!(rendered, "1) \"hello\"\n2) (integer) 42");
/// ```
pub struct FrameDisplay<'a>(pub &'a Frame);

/// Wrap a frame reference for `redis-cli`-style [`Display`](fmt::Display)
/// formatting.
///
/// See [`FrameDisplay`] for details and an example.
pub fn display(frame: &Frame) -> FrameDisplay<'_> {
    FrameDisplay(frame)
}

impl fmt::Display for FrameDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_frame(f, self.0, 0)
    }
}

/// Render a single frame, indenting continuation lines of nested collections by
/// `indent` columns so element numbering lines up like `redis-cli`.
fn fmt_frame(f: &mut fmt::Formatter<'_>, frame: &Frame, indent: usize) -> fmt::Result {
    match frame {
        Frame::SimpleString(b) => write!(f, "{}", String::from_utf8_lossy(b)),
        Frame::Error(b) | Frame::BlobError(b) => {
            write!(f, "(error) {}", String::from_utf8_lossy(b))
        }
        Frame::Integer(n) => write!(f, "(integer) {n}"),
        Frame::Double(d) => write!(f, "(double) {d}"),
        Frame::SpecialFloat(b) => write!(f, "(double) {}", String::from_utf8_lossy(b)),
        Frame::Boolean(b) => write!(f, "({b})"),
        Frame::BigNumber(b) => write!(f, "(big number) {}", String::from_utf8_lossy(b)),
        Frame::Null | Frame::BulkString(None) | Frame::Array(None) => write!(f, "(nil)"),
        Frame::BulkString(Some(b)) => write_quoted(f, b),
        Frame::VerbatimString(_, content) => write_quoted(f, content),
        Frame::Array(Some(items)) | Frame::Set(items) | Frame::Push(items) => {
            fmt_sequence(f, items, indent)
        }
        Frame::Map(pairs) | Frame::Attribute(pairs) => fmt_map(f, pairs, indent),
        // Streaming headers and chunk frames are intermediate parser artifacts
        // that should not appear in fully-parsed replies; fall back to Debug.
        other => write!(f, "{other:?}"),
    }
}

/// Render a numbered sequence (`1) ...`), indenting nested lines.
fn fmt_sequence(f: &mut fmt::Formatter<'_>, items: &[Frame], indent: usize) -> fmt::Result {
    if items.is_empty() {
        return write!(f, "(empty array)");
    }
    let width = items.len().to_string().len();
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            writeln!(f)?;
            write!(f, "{:indent$}", "")?;
        }
        let prefix = format!("{:>width$}) ", i + 1);
        write!(f, "{prefix}")?;
        fmt_frame(f, item, indent + prefix.len())?;
    }
    Ok(())
}

/// Render a map (`1# key => value`), indenting nested lines.
fn fmt_map(f: &mut fmt::Formatter<'_>, pairs: &[(Frame, Frame)], indent: usize) -> fmt::Result {
    if pairs.is_empty() {
        return write!(f, "(empty map)");
    }
    let width = pairs.len().to_string().len();
    for (i, (key, value)) in pairs.iter().enumerate() {
        if i > 0 {
            writeln!(f)?;
            write!(f, "{:indent$}", "")?;
        }
        let prefix = format!("{:>width$}# ", i + 1);
        write!(f, "{prefix}")?;
        fmt_frame(f, key, indent + prefix.len())?;
        write!(f, " => ")?;
        fmt_frame(f, value, indent + prefix.len())?;
    }
    Ok(())
}

/// Write a byte string as a double-quoted, escaped literal like `redis-cli`.
fn write_quoted(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    f.write_str("\"")?;
    for &b in bytes {
        match b {
            b'"' => f.write_str("\\\"")?,
            b'\\' => f.write_str("\\\\")?,
            b'\n' => f.write_str("\\n")?,
            b'\r' => f.write_str("\\r")?,
            b'\t' => f.write_str("\\t")?,
            0x20..=0x7e => f.write_str(core::str::from_utf8(&[b]).unwrap_or("?"))?,
            _ => write!(f, "\\x{b:02x}")?,
        }
    }
    f.write_str("\"")
}

/// Convert a [`Frame`] into a [`serde_json::Value`].
///
/// This is a best-effort, lossy mapping intended for logging, inspection, and
/// serialization rather than exact round-tripping:
///
/// - simple/bulk/verbatim strings and big numbers become JSON strings;
/// - integers, doubles, and booleans become the matching JSON scalars (a
///   non-finite double becomes `null`);
/// - arrays, sets, and pushes become JSON arrays;
/// - maps and attributes become JSON objects, with each key rendered to a
///   string;
/// - errors become `{"error": "<message>"}`;
/// - null frames (including null bulk strings and null arrays) become `null`.
///
/// Available only when the `serde` feature is enabled.
///
/// ```
/// # use redis_tower_protocol::helpers::{array, bulk, frame_to_json};
/// # use redis_tower_protocol::Frame;
/// let frame = array(vec![bulk("a"), Frame::Integer(1)]);
/// assert_eq!(frame_to_json(&frame), serde_json::json!(["a", 1]));
/// ```
#[cfg(feature = "serde")]
pub fn frame_to_json(frame: &Frame) -> serde_json::Value {
    use serde_json::{Map, Value};

    match frame {
        Frame::SimpleString(b) | Frame::BigNumber(b) => {
            Value::String(String::from_utf8_lossy(b).into_owned())
        }
        Frame::BulkString(Some(b)) => Value::String(String::from_utf8_lossy(b).into_owned()),
        Frame::VerbatimString(_, content) => {
            Value::String(String::from_utf8_lossy(content).into_owned())
        }
        Frame::Error(b) | Frame::BlobError(b) => {
            let mut map = Map::new();
            map.insert(
                "error".to_string(),
                Value::String(String::from_utf8_lossy(b).into_owned()),
            );
            Value::Object(map)
        }
        Frame::Integer(n) => Value::from(*n),
        Frame::Double(d) => serde_json::Number::from_f64(*d).map_or(Value::Null, Value::Number),
        Frame::SpecialFloat(b) => Value::String(String::from_utf8_lossy(b).into_owned()),
        Frame::Boolean(b) => Value::Bool(*b),
        Frame::Null | Frame::BulkString(None) | Frame::Array(None) => Value::Null,
        Frame::Array(Some(items)) | Frame::Set(items) | Frame::Push(items) => {
            Value::Array(items.iter().map(frame_to_json).collect())
        }
        Frame::Map(pairs) | Frame::Attribute(pairs) => {
            let mut map = Map::new();
            for (key, value) in pairs {
                map.insert(json_key(key), frame_to_json(value));
            }
            Value::Object(map)
        }
        // Streaming intermediates should not reach a fully-parsed reply.
        other => Value::String(format!("{other:?}")),
    }
}

/// Render a frame used as a map key into a JSON object key string.
#[cfg(feature = "serde")]
fn json_key(frame: &Frame) -> String {
    match frame {
        Frame::SimpleString(b) | Frame::BigNumber(b) => String::from_utf8_lossy(b).into_owned(),
        Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        Frame::VerbatimString(_, content) => String::from_utf8_lossy(content).into_owned(),
        Frame::Integer(n) => n.to_string(),
        Frame::Double(d) => d.to_string(),
        Frame::Boolean(b) => b.to_string(),
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_scalars() {
        assert_eq!(display(&Frame::Integer(42)).to_string(), "(integer) 42");
        assert_eq!(display(&Frame::Boolean(true)).to_string(), "(true)");
        assert_eq!(display(&bulk("hi")).to_string(), "\"hi\"");
        assert_eq!(display(&Frame::Null).to_string(), "(nil)");
        assert_eq!(display(&null_bulk()).to_string(), "(nil)");
        assert_eq!(
            display(&Frame::SimpleString(Bytes::from_static(b"OK"))).to_string(),
            "OK"
        );
        assert_eq!(
            display(&Frame::Error(Bytes::from_static(b"ERR nope"))).to_string(),
            "(error) ERR nope"
        );
    }

    #[test]
    fn display_quotes_and_escapes_bulk_strings() {
        let frame = bulk("a\tb\"c\\d");
        assert_eq!(display(&frame).to_string(), "\"a\\tb\\\"c\\\\d\"");
        let raw = Frame::BulkString(Some(Bytes::from_static(&[0x00, 0xff])));
        assert_eq!(display(&raw).to_string(), "\"\\x00\\xff\"");
    }

    #[test]
    fn display_numbered_array_with_nesting() {
        let frame = array(vec![
            bulk("hello"),
            Frame::Integer(42),
            array(vec![bulk("a"), bulk("b")]),
        ]);
        // Nested array lines are indented so its numbering aligns under "3) ".
        let expected = "1) \"hello\"\n2) (integer) 42\n3) 1) \"a\"\n   2) \"b\"";
        assert_eq!(display(&frame).to_string(), expected);
    }

    #[test]
    fn display_empty_collections() {
        assert_eq!(display(&array(vec![])).to_string(), "(empty array)");
        assert_eq!(display(&Frame::Map(vec![])).to_string(), "(empty map)");
    }

    #[test]
    fn display_map() {
        let frame = Frame::Map(vec![(bulk("name"), bulk("alice"))]);
        assert_eq!(display(&frame).to_string(), "1# \"name\" => \"alice\"");
    }
}

#[cfg(all(test, feature = "serde"))]
mod serde_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn frame_to_json_scalars() {
        assert_eq!(frame_to_json(&Frame::Integer(7)), json!(7));
        assert_eq!(frame_to_json(&Frame::Boolean(false)), json!(false));
        assert_eq!(frame_to_json(&bulk("hi")), json!("hi"));
        assert_eq!(frame_to_json(&Frame::Null), json!(null));
        assert_eq!(frame_to_json(&null_bulk()), json!(null));
        assert_eq!(frame_to_json(&Frame::Double(1.5)), json!(1.5));
    }

    #[test]
    fn frame_to_json_non_finite_double_is_null() {
        assert_eq!(frame_to_json(&Frame::Double(f64::NAN)), json!(null));
    }

    #[test]
    fn frame_to_json_array_and_map() {
        let frame = array(vec![bulk("a"), Frame::Integer(1)]);
        assert_eq!(frame_to_json(&frame), json!(["a", 1]));

        let map = Frame::Map(vec![
            (bulk("name"), bulk("alice")),
            (Frame::Integer(3), Frame::Boolean(true)),
        ]);
        assert_eq!(frame_to_json(&map), json!({"name": "alice", "3": true}));
    }

    #[test]
    fn frame_to_json_error() {
        let frame = Frame::Error(Bytes::from_static(b"ERR boom"));
        assert_eq!(frame_to_json(&frame), json!({"error": "ERR boom"}));
    }
}
