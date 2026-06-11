//! SCAN family commands for cursor-based iteration.

use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// Result of a SCAN command: cursor + batch of results.
#[derive(Debug, Clone)]
pub struct ScanResult<T> {
    /// The cursor to use for the next iteration. "0" means iteration is complete.
    pub cursor: String,
    /// The batch of results from this iteration.
    pub results: Vec<T>,
}

impl<T> ScanResult<T> {
    /// Returns true if the scan is complete (cursor is "0").
    pub fn is_finished(&self) -> bool {
        self.cursor == "0"
    }
}

/// SCAN cursor \[MATCH pattern\] \[COUNT count\] \[TYPE type\]
///
/// Iterates over all keys in the database.
#[derive(Clone)]
pub struct Scan {
    cursor: String,
    pattern: Option<String>,
    count: Option<u64>,
    key_type: Option<String>,
}

impl Scan {
    /// Start a new scan from cursor "0".
    pub fn new() -> Self {
        Self {
            cursor: "0".to_string(),
            pattern: None,
            count: None,
            key_type: None,
        }
    }

    /// Continue a scan from a specific cursor.
    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = cursor.into();
        self
    }

    /// Filter keys matching a glob pattern.
    pub fn match_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Hint for how many elements to return per iteration.
    pub fn count(mut self, n: u64) -> Self {
        self.count = Some(n);
        self
    }

    /// Filter by key type (string, list, set, zset, hash, stream).
    pub fn key_type(mut self, t: impl Into<String>) -> Self {
        self.key_type = Some(t.into());
        self
    }
}

impl Default for Scan {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Scan {
    type Response = ScanResult<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SCAN"), bulk(self.cursor.as_str())];
        if let Some(ref pattern) = self.pattern {
            args.push(bulk("MATCH"));
            args.push(bulk(pattern.as_str()));
        }
        if let Some(n) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(n.to_string()));
        }
        if let Some(ref t) = self.key_type {
            args.push(bulk("TYPE"));
            args.push(bulk(t.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_scan_response(frame, extract_bulk_bytes)
    }

    fn name(&self) -> &str {
        "SCAN"
    }
}

/// SSCAN key cursor \[MATCH pattern\] \[COUNT count\]
///
/// Iterates over members of a set.
#[derive(Clone)]
pub struct SScan {
    key: String,
    cursor: String,
    pattern: Option<String>,
    count: Option<u64>,
}

impl SScan {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            cursor: "0".to_string(),
            pattern: None,
            count: None,
        }
    }

    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = cursor.into();
        self
    }

    pub fn match_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    pub fn count(mut self, n: u64) -> Self {
        self.count = Some(n);
        self
    }
}

impl Command for SScan {
    type Response = ScanResult<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("SSCAN"),
            bulk(self.key.as_str()),
            bulk(self.cursor.as_str()),
        ];
        if let Some(ref pattern) = self.pattern {
            args.push(bulk("MATCH"));
            args.push(bulk(pattern.as_str()));
        }
        if let Some(n) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(n.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_scan_response(frame, extract_bulk_bytes)
    }

    fn name(&self) -> &str {
        "SSCAN"
    }
}

/// HSCAN key cursor \[MATCH pattern\] \[COUNT count\]
///
/// Iterates over fields and values of a hash. Returns (field, value) pairs.
#[derive(Clone)]
pub struct HScan {
    key: String,
    cursor: String,
    pattern: Option<String>,
    count: Option<u64>,
}

impl HScan {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            cursor: "0".to_string(),
            pattern: None,
            count: None,
        }
    }

    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = cursor.into();
        self
    }

    pub fn match_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    pub fn count(mut self, n: u64) -> Self {
        self.count = Some(n);
        self
    }
}

impl Command for HScan {
    type Response = ScanResult<(Bytes, Bytes)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("HSCAN"),
            bulk(self.key.as_str()),
            bulk(self.cursor.as_str()),
        ];
        if let Some(ref pattern) = self.pattern {
            args.push(bulk("MATCH"));
            args.push(bulk(pattern.as_str()));
        }
        if let Some(n) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(n.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_scan_pair_response(frame)
    }

    fn name(&self) -> &str {
        "HSCAN"
    }
}

/// ZSCAN key cursor \[MATCH pattern\] \[COUNT count\]
///
/// Iterates over members and scores of a sorted set. Returns (member, score) pairs.
#[derive(Clone)]
pub struct ZScan {
    key: String,
    cursor: String,
    pattern: Option<String>,
    count: Option<u64>,
}

impl ZScan {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            cursor: "0".to_string(),
            pattern: None,
            count: None,
        }
    }

    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = cursor.into();
        self
    }

    pub fn match_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    pub fn count(mut self, n: u64) -> Self {
        self.count = Some(n);
        self
    }
}

impl Command for ZScan {
    type Response = ScanResult<(Bytes, f64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("ZSCAN"),
            bulk(self.key.as_str()),
            bulk(self.cursor.as_str()),
        ];
        if let Some(ref pattern) = self.pattern {
            args.push(bulk("MATCH"));
            args.push(bulk(pattern.as_str()));
        }
        if let Some(n) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(n.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_scan_score_response(frame)
    }

    fn name(&self) -> &str {
        "ZSCAN"
    }
}

// -- Response parsing helpers --

/// Parse a SCAN response: [cursor, [results...]]
fn parse_scan_response<T>(
    frame: Frame,
    parse_item: impl Fn(&Frame) -> Result<T, RedisError>,
) -> Result<ScanResult<T>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) if items.len() == 2 => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "two-element array [cursor, results]",
                actual: format!("{other:?}"),
            });
        }
    };

    let cursor = match &items[0] {
        Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "bulk string cursor",
                actual: format!("{other:?}"),
            });
        }
    };

    let result_items = match &items[1] {
        Frame::Array(Some(items)) => items,
        Frame::Array(None) => {
            return Ok(ScanResult {
                cursor,
                results: Vec::new(),
            });
        }
        // RESP3 may return Set for SSCAN
        Frame::Set(items) => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of results",
                actual: format!("{other:?}"),
            });
        }
    };

    let mut results = Vec::with_capacity(result_items.len());
    for item in result_items {
        results.push(parse_item(item)?);
    }

    Ok(ScanResult { cursor, results })
}

/// Parse HSCAN response: [cursor, [field, value, field, value, ...]]
fn parse_scan_pair_response(frame: Frame) -> Result<ScanResult<(Bytes, Bytes)>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) if items.len() == 2 => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "two-element array [cursor, results]",
                actual: format!("{other:?}"),
            });
        }
    };

    let cursor = match &items[0] {
        Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "bulk string cursor",
                actual: format!("{other:?}"),
            });
        }
    };

    let result_items = match &items[1] {
        Frame::Array(Some(items)) => items,
        Frame::Array(None) => {
            return Ok(ScanResult {
                cursor,
                results: Vec::new(),
            });
        }
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of field-value pairs",
                actual: format!("{other:?}"),
            });
        }
    };

    if result_items.len() % 2 != 0 {
        return Err(RedisError::UnexpectedResponse {
            expected: "even number of elements",
            actual: format!("{} elements", result_items.len()),
        });
    }

    let mut results = Vec::with_capacity(result_items.len() / 2);
    for chunk in result_items.chunks(2) {
        let field = extract_bulk_bytes(&chunk[0])?;
        let value = extract_bulk_bytes(&chunk[1])?;
        results.push((field, value));
    }

    Ok(ScanResult { cursor, results })
}

/// Parse ZSCAN response: [cursor, [member, score, member, score, ...]]
fn parse_scan_score_response(frame: Frame) -> Result<ScanResult<(Bytes, f64)>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) if items.len() == 2 => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "two-element array [cursor, results]",
                actual: format!("{other:?}"),
            });
        }
    };

    let cursor = match &items[0] {
        Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "bulk string cursor",
                actual: format!("{other:?}"),
            });
        }
    };

    let result_items = match &items[1] {
        Frame::Array(Some(items)) => items,
        Frame::Array(None) => {
            return Ok(ScanResult {
                cursor,
                results: Vec::new(),
            });
        }
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of member-score pairs",
                actual: format!("{other:?}"),
            });
        }
    };

    if result_items.len() % 2 != 0 {
        return Err(RedisError::UnexpectedResponse {
            expected: "even number of elements",
            actual: format!("{} elements", result_items.len()),
        });
    }

    let mut results = Vec::with_capacity(result_items.len() / 2);
    for chunk in result_items.chunks(2) {
        let member = extract_bulk_bytes(&chunk[0])?;
        let score = match &chunk[1] {
            Frame::BulkString(Some(b)) => {
                let s = std::str::from_utf8(b).map_err(|e| RedisError::UnexpectedResponse {
                    expected: "valid UTF-8 score",
                    actual: e.to_string(),
                })?;
                s.parse::<f64>()
                    .map_err(|e| RedisError::UnexpectedResponse {
                        expected: "valid f64 score",
                        actual: e.to_string(),
                    })?
            }
            Frame::Double(d) => *d,
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "bulk string or double score",
                    actual: format!("{other:?}"),
                });
            }
        };
        results.push((member, score));
    }

    Ok(ScanResult { cursor, results })
}

fn extract_bulk_bytes(frame: &Frame) -> Result<Bytes, RedisError> {
    match frame {
        Frame::BulkString(Some(b)) => Ok(b.clone()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "bulk string",
            actual: format!("{other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    // -- Scan --

    #[test]
    fn scan_default_to_frame() {
        let cmd = Scan::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("SCAN"), bulk("0")]));
    }

    #[test]
    fn scan_with_options_to_frame() {
        let cmd = Scan::new()
            .cursor("42")
            .match_pattern("user:*")
            .count(100)
            .key_type("string");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("SCAN"),
                bulk("42"),
                bulk("MATCH"),
                bulk("user:*"),
                bulk("COUNT"),
                bulk("100"),
                bulk("TYPE"),
                bulk("string"),
            ])
        );
    }

    #[test]
    fn scan_parse_response_with_results() {
        let cmd = Scan::new();
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("17"))),
            Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from("key1"))),
                Frame::BulkString(Some(Bytes::from("key2"))),
            ])),
        ]));
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result.cursor, "17");
        assert!(!result.is_finished());
        assert_eq!(
            result.results,
            vec![Bytes::from("key1"), Bytes::from("key2")]
        );
    }

    #[test]
    fn scan_parse_response_finished() {
        let cmd = Scan::new();
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("0"))),
            Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from("last_key")))])),
        ]));
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result.cursor, "0");
        assert!(result.is_finished());
    }

    #[test]
    fn scan_parse_response_empty_results() {
        let cmd = Scan::new();
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("5"))),
            Frame::Array(None),
        ]));
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result.cursor, "5");
        assert!(result.results.is_empty());
    }

    #[test]
    fn scan_parse_error_on_wrong_shape() {
        let cmd = Scan::new();
        assert!(cmd.parse_response(Frame::Integer(0)).is_err());
    }

    // -- SScan --

    #[test]
    fn sscan_to_frame() {
        let cmd = SScan::new("myset").match_pattern("a*").count(10);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("SSCAN"),
                bulk("myset"),
                bulk("0"),
                bulk("MATCH"),
                bulk("a*"),
                bulk("COUNT"),
                bulk("10"),
            ])
        );
    }

    #[test]
    fn sscan_parse_response() {
        let cmd = SScan::new("myset");
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("0"))),
            Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from("member1")))])),
        ]));
        let result = cmd.parse_response(frame).unwrap();
        assert!(result.is_finished());
        assert_eq!(result.results, vec![Bytes::from("member1")]);
    }

    // -- HScan --

    #[test]
    fn hscan_to_frame() {
        let cmd = HScan::new("myhash").cursor("10");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("HSCAN"), bulk("myhash"), bulk("10")])
        );
    }

    #[test]
    fn hscan_parse_response_pairs() {
        let cmd = HScan::new("myhash");
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("0"))),
            Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from("field1"))),
                Frame::BulkString(Some(Bytes::from("value1"))),
                Frame::BulkString(Some(Bytes::from("field2"))),
                Frame::BulkString(Some(Bytes::from("value2"))),
            ])),
        ]));
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result.results.len(), 2);
        assert_eq!(
            result.results[0],
            (Bytes::from("field1"), Bytes::from("value1"))
        );
        assert_eq!(
            result.results[1],
            (Bytes::from("field2"), Bytes::from("value2"))
        );
    }

    #[test]
    fn hscan_parse_error_on_odd_elements() {
        let cmd = HScan::new("myhash");
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("0"))),
            Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from("field1"))),
                Frame::BulkString(Some(Bytes::from("value1"))),
                Frame::BulkString(Some(Bytes::from("field2"))),
            ])),
        ]));
        assert!(cmd.parse_response(frame).is_err());
    }

    // -- ZScan --

    #[test]
    fn zscan_to_frame() {
        let cmd = ZScan::new("myzset").match_pattern("a*");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("ZSCAN"),
                bulk("myzset"),
                bulk("0"),
                bulk("MATCH"),
                bulk("a*"),
            ])
        );
    }

    #[test]
    fn zscan_parse_response_member_score_pairs() {
        let cmd = ZScan::new("myzset");
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("0"))),
            Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from("member1"))),
                Frame::BulkString(Some(Bytes::from("1.5"))),
                Frame::BulkString(Some(Bytes::from("member2"))),
                Frame::BulkString(Some(Bytes::from("3.0"))),
            ])),
        ]));
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result.results.len(), 2);
        assert_eq!(result.results[0].0, Bytes::from("member1"));
        assert!((result.results[0].1 - 1.5).abs() < f64::EPSILON);
        assert_eq!(result.results[1].0, Bytes::from("member2"));
        assert!((result.results[1].1 - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn zscan_parse_double_score() {
        let cmd = ZScan::new("myzset");
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("0"))),
            Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from("member1"))),
                Frame::Double(2.5),
            ])),
        ]));
        let result = cmd.parse_response(frame).unwrap();
        assert!((result.results[0].1 - 2.5).abs() < f64::EPSILON);
    }
}
