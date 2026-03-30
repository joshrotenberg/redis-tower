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
