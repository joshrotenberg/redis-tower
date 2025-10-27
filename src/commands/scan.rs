//! Scan commands for iterating over large datasets
//!
//! SCAN family commands allow iterating over keys without blocking the server.
//! They return a cursor that can be used for subsequent calls.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// SCAN command - iterate over keys in the database
///
/// Returns a ScanResult with cursor and keys.
///
/// # Example
/// ```no_run
/// use redis_tower::commands::scan::Scan;
/// use redis_tower::client::RedisConnection;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// // Start scanning from cursor 0
/// let mut result = client.execute(Scan::new(0)).await?;
/// println!("Found {} keys", result.keys.len());
///
/// // Continue scanning until cursor is 0
/// while result.cursor != 0 {
///     result = client.execute(Scan::new(result.cursor)).await?;
///     println!("Found {} more keys", result.keys.len());
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Scan {
    pub(crate) cursor: u64,
    pub(crate) pattern: Option<String>,
    pub(crate) count: Option<usize>,
}

impl Scan {
    /// Create a new SCAN command starting from the given cursor
    pub fn new(cursor: u64) -> Self {
        Self {
            cursor,
            pattern: None,
            count: None,
        }
    }

    /// Filter keys by pattern (MATCH option)
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Hint for number of elements to return per call (COUNT option)
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }
}

/// Response from SCAN command
///
/// Contains the next cursor and the list of keys found.
/// When cursor is 0, the iteration is complete.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanResult {
    /// Next cursor (0 means iteration complete)
    pub cursor: u64,
    /// Keys found in this iteration
    pub keys: Vec<Bytes>,
}

impl Command for Scan {
    type Response = ScanResult;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("SCAN"))),
            Frame::BulkString(Some(Bytes::from(self.cursor.to_string()))),
        ];

        // Add MATCH pattern if specified
        if let Some(ref pattern) = self.pattern {
            frames.push(Frame::BulkString(Some(Bytes::from("MATCH"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                pattern.as_bytes(),
            ))));
        }

        // Add COUNT if specified
        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        // SCAN returns an array with two elements:
        // [0] = cursor (as bulk string)
        // [1] = array of keys
        match frame {
            Frame::Array(mut elements) if elements.len() == 2 => {
                let keys_frame = elements.pop().unwrap();
                let cursor_frame = elements.pop().unwrap();

                // Parse cursor
                let cursor = match cursor_frame {
                    Frame::BulkString(Some(cursor_bytes)) => {
                        let cursor_str = String::from_utf8_lossy(&cursor_bytes);
                        cursor_str
                            .parse::<u64>()
                            .map_err(|_| RedisError::Protocol("Invalid cursor".to_string()))?
                    }
                    _ => {
                        return Err(RedisError::Protocol(
                            "SCAN cursor must be bulk string".to_string(),
                        ));
                    }
                };

                // Parse keys array
                let keys = match keys_frame {
                    Frame::Array(key_frames) => {
                        let mut keys = Vec::with_capacity(key_frames.len());
                        for key_frame in key_frames {
                            match key_frame {
                                Frame::BulkString(Some(key)) => keys.push(key),
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "SCAN key must be bulk string".to_string(),
                                    ));
                                }
                            }
                        }
                        keys
                    }
                    _ => return Err(RedisError::Protocol("SCAN keys must be array".to_string())),
                };

                Ok(ScanResult { cursor, keys })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// SSCAN command for iterating over set members.
///
/// Incrementally iterates over the members of a set using a cursor.
///
/// # Example
///
/// ```rust
/// use redis_tower::commands::SScan;
///
/// let scan = SScan::new("myset", 0)
///     .pattern("prefix:*")
///     .count(100);
/// ```
///
/// Available since: Redis 2.8.0
#[derive(Debug, Clone)]
pub struct SScan {
    pub(crate) key: String,
    pub(crate) cursor: u64,
    pub(crate) pattern: Option<String>,
    pub(crate) count: Option<usize>,
}

impl SScan {
    /// Create a new SSCAN command.
    ///
    /// # Arguments
    ///
    /// * `key` - The set key to scan
    /// * `cursor` - The cursor position (0 to start)
    pub fn new(key: impl Into<String>, cursor: u64) -> Self {
        Self {
            key: key.into(),
            cursor,
            pattern: None,
            count: None,
        }
    }

    /// Set the MATCH pattern for filtering members.
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Set the COUNT hint for number of members to return.
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }
}

/// Result from SSCAN command containing cursor and members.
#[derive(Debug, Clone, PartialEq)]
pub struct SScanResult {
    /// Next cursor position (0 indicates iteration complete)
    pub cursor: u64,
    /// Set members matching the scan
    pub members: Vec<Bytes>,
}

impl Command for SScan {
    type Response = SScanResult;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(b"SSCAN".to_vec().into())),
            Frame::BulkString(Some(self.key.as_bytes().to_vec().into())),
            Frame::BulkString(Some(self.cursor.to_string().as_bytes().to_vec().into())),
        ];

        if let Some(pattern) = &self.pattern {
            args.push(Frame::BulkString(Some(b"MATCH".to_vec().into())));
            args.push(Frame::BulkString(Some(pattern.as_bytes().to_vec().into())));
        }

        if let Some(count) = self.count {
            args.push(Frame::BulkString(Some(b"COUNT".to_vec().into())));
            args.push(Frame::BulkString(Some(
                count.to_string().as_bytes().to_vec().into(),
            )));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(mut parts) if parts.len() == 2 => {
                let cursor = match parts.remove(0) {
                    Frame::BulkString(Some(data)) => String::from_utf8_lossy(&data)
                        .parse::<u64>()
                        .map_err(|_| RedisError::Protocol("Invalid cursor".to_string()))?,
                    _ => {
                        return Err(RedisError::Protocol(
                            "SSCAN cursor must be bulk string".to_string(),
                        ));
                    }
                };

                let members = match parts.remove(0) {
                    Frame::Array(members) => {
                        let mut result = Vec::with_capacity(members.len());
                        for member in members {
                            match member {
                                Frame::BulkString(Some(value)) => {
                                    result.push(value);
                                }
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "SSCAN members must be bulk strings".to_string(),
                                    ));
                                }
                            }
                        }
                        result
                    }
                    _ => {
                        return Err(RedisError::Protocol(
                            "SSCAN members must be array".to_string(),
                        ));
                    }
                };

                Ok(SScanResult { cursor, members })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ZSCAN command for iterating over sorted set members and scores.
///
/// Incrementally iterates over the members and scores of a sorted set using a cursor.
///
/// # Example
///
/// ```rust
/// use redis_tower::commands::ZScan;
///
/// let scan = ZScan::new("leaderboard", 0)
///     .pattern("player:*")
///     .count(100);
/// ```
///
/// Available since: Redis 2.8.0
#[derive(Debug, Clone)]
pub struct ZScan {
    pub(crate) key: String,
    pub(crate) cursor: u64,
    pub(crate) pattern: Option<String>,
    pub(crate) count: Option<usize>,
}

impl ZScan {
    /// Create a new ZSCAN command.
    ///
    /// # Arguments
    ///
    /// * `key` - The sorted set key to scan
    /// * `cursor` - The cursor position (0 to start)
    pub fn new(key: impl Into<String>, cursor: u64) -> Self {
        Self {
            key: key.into(),
            cursor,
            pattern: None,
            count: None,
        }
    }

    /// Set the MATCH pattern for filtering members.
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Set the COUNT hint for number of members to return.
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }
}

/// Result from ZSCAN command containing cursor and member-score pairs.
#[derive(Debug, Clone, PartialEq)]
pub struct ZScanResult {
    /// Next cursor position (0 indicates iteration complete)
    pub cursor: u64,
    /// Sorted set members with scores
    pub members: Vec<(Bytes, f64)>,
}

impl Command for ZScan {
    type Response = ZScanResult;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(b"ZSCAN".to_vec().into())),
            Frame::BulkString(Some(self.key.as_bytes().to_vec().into())),
            Frame::BulkString(Some(self.cursor.to_string().as_bytes().to_vec().into())),
        ];

        if let Some(pattern) = &self.pattern {
            args.push(Frame::BulkString(Some(b"MATCH".to_vec().into())));
            args.push(Frame::BulkString(Some(pattern.as_bytes().to_vec().into())));
        }

        if let Some(count) = self.count {
            args.push(Frame::BulkString(Some(b"COUNT".to_vec().into())));
            args.push(Frame::BulkString(Some(
                count.to_string().as_bytes().to_vec().into(),
            )));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(mut parts) if parts.len() == 2 => {
                let cursor = match parts.remove(0) {
                    Frame::BulkString(Some(data)) => String::from_utf8_lossy(&data)
                        .parse::<u64>()
                        .map_err(|_| RedisError::Protocol("Invalid cursor".to_string()))?,
                    _ => {
                        return Err(RedisError::Protocol(
                            "ZSCAN cursor must be bulk string".to_string(),
                        ));
                    }
                };

                let members = match parts.remove(0) {
                    Frame::Array(elements) if elements.len() % 2 == 0 => {
                        let mut result = Vec::with_capacity(elements.len() / 2);
                        let mut iter = elements.into_iter();
                        while let (Some(member), Some(score)) = (iter.next(), iter.next()) {
                            match (member, score) {
                                (
                                    Frame::BulkString(Some(member)),
                                    Frame::BulkString(Some(score)),
                                ) => {
                                    let score_val =
                                        String::from_utf8_lossy(&score).parse::<f64>().map_err(
                                            |_| RedisError::Protocol("Invalid score".to_string()),
                                        )?;
                                    result.push((member, score_val));
                                }
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "ZSCAN elements must be bulk strings".to_string(),
                                    ));
                                }
                            }
                        }
                        result
                    }
                    _ => {
                        return Err(RedisError::Protocol(
                            "ZSCAN members must be array with even length".to_string(),
                        ));
                    }
                };

                Ok(ZScanResult { cursor, members })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// HSCAN command - iterate over fields in a hash
#[derive(Debug, Clone)]
pub struct HScan {
    pub(crate) key: String,
    pub(crate) cursor: u64,
    pub(crate) pattern: Option<String>,
    pub(crate) count: Option<usize>,
}

impl HScan {
    /// Create a new HSCAN command for the given key
    pub fn new(key: impl Into<String>, cursor: u64) -> Self {
        Self {
            key: key.into(),
            cursor,
            pattern: None,
            count: None,
        }
    }

    /// Filter fields by pattern (MATCH option)
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    /// Hint for number of elements to return per call (COUNT option)
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }
}

/// Response from HSCAN command
#[derive(Debug, Clone, PartialEq)]
pub struct HScanResult {
    /// Next cursor (0 means iteration complete)
    pub cursor: u64,
    /// Field-value pairs found in this iteration
    pub fields: Vec<(Bytes, Bytes)>,
}

impl Command for HScan {
    type Response = HScanResult;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("HSCAN"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.cursor.to_string()))),
        ];

        if let Some(ref pattern) = self.pattern {
            frames.push(Frame::BulkString(Some(Bytes::from("MATCH"))));
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                pattern.as_bytes(),
            ))));
        }

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(mut elements) if elements.len() == 2 => {
                let fields_frame = elements.pop().unwrap();
                let cursor_frame = elements.pop().unwrap();

                // Parse cursor
                let cursor = match cursor_frame {
                    Frame::BulkString(Some(cursor_bytes)) => {
                        let cursor_str = String::from_utf8_lossy(&cursor_bytes);
                        cursor_str
                            .parse::<u64>()
                            .map_err(|_| RedisError::Protocol("Invalid cursor".to_string()))?
                    }
                    _ => {
                        return Err(RedisError::Protocol(
                            "HSCAN cursor must be bulk string".to_string(),
                        ));
                    }
                };

                // Parse field-value pairs (flat array)
                let fields = match fields_frame {
                    Frame::Array(pair_frames) => {
                        if pair_frames.len() % 2 != 0 {
                            return Err(RedisError::Protocol(
                                "HSCAN must return even number of elements".to_string(),
                            ));
                        }

                        let mut fields = Vec::with_capacity(pair_frames.len() / 2);
                        let mut iter = pair_frames.into_iter();

                        while let (Some(field_frame), Some(value_frame)) =
                            (iter.next(), iter.next())
                        {
                            match (field_frame, value_frame) {
                                (
                                    Frame::BulkString(Some(field)),
                                    Frame::BulkString(Some(value)),
                                ) => {
                                    fields.push((field, value));
                                }
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "HSCAN elements must be bulk strings".to_string(),
                                    ));
                                }
                            }
                        }
                        fields
                    }
                    _ => {
                        return Err(RedisError::Protocol(
                            "HSCAN fields must be array".to_string(),
                        ));
                    }
                };

                Ok(HScanResult { cursor, fields })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}
