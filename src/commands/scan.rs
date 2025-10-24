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
