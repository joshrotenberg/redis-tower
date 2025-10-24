//! Redis Streams commands (XADD, XREAD, etc.)
//!
//! Streams are append-only logs with complex nested response structures.
//! Level 3/4 complexity: Custom types + optional blocking.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;
use std::collections::HashMap;

/// Stream entry ID (timestamp-sequence format)
///
/// Redis stream IDs are formatted as "timestamp-sequence" (e.g., "1234567890123-0")
/// Special IDs:
/// - "*" - Auto-generate ID (for XADD)
/// - "$" - Start from latest (for XREAD)
/// - "0" - Start from beginning
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamId(pub String);

impl StreamId {
    /// Auto-generate ID (for XADD)
    pub fn auto() -> Self {
        StreamId("*".to_string())
    }

    /// Start from latest entries (for XREAD)
    pub fn latest() -> Self {
        StreamId("$".to_string())
    }

    /// Start from beginning
    pub fn beginning() -> Self {
        StreamId("0".to_string())
    }

    /// Create from timestamp-sequence string
    pub fn new(id: impl Into<String>) -> Self {
        StreamId(id.into())
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Stream entry - an ID with field-value pairs
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEntry {
    /// Entry ID (timestamp-sequence)
    pub id: StreamId,
    /// Field-value pairs
    pub fields: HashMap<String, Bytes>,
}

/// XADD command - add entry to stream
///
/// Appends a new entry to the stream with the given fields.
/// Returns the generated entry ID.
///
/// # Example
/// ```no_run
/// use redis_tower::commands::streams::{XAdd, StreamId};
/// use redis_tower::RedisClient;
/// use std::collections::HashMap;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisClient::connect("localhost:6379").await?;
///
/// let mut fields = HashMap::new();
/// fields.insert("sensor".to_string(), "temperature".into());
/// fields.insert("value".to_string(), "23.5".into());
///
/// // Auto-generate ID
/// let id = client.call(XAdd::new("sensor_data", StreamId::auto(), fields)).await?;
/// println!("Added entry with ID: {}", id);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct XAdd {
    pub(crate) key: String,
    pub(crate) id: StreamId,
    pub(crate) fields: HashMap<String, Bytes>,
    pub(crate) maxlen: Option<usize>, // Optional MAXLEN
}

impl XAdd {
    /// Create a new XADD command
    pub fn new(key: impl Into<String>, id: StreamId, fields: HashMap<String, Bytes>) -> Self {
        Self {
            key: key.into(),
            id,
            fields,
            maxlen: None,
        }
    }

    /// Limit stream to maximum length (approximate)
    pub fn maxlen(mut self, maxlen: usize) -> Self {
        self.maxlen = Some(maxlen);
        self
    }
}

impl Command for XAdd {
    type Response = StreamId;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("XADD"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        // Add MAXLEN if specified
        if let Some(maxlen) = self.maxlen {
            frames.push(Frame::BulkString(Some(Bytes::from("MAXLEN"))));
            frames.push(Frame::BulkString(Some(Bytes::from("~")))); // Approximate
            frames.push(Frame::BulkString(Some(Bytes::from(maxlen.to_string()))));
        }

        // Add ID
        frames.push(Frame::BulkString(Some(Bytes::from(self.id.to_string()))));

        // Add field-value pairs
        for (field, value) in &self.fields {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                field.as_bytes(),
            ))));
            frames.push(Frame::BulkString(Some(value.clone())));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(id_bytes)) => {
                let id_str = String::from_utf8_lossy(&id_bytes).to_string();
                Ok(StreamId::new(id_str))
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// XREAD command - read entries from streams
///
/// Reads new entries from one or more streams.
/// Can block until new data arrives.
///
/// # Level 4 Complexity
/// - Complex nested response: Map of streams -> array of entries
/// - Optional blocking with timeout
/// - Multiple streams with different starting IDs
///
/// # Example
/// ```no_run
/// use redis_tower::commands::streams::{XRead, StreamId};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisClient::connect("localhost:6379").await?;
///
/// // Non-blocking read
/// let streams = vec![("sensor_data".to_string(), StreamId::beginning())];
/// let results = client.call(XRead::new(streams)).await?;
///
/// for (stream, entries) in results {
///     println!("Stream: {}", stream);
///     for entry in entries {
///         println!("  ID: {}", entry.id);
///         for (field, value) in &entry.fields {
///             println!("    {}: {}", field, String::from_utf8_lossy(value));
///         }
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct XRead {
    pub(crate) streams: Vec<(String, StreamId)>,
    pub(crate) count: Option<usize>,
    pub(crate) block: Option<u64>, // milliseconds (None = non-blocking)
}

impl XRead {
    /// Create a new XREAD command
    ///
    /// # Arguments
    /// * `streams` - List of (stream_name, starting_id) tuples
    pub fn new(streams: Vec<(String, StreamId)>) -> Self {
        Self {
            streams,
            count: None,
            block: None,
        }
    }

    /// Limit number of entries returned per stream
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }

    /// Block for specified milliseconds (0 = block forever)
    pub fn block(mut self, milliseconds: u64) -> Self {
        self.block = Some(milliseconds);
        self
    }
}

/// Result from XREAD - map of stream names to entries
pub type XReadResult = HashMap<String, Vec<StreamEntry>>;

impl Command for XRead {
    type Response = XReadResult;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![Frame::BulkString(Some(Bytes::from("XREAD")))];

        // Add COUNT if specified
        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        // Add BLOCK if specified
        if let Some(block_ms) = self.block {
            frames.push(Frame::BulkString(Some(Bytes::from("BLOCK"))));
            frames.push(Frame::BulkString(Some(Bytes::from(block_ms.to_string()))));
        }

        // Add STREAMS keyword
        frames.push(Frame::BulkString(Some(Bytes::from("STREAMS"))));

        // Add stream names
        for (name, _) in &self.streams {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                name.as_bytes(),
            ))));
        }

        // Add stream IDs
        for (_, id) in &self.streams {
            frames.push(Frame::BulkString(Some(Bytes::from(id.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // Timeout or no new data - returns null
            Frame::Null | Frame::BulkString(None) => Ok(HashMap::new()),

            // Success - returns array of [stream_name, [entries...]]
            Frame::Array(stream_arrays) => {
                let mut result = HashMap::new();

                for stream_frame in stream_arrays {
                    match stream_frame {
                        Frame::Array(mut stream_data) if stream_data.len() == 2 => {
                            let entries_frame = stream_data.pop().unwrap();
                            let name_frame = stream_data.pop().unwrap();

                            // Parse stream name
                            let stream_name = match name_frame {
                                Frame::BulkString(Some(name_bytes)) => {
                                    String::from_utf8_lossy(&name_bytes).to_string()
                                }
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "Stream name must be bulk string".to_string(),
                                    ));
                                }
                            };

                            // Parse entries array
                            let entries = match entries_frame {
                                Frame::Array(entry_frames) => parse_stream_entries(entry_frames)?,
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "Entries must be array".to_string(),
                                    ));
                                }
                            };

                            result.insert(stream_name, entries);
                        }
                        _ => {
                            return Err(RedisError::Protocol(
                                "Each stream must be [name, entries]".to_string(),
                            ));
                        }
                    }
                }

                Ok(result)
            }

            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),

            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Helper to parse stream entries from array
fn parse_stream_entries(entry_frames: Vec<Frame>) -> Result<Vec<StreamEntry>, RedisError> {
    let mut entries = Vec::new();

    for entry_frame in entry_frames {
        match entry_frame {
            Frame::Array(mut entry_data) if entry_data.len() == 2 => {
                let fields_frame = entry_data.pop().unwrap();
                let id_frame = entry_data.pop().unwrap();

                // Parse entry ID
                let id = match id_frame {
                    Frame::BulkString(Some(id_bytes)) => {
                        StreamId::new(String::from_utf8_lossy(&id_bytes).to_string())
                    }
                    _ => {
                        return Err(RedisError::Protocol(
                            "Entry ID must be bulk string".to_string(),
                        ));
                    }
                };

                // Parse field-value pairs
                let fields = match fields_frame {
                    Frame::Array(field_frames) => {
                        if field_frames.len() % 2 != 0 {
                            return Err(RedisError::Protocol(
                                "Fields must have even number of elements".to_string(),
                            ));
                        }

                        let mut fields = HashMap::new();
                        let mut iter = field_frames.into_iter();

                        while let (Some(field_frame), Some(value_frame)) =
                            (iter.next(), iter.next())
                        {
                            match (field_frame, value_frame) {
                                (
                                    Frame::BulkString(Some(field_bytes)),
                                    Frame::BulkString(Some(value_bytes)),
                                ) => {
                                    let field = String::from_utf8_lossy(&field_bytes).to_string();
                                    fields.insert(field, value_bytes);
                                }
                                _ => {
                                    return Err(RedisError::Protocol(
                                        "Field/value must be bulk strings".to_string(),
                                    ));
                                }
                            }
                        }
                        fields
                    }
                    _ => return Err(RedisError::Protocol("Fields must be array".to_string())),
                };

                entries.push(StreamEntry { id, fields });
            }
            _ => {
                return Err(RedisError::Protocol(
                    "Entry must be [id, fields]".to_string(),
                ));
            }
        }
    }

    Ok(entries)
}

/// XLEN command - get stream length
///
/// Returns the number of entries in a stream.
///
/// # Example
/// ```no_run
/// use redis_tower::commands::streams::XLen;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisClient::connect("localhost:6379").await?;
/// let length: i64 = client.call(XLen::new("sensor_data")).await?;
/// println!("Stream has {} entries", length);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct XLen {
    pub(crate) key: String,
}

impl XLen {
    /// Create a new XLEN command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for XLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("XLEN"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(count) => Ok(count),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// XDEL command - delete stream entries
///
/// Removes one or more entries from a stream by ID.
/// Returns the number of entries actually deleted.
///
/// # Example
/// ```no_run
/// use redis_tower::commands::streams::{XDel, StreamId};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisClient::connect("localhost:6379").await?;
/// let deleted: i64 = client.call(
///     XDel::new("sensor_data", vec![
///         StreamId::new("1234567890123-0"),
///         StreamId::new("1234567890124-0"),
///     ])
/// ).await?;
/// println!("Deleted {} entries", deleted);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct XDel {
    pub(crate) key: String,
    pub(crate) ids: Vec<StreamId>,
}

impl XDel {
    /// Create a new XDEL command
    pub fn new(key: impl Into<String>, ids: Vec<StreamId>) -> Self {
        Self {
            key: key.into(),
            ids,
        }
    }
}

impl Command for XDel {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("XDEL"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for id in &self.ids {
            frames.push(Frame::BulkString(Some(Bytes::from(id.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(count) => Ok(count),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// XTRIM command - trim stream to approximate max length or by minimum ID
///
/// Trims the stream to a maximum number of entries or removes entries older than a minimum ID.
/// Returns the number of entries removed.
///
/// # Example
/// ```no_run
/// use redis_tower::commands::streams::{XTrim, StreamId};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisClient::connect("localhost:6379").await?;
///
/// // Trim to ~1000 entries (approximate)
/// let removed: i64 = client.call(XTrim::maxlen("sensor_data", 1000)).await?;
/// println!("Removed {} old entries", removed);
///
/// // Trim by minimum ID
/// let removed: i64 = client.call(
///     XTrim::minid("sensor_data", StreamId::new("1234567890000-0"))
/// ).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct XTrim {
    pub(crate) key: String,
    pub(crate) strategy: TrimStrategy,
    pub(crate) exact: bool, // false = approximate (~), true = exact (=)
    pub(crate) limit: Option<usize>,
}

/// Strategy for trimming a stream
#[derive(Debug, Clone)]
pub enum TrimStrategy {
    /// Trim to maximum length
    MaxLen(usize),
    /// Trim entries older than minimum ID
    MinId(StreamId),
}

impl XTrim {
    /// Trim to maximum length (approximate)
    pub fn maxlen(key: impl Into<String>, maxlen: usize) -> Self {
        Self {
            key: key.into(),
            strategy: TrimStrategy::MaxLen(maxlen),
            exact: false,
            limit: None,
        }
    }

    /// Trim by minimum ID (approximate)
    pub fn minid(key: impl Into<String>, minid: StreamId) -> Self {
        Self {
            key: key.into(),
            strategy: TrimStrategy::MinId(minid),
            exact: false,
            limit: None,
        }
    }

    /// Make trim exact (slower) instead of approximate
    pub fn exact(mut self) -> Self {
        self.exact = true;
        self
    }

    /// Limit number of entries to evict (for performance)
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl Command for XTrim {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("XTRIM"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        // Add strategy
        match &self.strategy {
            TrimStrategy::MaxLen(maxlen) => {
                frames.push(Frame::BulkString(Some(Bytes::from("MAXLEN"))));
                if !self.exact {
                    frames.push(Frame::BulkString(Some(Bytes::from("~"))));
                }
                frames.push(Frame::BulkString(Some(Bytes::from(maxlen.to_string()))));
            }
            TrimStrategy::MinId(minid) => {
                frames.push(Frame::BulkString(Some(Bytes::from("MINID"))));
                if !self.exact {
                    frames.push(Frame::BulkString(Some(Bytes::from("~"))));
                }
                frames.push(Frame::BulkString(Some(Bytes::from(minid.to_string()))));
            }
        }

        // Add LIMIT if specified
        if let Some(limit) = self.limit {
            frames.push(Frame::BulkString(Some(Bytes::from("LIMIT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(limit.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(count) => Ok(count),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// XRANGE command - query stream entries by ID range
///
/// Returns entries with IDs between start and end (inclusive).
/// Can limit the number of results returned.
///
/// # Special IDs
/// - "-" - Minimum possible ID
/// - "+" - Maximum possible ID
///
/// # Example
/// ```no_run
/// use redis_tower::commands::streams::{XRange, StreamId};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisClient::connect("localhost:6379").await?;
///
/// // Get all entries
/// let entries = client.call(XRange::all("sensor_data")).await?;
///
/// // Get specific range
/// let entries = client.call(
///     XRange::new(
///         "sensor_data",
///         StreamId::new("1234567890000-0"),
///         StreamId::new("1234567890999-0"),
///     )
/// ).await?;
///
/// // Limit results
/// let entries = client.call(XRange::all("sensor_data").count(10)).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct XRange {
    pub(crate) key: String,
    pub(crate) start: StreamId,
    pub(crate) end: StreamId,
    pub(crate) count: Option<usize>,
}

impl XRange {
    /// Create a new XRANGE command with specific start and end IDs
    pub fn new(key: impl Into<String>, start: StreamId, end: StreamId) -> Self {
        Self {
            key: key.into(),
            start,
            end,
            count: None,
        }
    }

    /// Get all entries in the stream
    pub fn all(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            start: StreamId::new("-"),
            end: StreamId::new("+"),
            count: None,
        }
    }

    /// Limit number of entries returned
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for XRange {
    type Response = Vec<StreamEntry>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("XRANGE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.start.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.end.to_string()))),
        ];

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(entries) => parse_stream_entries(entries),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// XREADGROUP command - Read from stream as consumer group
///
/// Read entries from stream as part of a consumer group.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::XReadGroup;
///
/// // Read new messages from group
/// let cmd = XReadGroup::new("mygroup", "consumer1")
///     .stream("mystream", ">");
///
/// // Read with count and block
/// let cmd = XReadGroup::new("mygroup", "consumer1")
///     .stream("mystream", ">")
///     .count(10)
///     .block(5000);
/// ```
#[derive(Debug, Clone)]
pub struct XReadGroup {
    group: String,
    consumer: String,
    streams: Vec<(String, String)>,
    count: Option<usize>,
    block: Option<i64>,
    noack: bool,
}

impl XReadGroup {
    /// Create a new XREADGROUP command
    pub fn new(group: impl Into<String>, consumer: impl Into<String>) -> Self {
        Self {
            group: group.into(),
            consumer: consumer.into(),
            streams: Vec::new(),
            count: None,
            block: None,
            noack: false,
        }
    }

    /// Add a stream to read from
    pub fn stream(mut self, key: impl Into<String>, id: impl Into<String>) -> Self {
        self.streams.push((key.into(), id.into()));
        self
    }

    /// Limit number of entries per stream
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }

    /// Block for specified milliseconds
    pub fn block(mut self, milliseconds: i64) -> Self {
        self.block = Some(milliseconds);
        self
    }

    /// Don't auto-acknowledge messages
    pub fn noack(mut self) -> Self {
        self.noack = true;
        self
    }
}

impl Command for XReadGroup {
    type Response = XReadResult;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("XREADGROUP"))),
            Frame::BulkString(Some(Bytes::from("GROUP"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.group.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.consumer.as_bytes()))),
        ];

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        if let Some(block) = self.block {
            frames.push(Frame::BulkString(Some(Bytes::from("BLOCK"))));
            frames.push(Frame::BulkString(Some(Bytes::from(block.to_string()))));
        }

        if self.noack {
            frames.push(Frame::BulkString(Some(Bytes::from("NOACK"))));
        }

        frames.push(Frame::BulkString(Some(Bytes::from("STREAMS"))));

        for (key, _) in &self.streams {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        for (_, id) in &self.streams {
            frames.push(Frame::BulkString(Some(Bytes::from(id.clone()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        XRead::parse_response(frame)
    }
}

/// XACK command - Acknowledge stream messages
///
/// Mark messages as processed in a consumer group.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::XAck;
///
/// let cmd = XAck::new("mystream", "mygroup", vec!["1234-0", "1235-0"]);
/// ```
#[derive(Debug, Clone)]
pub struct XAck {
    key: String,
    group: String,
    ids: Vec<String>,
}

impl XAck {
    /// Create a new XACK command
    pub fn new(
        key: impl Into<String>,
        group: impl Into<String>,
        ids: Vec<impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            ids: ids.into_iter().map(|s| s.into()).collect(),
        }
    }
}

impl Command for XAck {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("XACK"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.group.as_bytes()))),
        ];

        for id in &self.ids {
            frames.push(Frame::BulkString(Some(Bytes::from(id.clone()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// XPENDING command - Get pending messages info
///
/// Returns information about pending messages in a consumer group.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::XPending;
///
/// // Get summary
/// let cmd = XPending::new("mystream", "mygroup");
///
/// // Get detailed list with range
/// let cmd = XPending::new("mystream", "mygroup")
///     .range("-", "+", 10);
/// ```
#[derive(Debug, Clone)]
pub struct XPending {
    key: String,
    group: String,
    start: Option<String>,
    end: Option<String>,
    count: Option<i64>,
    consumer: Option<String>,
}

impl XPending {
    /// Create a new XPENDING command
    pub fn new(key: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            start: None,
            end: None,
            count: None,
            consumer: None,
        }
    }

    /// Get detailed list with range
    pub fn range(mut self, start: impl Into<String>, end: impl Into<String>, count: i64) -> Self {
        self.start = Some(start.into());
        self.end = Some(end.into());
        self.count = Some(count);
        self
    }

    /// Filter by consumer
    pub fn consumer(mut self, consumer: impl Into<String>) -> Self {
        self.consumer = Some(consumer.into());
        self
    }
}

impl Command for XPending {
    type Response = Bytes;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("XPENDING"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.group.as_bytes()))),
        ];

        if let Some(ref start) = self.start {
            frames.push(Frame::BulkString(Some(Bytes::from(start.clone()))));
            frames.push(Frame::BulkString(Some(Bytes::from(
                self.end.clone().unwrap(),
            ))));
            frames.push(Frame::BulkString(Some(Bytes::from(
                self.count.unwrap().to_string(),
            ))));

            if let Some(ref consumer) = self.consumer {
                frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                    consumer.as_bytes(),
                ))));
            }
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        // Complex response - return raw for now
        match frame {
            Frame::Array(_) => Ok(Bytes::from("PENDING_INFO")),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// XCLAIM command - Claim pending messages
///
/// Transfer ownership of pending messages to a different consumer.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::XClaim;
///
/// let cmd = XClaim::new("mystream", "mygroup", "consumer2", 3600000, vec!["1234-0"]);
/// ```
#[derive(Debug, Clone)]
pub struct XClaim {
    key: String,
    group: String,
    consumer: String,
    min_idle_time: i64,
    ids: Vec<String>,
}

impl XClaim {
    /// Create a new XCLAIM command
    pub fn new(
        key: impl Into<String>,
        group: impl Into<String>,
        consumer: impl Into<String>,
        min_idle_time: i64,
        ids: Vec<impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            consumer: consumer.into(),
            min_idle_time,
            ids: ids.into_iter().map(|s| s.into()).collect(),
        }
    }
}

impl Command for XClaim {
    type Response = Vec<StreamEntry>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("XCLAIM"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.group.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.consumer.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.min_idle_time.to_string()))),
        ];

        for id in &self.ids {
            frames.push(Frame::BulkString(Some(Bytes::from(id.clone()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(entries) => parse_stream_entries(entries),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// XGROUP CREATE command - Create consumer group
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::XGroupCreate;
///
/// // Create group from beginning
/// let cmd = XGroupCreate::new("mystream", "mygroup", "0");
///
/// // Create group with MKSTREAM
/// let cmd = XGroupCreate::new("mystream", "mygroup", "$").mkstream();
/// ```
#[derive(Debug, Clone)]
pub struct XGroupCreate {
    key: String,
    group: String,
    id: String,
    mkstream: bool,
}

impl XGroupCreate {
    /// Create a new XGROUP CREATE command
    pub fn new(key: impl Into<String>, group: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            id: id.into(),
            mkstream: false,
        }
    }

    /// Create stream if it doesn't exist
    pub fn mkstream(mut self) -> Self {
        self.mkstream = true;
        self
    }
}

impl Command for XGroupCreate {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("XGROUP"))),
            Frame::BulkString(Some(Bytes::from("CREATE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.group.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.id.clone()))),
        ];

        if self.mkstream {
            frames.push(Frame::BulkString(Some(Bytes::from("MKSTREAM"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// XGROUP DESTROY command - Destroy consumer group
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::XGroupDestroy;
///
/// let cmd = XGroupDestroy::new("mystream", "mygroup");
/// ```
#[derive(Debug, Clone)]
pub struct XGroupDestroy {
    key: String,
    group: String,
}

impl XGroupDestroy {
    /// Create a new XGROUP DESTROY command
    pub fn new(key: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
        }
    }
}

impl Command for XGroupDestroy {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("XGROUP"))),
            Frame::BulkString(Some(Bytes::from("DESTROY"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.group.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ReadOnly trait implementations
use crate::read_preference::ReadOnly;

impl ReadOnly for XLen {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for XRange {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for XRevRange {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for XRead {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for XReadGroup {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for XPending {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands
impl ReadOnly for XAdd {}
impl ReadOnly for XDel {}
impl ReadOnly for XTrim {}
impl ReadOnly for XAck {}
impl ReadOnly for XClaim {}
impl ReadOnly for XGroupCreate {}
impl ReadOnly for XGroupDestroy {}

/// XREVRANGE command - query stream entries in reverse order
///
/// Like XRANGE but returns entries in reverse order (newest to oldest).
/// Note: start and end are still specified in forward order, but results are reversed.
///
/// # Example
/// ```no_run
/// use redis_tower::commands::streams::{XRevRange, StreamId};
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisClient::connect("localhost:6379").await?;
///
/// // Get last 10 entries (newest first)
/// let entries = client.call(XRevRange::all("sensor_data").count(10)).await?;
///
/// for entry in entries {
///     println!("ID: {} (newest to oldest)", entry.id);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct XRevRange {
    pub(crate) key: String,
    pub(crate) end: StreamId,
    pub(crate) start: StreamId,
    pub(crate) count: Option<usize>,
}

impl XRevRange {
    /// Create a new XREVRANGE command with specific start and end IDs
    /// Note: Despite being reversed, you still specify start before end
    pub fn new(key: impl Into<String>, start: StreamId, end: StreamId) -> Self {
        Self {
            key: key.into(),
            end,
            start,
            count: None,
        }
    }

    /// Get all entries in the stream (reversed)
    pub fn all(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            end: StreamId::new("+"),
            start: StreamId::new("-"),
            count: None,
        }
    }

    /// Limit number of entries returned
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for XRevRange {
    type Response = Vec<StreamEntry>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("XREVRANGE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.end.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.start.to_string()))),
        ];

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(entries) => parse_stream_entries(entries),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}
