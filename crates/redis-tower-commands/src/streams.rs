//! Redis Streams commands.

use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// A stream entry: an ID and a list of field-value pairs.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(String, Bytes)>,
}

/// XADD key \[NOMKSTREAM\] \[MAXLEN|MINID \[=|~\] threshold\] \[*|id\] field value \[field value ...\]
///
/// Appends an entry to a stream. Returns the entry ID.
pub struct XAdd {
    key: String,
    id: String,
    fields: Vec<(String, String)>,
    nomkstream: bool,
    maxlen: Option<(bool, u64)>,   // (approximate, count)
    minid: Option<(bool, String)>, // (approximate, id)
}

impl XAdd {
    /// Create an XADD with auto-generated ID (*).
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            id: "*".to_string(),
            fields: Vec::new(),
            nomkstream: false,
            maxlen: None,
            minid: None,
        }
    }

    /// Set a specific entry ID instead of auto-generated.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// Add a field-value pair.
    pub fn field(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push((name.into(), value.into()));
        self
    }

    /// Don't create the stream if it doesn't exist.
    pub fn nomkstream(mut self) -> Self {
        self.nomkstream = true;
        self
    }

    /// Trim stream to approximately `count` entries.
    pub fn maxlen_approx(mut self, count: u64) -> Self {
        self.maxlen = Some((true, count));
        self.minid = None;
        self
    }

    /// Trim stream to exactly `count` entries.
    pub fn maxlen(mut self, count: u64) -> Self {
        self.maxlen = Some((false, count));
        self.minid = None;
        self
    }
}

impl Command for XAdd {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("XADD"), bulk(self.key.as_str())];
        if self.nomkstream {
            args.push(bulk("NOMKSTREAM"));
        }
        if let Some((approx, count)) = &self.maxlen {
            args.push(bulk("MAXLEN"));
            if *approx {
                args.push(bulk("~"));
            }
            args.push(bulk(count.to_string()));
        }
        if let Some((approx, id)) = &self.minid {
            args.push(bulk("MINID"));
            if *approx {
                args.push(bulk("~"));
            }
            args.push(bulk(id.as_str()));
        }
        args.push(bulk(self.id.as_str()));
        for (name, value) in &self.fields {
            args.push(bulk(name.as_str()));
            args.push(bulk(value.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(b)) => Ok(String::from_utf8_lossy(&b).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string (entry ID)",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "XADD"
    }
}

/// XLEN key
///
/// Returns the number of entries in a stream.
pub struct XLen {
    key: String,
}

impl XLen {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for XLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("XLEN"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "XLEN"
    }
}

/// XRANGE key start end \[COUNT count\]
///
/// Returns entries in a stream within a range of IDs.
pub struct XRange {
    key: String,
    start: String,
    end: String,
    count: Option<u64>,
}

impl XRange {
    /// Query all entries: start="-", end="+".
    pub fn all(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            start: "-".to_string(),
            end: "+".to_string(),
            count: None,
        }
    }

    /// Query a specific range.
    pub fn new(key: impl Into<String>, start: impl Into<String>, end: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            start: start.into(),
            end: end.into(),
            count: None,
        }
    }

    /// Limit the number of returned entries.
    pub fn count(mut self, n: u64) -> Self {
        self.count = Some(n);
        self
    }
}

impl Command for XRange {
    type Response = Vec<StreamEntry>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("XRANGE"),
            bulk(self.key.as_str()),
            bulk(self.start.as_str()),
            bulk(self.end.as_str()),
        ];
        if let Some(n) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(n.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_stream_entries(&frame)
    }

    fn name(&self) -> &str {
        "XRANGE"
    }
}

/// XREVRANGE key end start \[COUNT count\]
///
/// Like XRANGE but in reverse order.
pub struct XRevRange {
    key: String,
    end: String,
    start: String,
    count: Option<u64>,
}

impl XRevRange {
    pub fn all(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            end: "+".to_string(),
            start: "-".to_string(),
            count: None,
        }
    }

    pub fn new(key: impl Into<String>, end: impl Into<String>, start: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            end: end.into(),
            start: start.into(),
            count: None,
        }
    }

    pub fn count(mut self, n: u64) -> Self {
        self.count = Some(n);
        self
    }
}

impl Command for XRevRange {
    type Response = Vec<StreamEntry>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("XREVRANGE"),
            bulk(self.key.as_str()),
            bulk(self.end.as_str()),
            bulk(self.start.as_str()),
        ];
        if let Some(n) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(n.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_stream_entries(&frame)
    }

    fn name(&self) -> &str {
        "XREVRANGE"
    }
}

/// XDEL key id \[id ...\]
///
/// Removes entries from a stream. Returns the number deleted.
pub struct XDel {
    key: String,
    ids: Vec<String>,
}

impl XDel {
    pub fn new(key: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            ids: vec![id.into()],
        }
    }

    pub fn ids(key: impl Into<String>, ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            ids: ids.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for XDel {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("XDEL"), bulk(self.key.as_str())];
        for id in &self.ids {
            args.push(bulk(id.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "XDEL"
    }
}

/// XTRIM key MAXLEN|MINID \[=|~\] threshold
///
/// Trims a stream. Returns the number of entries deleted.
pub struct XTrim {
    key: String,
    maxlen: Option<(bool, u64)>,
    minid: Option<(bool, String)>,
}

impl XTrim {
    pub fn maxlen(key: impl Into<String>, count: u64) -> Self {
        Self {
            key: key.into(),
            maxlen: Some((false, count)),
            minid: None,
        }
    }

    pub fn maxlen_approx(key: impl Into<String>, count: u64) -> Self {
        Self {
            key: key.into(),
            maxlen: Some((true, count)),
            minid: None,
        }
    }

    pub fn minid(key: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            maxlen: None,
            minid: Some((false, id.into())),
        }
    }
}

impl Command for XTrim {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("XTRIM"), bulk(self.key.as_str())];
        if let Some((approx, count)) = &self.maxlen {
            args.push(bulk("MAXLEN"));
            if *approx {
                args.push(bulk("~"));
            }
            args.push(bulk(count.to_string()));
        }
        if let Some((approx, id)) = &self.minid {
            args.push(bulk("MINID"));
            if *approx {
                args.push(bulk("~"));
            }
            args.push(bulk(id.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "XTRIM"
    }
}

/// XACK key group id \[id ...\]
///
/// Acknowledges stream entries in a consumer group. Returns count acknowledged.
pub struct XAck {
    key: String,
    group: String,
    ids: Vec<String>,
}

impl XAck {
    pub fn new(key: impl Into<String>, group: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            ids: vec![id.into()],
        }
    }

    pub fn ids(
        key: impl Into<String>,
        group: impl Into<String>,
        ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            ids: ids.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for XAck {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("XACK"),
            bulk(self.key.as_str()),
            bulk(self.group.as_str()),
        ];
        for id in &self.ids {
            args.push(bulk(id.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "XACK"
    }
}

/// XGROUP CREATE key group id \[MKSTREAM\]
///
/// Creates a consumer group.
pub struct XGroupCreate {
    key: String,
    group: String,
    id: String,
    mkstream: bool,
}

impl XGroupCreate {
    /// Create a group starting from the given ID.
    /// Use "$" to only receive new entries, "0" for all existing entries.
    pub fn new(key: impl Into<String>, group: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            id: id.into(),
            mkstream: false,
        }
    }

    /// Create the stream if it doesn't exist.
    pub fn mkstream(mut self) -> Self {
        self.mkstream = true;
        self
    }
}

impl Command for XGroupCreate {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("XGROUP"),
            bulk("CREATE"),
            bulk(self.key.as_str()),
            bulk(self.group.as_str()),
            bulk(self.id.as_str()),
        ];
        if self.mkstream {
            args.push(bulk("MKSTREAM"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "XGROUP CREATE"
    }
}

/// XGROUP DESTROY key group
///
/// Destroys a consumer group.
pub struct XGroupDestroy {
    key: String,
    group: String,
}

impl XGroupDestroy {
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
        array(vec![
            bulk("XGROUP"),
            bulk("DESTROY"),
            bulk(self.key.as_str()),
            bulk(self.group.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "XGROUP DESTROY"
    }
}

/// XREADGROUP GROUP group consumer \[COUNT count\] \[BLOCK ms\] STREAMS key \[key ...\] id \[id ...\]
///
/// Read from streams as a consumer group member.
pub struct XReadGroup {
    group: String,
    consumer: String,
    streams: Vec<(String, String)>,
    count: Option<u64>,
    block: Option<u64>,
}

impl XReadGroup {
    /// Read new entries (id = ">") from a single stream.
    pub fn new(
        group: impl Into<String>,
        consumer: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            group: group.into(),
            consumer: consumer.into(),
            streams: vec![(key.into(), ">".to_string())],
            count: None,
            block: None,
        }
    }

    /// Add another stream to read from.
    pub fn stream(mut self, key: impl Into<String>, id: impl Into<String>) -> Self {
        self.streams.push((key.into(), id.into()));
        self
    }

    /// Limit the number of entries returned per stream.
    pub fn count(mut self, n: u64) -> Self {
        self.count = Some(n);
        self
    }

    /// Block for up to `ms` milliseconds. 0 = block indefinitely.
    pub fn block(mut self, ms: u64) -> Self {
        self.block = Some(ms);
        self
    }
}

impl Command for XReadGroup {
    type Response = Vec<(String, Vec<StreamEntry>)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("XREADGROUP"),
            bulk("GROUP"),
            bulk(self.group.as_str()),
            bulk(self.consumer.as_str()),
        ];
        if let Some(n) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(n.to_string()));
        }
        if let Some(ms) = self.block {
            args.push(bulk("BLOCK"));
            args.push(bulk(ms.to_string()));
        }
        args.push(bulk("STREAMS"));
        for (key, _) in &self.streams {
            args.push(bulk(key.as_str()));
        }
        for (_, id) in &self.streams {
            args.push(bulk(id.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_xread_response(&frame)
    }

    fn name(&self) -> &str {
        "XREADGROUP"
    }
}

/// XREAD \[COUNT count\] \[BLOCK ms\] STREAMS key \[key ...\] id \[id ...\]
///
/// Read from one or more streams.
pub struct XRead {
    streams: Vec<(String, String)>,
    count: Option<u64>,
    block: Option<u64>,
}

impl XRead {
    /// Read entries after `id` from a single stream. Use "$" for only new entries.
    pub fn new(key: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            streams: vec![(key.into(), id.into())],
            count: None,
            block: None,
        }
    }

    /// Add another stream to read from.
    pub fn stream(mut self, key: impl Into<String>, id: impl Into<String>) -> Self {
        self.streams.push((key.into(), id.into()));
        self
    }

    pub fn count(mut self, n: u64) -> Self {
        self.count = Some(n);
        self
    }

    pub fn block(mut self, ms: u64) -> Self {
        self.block = Some(ms);
        self
    }
}

impl Command for XRead {
    type Response = Option<Vec<(String, Vec<StreamEntry>)>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("XREAD")];
        if let Some(n) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(n.to_string()));
        }
        if let Some(ms) = self.block {
            args.push(bulk("BLOCK"));
            args.push(bulk(ms.to_string()));
        }
        args.push(bulk("STREAMS"));
        for (key, _) in &self.streams {
            args.push(bulk(key.as_str()));
        }
        for (_, id) in &self.streams {
            args.push(bulk(id.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match &frame {
            Frame::Null | Frame::Array(None) | Frame::BulkString(None) => Ok(None),
            _ => parse_xread_response(&frame).map(Some),
        }
    }

    fn name(&self) -> &str {
        "XREAD"
    }
}

// -- Response parsing helpers --

/// Parse a stream entry: \[id, \[field, value, field, value, ...\]\]
fn parse_stream_entry(frame: &Frame) -> Result<StreamEntry, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) if items.len() == 2 => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "stream entry array [id, [fields...]]",
                actual: format!("{other:?}"),
            });
        }
    };

    let id = match &items[0] {
        Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "bulk string entry ID",
                actual: format!("{other:?}"),
            });
        }
    };

    let field_values = match &items[1] {
        Frame::Array(Some(fvs)) => fvs,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of field-value pairs",
                actual: format!("{other:?}"),
            });
        }
    };

    if field_values.len() % 2 != 0 {
        return Err(RedisError::UnexpectedResponse {
            expected: "even number of field-value elements",
            actual: format!("{} elements", field_values.len()),
        });
    }

    let mut fields = Vec::with_capacity(field_values.len() / 2);
    for chunk in field_values.chunks(2) {
        let name = match &chunk[0] {
            Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "bulk string field name",
                    actual: format!("{other:?}"),
                });
            }
        };
        let value = match &chunk[1] {
            Frame::BulkString(Some(b)) => b.clone(),
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "bulk string field value",
                    actual: format!("{other:?}"),
                });
            }
        };
        fields.push((name, value));
    }

    Ok(StreamEntry { id, fields })
}

/// Parse a list of stream entries (from XRANGE/XREVRANGE).
fn parse_stream_entries(frame: &Frame) -> Result<Vec<StreamEntry>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) => items,
        Frame::Array(None) => return Ok(Vec::new()),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of stream entries",
                actual: format!("{other:?}"),
            });
        }
    };

    items.iter().map(parse_stream_entry).collect()
}

/// Parse XREAD/XREADGROUP response: \[\[stream_key, \[entries...\]\], ...\]
fn parse_xread_response(frame: &Frame) -> Result<Vec<(String, Vec<StreamEntry>)>, RedisError> {
    let streams = match frame {
        Frame::Array(Some(items)) => items,
        Frame::Array(None) => return Ok(Vec::new()),
        // RESP3 may return a Map.
        Frame::Map(entries) => {
            let mut result = Vec::new();
            for (key_frame, entries_frame) in entries {
                let key = match key_frame {
                    Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "bulk string stream key",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                let entries = parse_stream_entries(entries_frame)?;
                result.push((key, entries));
            }
            return Ok(result);
        }
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of [stream_key, [entries...]]",
                actual: format!("{other:?}"),
            });
        }
    };

    let mut result = Vec::new();
    for stream_frame in streams {
        let items = match stream_frame {
            Frame::Array(Some(items)) if items.len() == 2 => items,
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "stream [key, entries] pair",
                    actual: format!("{other:?}"),
                });
            }
        };

        let key = match &items[0] {
            Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "bulk string stream key",
                    actual: format!("{other:?}"),
                });
            }
        };

        let entries = parse_stream_entries(&items[1])?;
        result.push((key, entries));
    }

    Ok(result)
}
