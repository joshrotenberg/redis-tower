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

    /// Override the ID for all streams already added.
    ///
    /// Use `"0"` to read pending entries or `">"` for new entries.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        let id = id.into();
        for stream in &mut self.streams {
            stream.1 = id.clone();
        }
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
        match &frame {
            // BLOCK timeout or no pending entries returns Null.
            Frame::Null | Frame::Array(None) | Frame::BulkString(None) => Ok(Vec::new()),
            _ => parse_xread_response(&frame),
        }
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

/// XGROUP SETID key group id
///
/// Sets the last-delivered ID for a consumer group.
pub struct XGroupSetId {
    key: String,
    group: String,
    id: String,
}

impl XGroupSetId {
    pub fn new(key: impl Into<String>, group: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            id: id.into(),
        }
    }
}

impl Command for XGroupSetId {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("XGROUP"),
            bulk("SETID"),
            bulk(self.key.as_str()),
            bulk(self.group.as_str()),
            bulk(self.id.as_str()),
        ])
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
        "XGROUP SETID"
    }
}

/// XGROUP CREATECONSUMER key group consumer
///
/// Creates a consumer in a consumer group. Returns 1 if created, 0 if already existed.
pub struct XGroupCreateConsumer {
    key: String,
    group: String,
    consumer: String,
}

impl XGroupCreateConsumer {
    pub fn new(
        key: impl Into<String>,
        group: impl Into<String>,
        consumer: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            consumer: consumer.into(),
        }
    }
}

impl Command for XGroupCreateConsumer {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("XGROUP"),
            bulk("CREATECONSUMER"),
            bulk(self.key.as_str()),
            bulk(self.group.as_str()),
            bulk(self.consumer.as_str()),
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
        "XGROUP CREATECONSUMER"
    }
}

/// XGROUP DELCONSUMER key group consumer
///
/// Deletes a consumer from a consumer group. Returns the number of pending entries the consumer had.
pub struct XGroupDelConsumer {
    key: String,
    group: String,
    consumer: String,
}

impl XGroupDelConsumer {
    pub fn new(
        key: impl Into<String>,
        group: impl Into<String>,
        consumer: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            consumer: consumer.into(),
        }
    }
}

impl Command for XGroupDelConsumer {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("XGROUP"),
            bulk("DELCONSUMER"),
            bulk(self.key.as_str()),
            bulk(self.group.as_str()),
            bulk(self.consumer.as_str()),
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
        "XGROUP DELCONSUMER"
    }
}

/// XCLAIM key group consumer min-idle-time id \[id ...\] \[IDLE ms\] \[TIME ms\] \[RETRYCOUNT count\] \[FORCE\] \[JUSTID\]
///
/// Claims ownership of pending stream entries.
pub struct XClaim {
    key: String,
    group: String,
    consumer: String,
    min_idle_time: u64,
    ids: Vec<String>,
    idle: Option<u64>,
    time: Option<u64>,
    retrycount: Option<u64>,
    force: bool,
    justid: bool,
}

impl XClaim {
    pub fn new(
        key: impl Into<String>,
        group: impl Into<String>,
        consumer: impl Into<String>,
        min_idle_time: u64,
        ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            consumer: consumer.into(),
            min_idle_time,
            ids: ids.into_iter().map(Into::into).collect(),
            idle: None,
            time: None,
            retrycount: None,
            force: false,
            justid: false,
        }
    }

    /// Set the idle time (ms) for the claimed entries.
    pub fn idle(mut self, ms: u64) -> Self {
        self.idle = Some(ms);
        self
    }

    /// Set the last delivery time (ms unix timestamp).
    pub fn time(mut self, ms: u64) -> Self {
        self.time = Some(ms);
        self
    }

    /// Set the retry counter.
    pub fn retrycount(mut self, count: u64) -> Self {
        self.retrycount = Some(count);
        self
    }

    /// Force claim even if the entry is not in the PEL.
    pub fn force(mut self) -> Self {
        self.force = true;
        self
    }

    /// Return only IDs, not full entries.
    pub fn justid(mut self) -> Self {
        self.justid = true;
        self
    }
}

impl Command for XClaim {
    type Response = Vec<StreamEntry>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("XCLAIM"),
            bulk(self.key.as_str()),
            bulk(self.group.as_str()),
            bulk(self.consumer.as_str()),
            bulk(self.min_idle_time.to_string()),
        ];
        for id in &self.ids {
            args.push(bulk(id.as_str()));
        }
        if let Some(ms) = self.idle {
            args.push(bulk("IDLE"));
            args.push(bulk(ms.to_string()));
        }
        if let Some(ms) = self.time {
            args.push(bulk("TIME"));
            args.push(bulk(ms.to_string()));
        }
        if let Some(count) = self.retrycount {
            args.push(bulk("RETRYCOUNT"));
            args.push(bulk(count.to_string()));
        }
        if self.force {
            args.push(bulk("FORCE"));
        }
        if self.justid {
            args.push(bulk("JUSTID"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_stream_entries(&frame)
    }

    fn name(&self) -> &str {
        "XCLAIM"
    }
}

/// Result from XAUTOCLAIM: \[next-start-id, \[entries...\], \[deleted-ids...\]\]
#[derive(Debug, Clone, PartialEq)]
pub struct AutoClaimResult {
    pub next_start_id: String,
    pub entries: Vec<StreamEntry>,
    pub deleted_ids: Vec<String>,
}

/// XAUTOCLAIM key group consumer min-idle-time start \[COUNT count\] \[JUSTID\]
///
/// Automatically claims pending entries that have been idle for at least min-idle-time.
pub struct XAutoClaim {
    key: String,
    group: String,
    consumer: String,
    min_idle_time: u64,
    start: String,
    count: Option<u64>,
}

impl XAutoClaim {
    pub fn new(
        key: impl Into<String>,
        group: impl Into<String>,
        consumer: impl Into<String>,
        min_idle_time: u64,
        start: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            consumer: consumer.into(),
            min_idle_time,
            start: start.into(),
            count: None,
        }
    }

    /// Limit the number of entries to claim.
    pub fn count(mut self, n: u64) -> Self {
        self.count = Some(n);
        self
    }
}

impl Command for XAutoClaim {
    type Response = AutoClaimResult;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("XAUTOCLAIM"),
            bulk(self.key.as_str()),
            bulk(self.group.as_str()),
            bulk(self.consumer.as_str()),
            bulk(self.min_idle_time.to_string()),
            bulk(self.start.as_str()),
        ];
        if let Some(n) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(n.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_autoclaim_response(&frame)
    }

    fn name(&self) -> &str {
        "XAUTOCLAIM"
    }
}

/// Summary from XPENDING (no range): count, min-id, max-id, and per-consumer counts.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingSummary {
    pub count: i64,
    pub min_id: Option<String>,
    pub max_id: Option<String>,
    pub consumers: Vec<(String, i64)>,
}

/// XPENDING key group (summary form)
///
/// Returns a summary of pending entries for a consumer group.
pub struct XPendingSummary {
    key: String,
    group: String,
}

impl XPendingSummary {
    pub fn new(key: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
        }
    }
}

impl Command for XPendingSummary {
    type Response = PendingSummary;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("XPENDING"),
            bulk(self.key.as_str()),
            bulk(self.group.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_pending_summary(&frame)
    }

    fn name(&self) -> &str {
        "XPENDING"
    }
}

/// A pending entry detail from XPENDING range form.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingEntry {
    pub id: String,
    pub consumer: String,
    pub idle_ms: i64,
    pub delivery_count: i64,
}

/// XPENDING key group \[IDLE min-idle\] start end count \[consumer\]
///
/// Returns detailed pending entries for a consumer group.
pub struct XPendingRange {
    key: String,
    group: String,
    start: String,
    end: String,
    count: u64,
    consumer: Option<String>,
    idle: Option<u64>,
}

impl XPendingRange {
    pub fn new(
        key: impl Into<String>,
        group: impl Into<String>,
        start: impl Into<String>,
        end: impl Into<String>,
        count: u64,
    ) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
            start: start.into(),
            end: end.into(),
            count,
            consumer: None,
            idle: None,
        }
    }

    /// Filter by consumer name.
    pub fn consumer(mut self, consumer: impl Into<String>) -> Self {
        self.consumer = Some(consumer.into());
        self
    }

    /// Filter entries idle for at least `ms` milliseconds.
    pub fn idle(mut self, ms: u64) -> Self {
        self.idle = Some(ms);
        self
    }
}

impl Command for XPendingRange {
    type Response = Vec<PendingEntry>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("XPENDING"),
            bulk(self.key.as_str()),
            bulk(self.group.as_str()),
        ];
        if let Some(ms) = self.idle {
            args.push(bulk("IDLE"));
            args.push(bulk(ms.to_string()));
        }
        args.push(bulk(self.start.as_str()));
        args.push(bulk(self.end.as_str()));
        args.push(bulk(self.count.to_string()));
        if let Some(c) = &self.consumer {
            args.push(bulk(c.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_pending_range(&frame)
    }

    fn name(&self) -> &str {
        "XPENDING"
    }
}

/// Stream info from XINFO STREAM.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamInfo {
    pub length: i64,
    pub radix_tree_keys: i64,
    pub radix_tree_nodes: i64,
    pub last_generated_id: String,
    pub groups: i64,
    pub first_entry: Option<StreamEntry>,
    pub last_entry: Option<StreamEntry>,
}

/// XINFO STREAM key
///
/// Returns information about a stream.
pub struct XInfoStream {
    key: String,
}

impl XInfoStream {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for XInfoStream {
    type Response = StreamInfo;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("XINFO"), bulk("STREAM"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_xinfo_stream(&frame)
    }

    fn name(&self) -> &str {
        "XINFO STREAM"
    }
}

/// Consumer group info from XINFO GROUPS.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupInfo {
    pub name: String,
    pub consumers: i64,
    pub pending: i64,
    pub last_delivered_id: String,
}

/// XINFO GROUPS key
///
/// Returns information about the consumer groups of a stream.
pub struct XInfoGroups {
    key: String,
}

impl XInfoGroups {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for XInfoGroups {
    type Response = Vec<GroupInfo>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("XINFO"), bulk("GROUPS"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_xinfo_groups(&frame)
    }

    fn name(&self) -> &str {
        "XINFO GROUPS"
    }
}

/// Consumer info from XINFO CONSUMERS.
#[derive(Debug, Clone, PartialEq)]
pub struct ConsumerInfo {
    pub name: String,
    pub pending: i64,
    pub idle: i64,
}

/// XINFO CONSUMERS key group
///
/// Returns information about the consumers of a consumer group.
pub struct XInfoConsumers {
    key: String,
    group: String,
}

impl XInfoConsumers {
    pub fn new(key: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            group: group.into(),
        }
    }
}

impl Command for XInfoConsumers {
    type Response = Vec<ConsumerInfo>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("XINFO"),
            bulk("CONSUMERS"),
            bulk(self.key.as_str()),
            bulk(self.group.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_xinfo_consumers(&frame)
    }

    fn name(&self) -> &str {
        "XINFO CONSUMERS"
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

/// Parse XAUTOCLAIM response: \[next-start-id, \[entries...\], \[deleted-ids...\]\]
fn parse_autoclaim_response(frame: &Frame) -> Result<AutoClaimResult, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) if items.len() >= 2 => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array [next-start-id, [entries...], [deleted-ids...]]",
                actual: format!("{other:?}"),
            });
        }
    };

    let next_start_id = match &items[0] {
        Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "bulk string next-start-id",
                actual: format!("{other:?}"),
            });
        }
    };

    let entries = parse_stream_entries(&items[1])?;

    let deleted_ids = if items.len() > 2 {
        parse_string_array(&items[2])?
    } else {
        Vec::new()
    };

    Ok(AutoClaimResult {
        next_start_id,
        entries,
        deleted_ids,
    })
}

/// Parse an array of bulk strings into Vec<String>.
fn parse_string_array(frame: &Frame) -> Result<Vec<String>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) => items,
        Frame::Array(None) => return Ok(Vec::new()),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of strings",
                actual: format!("{other:?}"),
            });
        }
    };

    items
        .iter()
        .map(|f| match f {
            Frame::BulkString(Some(b)) => Ok(String::from_utf8_lossy(b).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        })
        .collect()
}

/// Parse XPENDING summary: \[count, min-id, max-id, \[\[consumer, count\], ...\]\]
fn parse_pending_summary(frame: &Frame) -> Result<PendingSummary, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) if items.len() == 4 => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array [count, min-id, max-id, [[consumer, count]...]]",
                actual: format!("{other:?}"),
            });
        }
    };

    let count = match &items[0] {
        Frame::Integer(n) => *n,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "integer count",
                actual: format!("{other:?}"),
            });
        }
    };

    // min-id and max-id are null when count is 0
    let min_id = match &items[1] {
        Frame::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        Frame::BulkString(None) | Frame::Null => None,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "bulk string or null min-id",
                actual: format!("{other:?}"),
            });
        }
    };

    let max_id = match &items[2] {
        Frame::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        Frame::BulkString(None) | Frame::Null => None,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "bulk string or null max-id",
                actual: format!("{other:?}"),
            });
        }
    };

    let consumers = match &items[3] {
        Frame::Array(Some(consumer_items)) => {
            let mut result = Vec::new();
            for item in consumer_items {
                let pair = match item {
                    Frame::Array(Some(pair)) if pair.len() == 2 => pair,
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "[consumer, count] pair",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                let name = match &pair[0] {
                    Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "bulk string consumer name",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                let cnt = match &pair[1] {
                    Frame::BulkString(Some(b)) => String::from_utf8_lossy(b)
                        .parse::<i64>()
                        .map_err(|_| RedisError::UnexpectedResponse {
                            expected: "numeric string count",
                            actual: String::from_utf8_lossy(b).into_owned(),
                        })?,
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "bulk string count",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                result.push((name, cnt));
            }
            result
        }
        Frame::Array(None) | Frame::Null => Vec::new(),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of [consumer, count] pairs",
                actual: format!("{other:?}"),
            });
        }
    };

    Ok(PendingSummary {
        count,
        min_id,
        max_id,
        consumers,
    })
}

/// Parse XPENDING range: \[\[id, consumer, idle-ms, delivery-count\], ...\]
fn parse_pending_range(frame: &Frame) -> Result<Vec<PendingEntry>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) => items,
        Frame::Array(None) => return Ok(Vec::new()),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of pending entries",
                actual: format!("{other:?}"),
            });
        }
    };

    let mut result = Vec::new();
    for item in items {
        let entry = match item {
            Frame::Array(Some(parts)) if parts.len() == 4 => parts,
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "[id, consumer, idle-ms, delivery-count]",
                    actual: format!("{other:?}"),
                });
            }
        };
        let id = match &entry[0] {
            Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "bulk string id",
                    actual: format!("{other:?}"),
                });
            }
        };
        let consumer = match &entry[1] {
            Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "bulk string consumer",
                    actual: format!("{other:?}"),
                });
            }
        };
        let idle_ms = match &entry[2] {
            Frame::Integer(n) => *n,
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "integer idle-ms",
                    actual: format!("{other:?}"),
                });
            }
        };
        let delivery_count = match &entry[3] {
            Frame::Integer(n) => *n,
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "integer delivery-count",
                    actual: format!("{other:?}"),
                });
            }
        };
        result.push(PendingEntry {
            id,
            consumer,
            idle_ms,
            delivery_count,
        });
    }
    Ok(result)
}

/// Extract a bulk string from a Frame.
fn extract_bulk_str(frame: &Frame) -> Result<String, RedisError> {
    match frame {
        Frame::BulkString(Some(b)) => Ok(String::from_utf8_lossy(b).into_owned()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "bulk string",
            actual: format!("{other:?}"),
        }),
    }
}

/// Extract an integer from a Frame.
fn extract_integer(frame: &Frame) -> Result<i64, RedisError> {
    match frame {
        Frame::Integer(n) => Ok(*n),
        other => Err(RedisError::UnexpectedResponse {
            expected: "integer",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse XINFO STREAM response (flat key-value array).
fn parse_xinfo_stream(frame: &Frame) -> Result<StreamInfo, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of key-value pairs (XINFO STREAM)",
                actual: format!("{other:?}"),
            });
        }
    };

    let mut length = 0i64;
    let mut radix_tree_keys = 0i64;
    let mut radix_tree_nodes = 0i64;
    let mut last_generated_id = String::new();
    let mut groups = 0i64;
    let mut first_entry = None;
    let mut last_entry = None;

    for chunk in items.chunks(2) {
        if chunk.len() < 2 {
            break;
        }
        let key = match &chunk[0] {
            Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).to_lowercase(),
            _ => continue,
        };
        match key.as_str() {
            "length" => length = extract_integer(&chunk[1])?,
            "radix-tree-keys" => radix_tree_keys = extract_integer(&chunk[1])?,
            "radix-tree-nodes" => radix_tree_nodes = extract_integer(&chunk[1])?,
            "last-generated-id" => last_generated_id = extract_bulk_str(&chunk[1])?,
            "groups" => groups = extract_integer(&chunk[1])?,
            "first-entry" => {
                first_entry = match &chunk[1] {
                    Frame::Null | Frame::Array(None) | Frame::BulkString(None) => None,
                    entry => Some(parse_stream_entry(entry)?),
                };
            }
            "last-entry" => {
                last_entry = match &chunk[1] {
                    Frame::Null | Frame::Array(None) | Frame::BulkString(None) => None,
                    entry => Some(parse_stream_entry(entry)?),
                };
            }
            _ => {} // skip unknown fields
        }
    }

    Ok(StreamInfo {
        length,
        radix_tree_keys,
        radix_tree_nodes,
        last_generated_id,
        groups,
        first_entry,
        last_entry,
    })
}

/// Parse XINFO GROUPS response: array of flat key-value arrays.
fn parse_xinfo_groups(frame: &Frame) -> Result<Vec<GroupInfo>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) => items,
        Frame::Array(None) => return Ok(Vec::new()),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of group info arrays",
                actual: format!("{other:?}"),
            });
        }
    };

    let mut result = Vec::new();
    for group_frame in items {
        let fields = match group_frame {
            Frame::Array(Some(f)) => f,
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "array of key-value pairs (group info)",
                    actual: format!("{other:?}"),
                });
            }
        };

        let mut name = String::new();
        let mut consumers = 0i64;
        let mut pending = 0i64;
        let mut last_delivered_id = String::new();

        for chunk in fields.chunks(2) {
            if chunk.len() < 2 {
                break;
            }
            let key = match &chunk[0] {
                Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).to_lowercase(),
                _ => continue,
            };
            match key.as_str() {
                "name" => name = extract_bulk_str(&chunk[1])?,
                "consumers" => consumers = extract_integer(&chunk[1])?,
                "pending" => pending = extract_integer(&chunk[1])?,
                "last-delivered-id" => last_delivered_id = extract_bulk_str(&chunk[1])?,
                _ => {}
            }
        }

        result.push(GroupInfo {
            name,
            consumers,
            pending,
            last_delivered_id,
        });
    }

    Ok(result)
}

/// Parse XINFO CONSUMERS response: array of flat key-value arrays.
fn parse_xinfo_consumers(frame: &Frame) -> Result<Vec<ConsumerInfo>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) => items,
        Frame::Array(None) => return Ok(Vec::new()),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of consumer info arrays",
                actual: format!("{other:?}"),
            });
        }
    };

    let mut result = Vec::new();
    for consumer_frame in items {
        let fields = match consumer_frame {
            Frame::Array(Some(f)) => f,
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "array of key-value pairs (consumer info)",
                    actual: format!("{other:?}"),
                });
            }
        };

        let mut name = String::new();
        let mut pending = 0i64;
        let mut idle = 0i64;

        for chunk in fields.chunks(2) {
            if chunk.len() < 2 {
                break;
            }
            let key = match &chunk[0] {
                Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).to_lowercase(),
                _ => continue,
            };
            match key.as_str() {
                "name" => name = extract_bulk_str(&chunk[1])?,
                "pending" => pending = extract_integer(&chunk[1])?,
                "idle" => idle = extract_integer(&chunk[1])?,
                _ => {}
            }
        }

        result.push(ConsumerInfo {
            name,
            pending,
            idle,
        });
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    // -- XAdd --

    #[test]
    fn xadd_basic_to_frame() {
        let cmd = XAdd::new("mystream")
            .field("name", "John")
            .field("age", "30");
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            array(vec![
                bulk("XADD"),
                bulk("mystream"),
                bulk("*"),
                bulk("name"),
                bulk("John"),
                bulk("age"),
                bulk("30"),
            ])
        );
    }

    #[test]
    fn xadd_with_options_to_frame() {
        let cmd = XAdd::new("mystream")
            .nomkstream()
            .maxlen_approx(1000)
            .field("k", "v");
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(Some(args)) => {
                assert_eq!(args[0], bulk("XADD"));
                assert_eq!(args[1], bulk("mystream"));
                assert_eq!(args[2], bulk("NOMKSTREAM"));
                assert_eq!(args[3], bulk("MAXLEN"));
                assert_eq!(args[4], bulk("~"));
                assert_eq!(args[5], bulk("1000"));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn xadd_with_specific_id() {
        let cmd = XAdd::new("mystream").id("1-1").field("k", "v");
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(Some(args)) => {
                assert!(args.contains(&bulk("1-1")));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn xadd_parse_response() {
        let cmd = XAdd::new("mystream").field("k", "v");
        let frame = Frame::BulkString(Some(Bytes::from("1526919030474-55")));
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, "1526919030474-55");
    }

    #[test]
    fn xadd_parse_error_on_integer() {
        let cmd = XAdd::new("mystream").field("k", "v");
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    // -- XLen --

    #[test]
    fn xlen_to_frame() {
        let cmd = XLen::new("mystream");
        assert_eq!(cmd.to_frame(), array(vec![bulk("XLEN"), bulk("mystream")]));
    }

    #[test]
    fn xlen_parse_integer() {
        let cmd = XLen::new("mystream");
        assert_eq!(cmd.parse_response(Frame::Integer(42)).unwrap(), 42);
    }

    // -- XRange --

    #[test]
    fn xrange_all_to_frame() {
        let cmd = XRange::all("mystream");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("XRANGE"), bulk("mystream"), bulk("-"), bulk("+"),])
        );
    }

    #[test]
    fn xrange_with_count_to_frame() {
        let cmd = XRange::all("mystream").count(10);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("XRANGE"),
                bulk("mystream"),
                bulk("-"),
                bulk("+"),
                bulk("COUNT"),
                bulk("10"),
            ])
        );
    }

    #[test]
    fn xrange_parse_entries() {
        let cmd = XRange::all("mystream");
        let entry = array(vec![
            Frame::BulkString(Some(Bytes::from("1-0"))),
            array(vec![
                Frame::BulkString(Some(Bytes::from("name"))),
                Frame::BulkString(Some(Bytes::from("Alice"))),
            ]),
        ]);
        let frame = array(vec![entry]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "1-0");
        assert_eq!(result[0].fields.len(), 1);
        assert_eq!(result[0].fields[0].0, "name");
        assert_eq!(result[0].fields[0].1, Bytes::from("Alice"));
    }

    #[test]
    fn xrange_parse_empty() {
        let cmd = XRange::all("mystream");
        let frame = Frame::Array(None);
        let result = cmd.parse_response(frame).unwrap();
        assert!(result.is_empty());
    }

    // -- XDel --

    #[test]
    fn xdel_to_frame() {
        let cmd = XDel::new("mystream", "1-0");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("XDEL"), bulk("mystream"), bulk("1-0")])
        );
    }

    #[test]
    fn xdel_parse_integer() {
        let cmd = XDel::new("mystream", "1-0");
        assert_eq!(cmd.parse_response(Frame::Integer(1)).unwrap(), 1);
    }

    // -- XTrim --

    #[test]
    fn xtrim_maxlen_to_frame() {
        let cmd = XTrim::maxlen("mystream", 100);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("XTRIM"),
                bulk("mystream"),
                bulk("MAXLEN"),
                bulk("100"),
            ])
        );
    }

    #[test]
    fn xtrim_maxlen_approx_to_frame() {
        let cmd = XTrim::maxlen_approx("mystream", 100);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("XTRIM"),
                bulk("mystream"),
                bulk("MAXLEN"),
                bulk("~"),
                bulk("100"),
            ])
        );
    }

    // -- XAck --

    #[test]
    fn xack_to_frame() {
        let cmd = XAck::new("mystream", "mygroup", "1-0");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("XACK"),
                bulk("mystream"),
                bulk("mygroup"),
                bulk("1-0"),
            ])
        );
    }

    #[test]
    fn xack_multiple_to_frame() {
        let cmd = XAck::ids("mystream", "mygroup", vec!["1-0", "2-0"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("XACK"),
                bulk("mystream"),
                bulk("mygroup"),
                bulk("1-0"),
                bulk("2-0"),
            ])
        );
    }

    #[test]
    fn xack_parse_integer() {
        let cmd = XAck::new("mystream", "mygroup", "1-0");
        assert_eq!(cmd.parse_response(Frame::Integer(1)).unwrap(), 1);
    }

    // -- XGroupCreate --

    #[test]
    fn xgroup_create_to_frame() {
        let cmd = XGroupCreate::new("mystream", "mygroup", "$");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("XGROUP"),
                bulk("CREATE"),
                bulk("mystream"),
                bulk("mygroup"),
                bulk("$"),
            ])
        );
    }

    #[test]
    fn xgroup_create_mkstream_to_frame() {
        let cmd = XGroupCreate::new("mystream", "mygroup", "0").mkstream();
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("XGROUP"),
                bulk("CREATE"),
                bulk("mystream"),
                bulk("mygroup"),
                bulk("0"),
                bulk("MKSTREAM"),
            ])
        );
    }

    #[test]
    fn xgroup_create_parse_ok() {
        let cmd = XGroupCreate::new("mystream", "mygroup", "$");
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }

    // -- XRead --

    #[test]
    fn xread_to_frame() {
        let cmd = XRead::new("mystream", "0").count(10);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("XREAD"),
                bulk("COUNT"),
                bulk("10"),
                bulk("STREAMS"),
                bulk("mystream"),
                bulk("0"),
            ])
        );
    }

    #[test]
    fn xread_parse_null() {
        let cmd = XRead::new("mystream", "$");
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    // -- XGroupDestroy --

    #[test]
    fn xgroup_destroy_to_frame() {
        let cmd = XGroupDestroy::new("mystream", "mygroup");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("XGROUP"),
                bulk("DESTROY"),
                bulk("mystream"),
                bulk("mygroup"),
            ])
        );
    }

    // -- XRevRange --

    #[test]
    fn xrevrange_to_frame() {
        let cmd = XRevRange::all("mystream");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("XREVRANGE"),
                bulk("mystream"),
                bulk("+"),
                bulk("-"),
            ])
        );
    }
}
