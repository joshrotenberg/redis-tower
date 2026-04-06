use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Aggregation type for TimeSeries range and multi-range queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TsAggregation {
    Avg,
    Sum,
    Min,
    Max,
    Range,
    Count,
    First,
    Last,
    StdP,
    StdS,
    VarP,
    VarS,
    Twa,
}

impl TsAggregation {
    fn as_str(&self) -> &str {
        match self {
            TsAggregation::Avg => "AVG",
            TsAggregation::Sum => "SUM",
            TsAggregation::Min => "MIN",
            TsAggregation::Max => "MAX",
            TsAggregation::Range => "RANGE",
            TsAggregation::Count => "COUNT",
            TsAggregation::First => "FIRST",
            TsAggregation::Last => "LAST",
            TsAggregation::StdP => "STD.P",
            TsAggregation::StdS => "STD.S",
            TsAggregation::VarP => "VAR.P",
            TsAggregation::VarS => "VAR.S",
            TsAggregation::Twa => "TWA",
        }
    }
}

/// Duplicate policy for TimeSeries keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TsDuplicatePolicy {
    Block,
    First,
    Last,
    Min,
    Max,
    Sum,
}

impl TsDuplicatePolicy {
    fn as_str(&self) -> &str {
        match self {
            TsDuplicatePolicy::Block => "BLOCK",
            TsDuplicatePolicy::First => "FIRST",
            TsDuplicatePolicy::Last => "LAST",
            TsDuplicatePolicy::Min => "MIN",
            TsDuplicatePolicy::Max => "MAX",
            TsDuplicatePolicy::Sum => "SUM",
        }
    }
}

/// Encoding for TimeSeries keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TsEncoding {
    Compressed,
    Uncompressed,
}

impl TsEncoding {
    fn as_str(&self) -> &str {
        match self {
            TsEncoding::Compressed => "COMPRESSED",
            TsEncoding::Uncompressed => "UNCOMPRESSED",
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: push labels onto an args vec
// ---------------------------------------------------------------------------

fn push_labels(args: &mut Vec<Frame>, labels: &[(String, String)]) {
    if !labels.is_empty() {
        args.push(bulk("LABELS"));
        for (k, v) in labels {
            args.push(bulk(k.as_str()));
            args.push(bulk(v.as_str()));
        }
    }
}

// ---------------------------------------------------------------------------
// TS.CREATE
// ---------------------------------------------------------------------------

/// TS.CREATE key \[RETENTION ms\] \[ENCODING enc\] \[CHUNK_SIZE bytes\]
/// \[DUPLICATE_POLICY policy\] \[LABELS label value ...\]
///
/// Creates a new TimeSeries key.
pub struct TsCreate {
    key: String,
    retention: Option<u64>,
    encoding: Option<TsEncoding>,
    chunk_size: Option<u64>,
    duplicate_policy: Option<TsDuplicatePolicy>,
    labels: Vec<(String, String)>,
}

impl TsCreate {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            retention: None,
            encoding: None,
            chunk_size: None,
            duplicate_policy: None,
            labels: Vec::new(),
        }
    }

    /// Set the retention period in milliseconds.
    pub fn retention(mut self, ms: u64) -> Self {
        self.retention = Some(ms);
        self
    }

    /// Set the encoding type.
    pub fn encoding(mut self, enc: TsEncoding) -> Self {
        self.encoding = Some(enc);
        self
    }

    /// Set the chunk size in bytes.
    pub fn chunk_size(mut self, bytes: u64) -> Self {
        self.chunk_size = Some(bytes);
        self
    }

    /// Set the duplicate policy.
    pub fn duplicate_policy(mut self, policy: TsDuplicatePolicy) -> Self {
        self.duplicate_policy = Some(policy);
        self
    }

    /// Add a label key-value pair.
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }
}

impl Command for TsCreate {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TS.CREATE"), bulk(self.key.as_str())];
        if let Some(retention) = self.retention {
            args.push(bulk("RETENTION"));
            args.push(bulk(retention.to_string()));
        }
        if let Some(enc) = &self.encoding {
            args.push(bulk("ENCODING"));
            args.push(bulk(enc.as_str()));
        }
        if let Some(cs) = self.chunk_size {
            args.push(bulk("CHUNK_SIZE"));
            args.push(bulk(cs.to_string()));
        }
        if let Some(dp) = &self.duplicate_policy {
            args.push(bulk("DUPLICATE_POLICY"));
            args.push(bulk(dp.as_str()));
        }
        push_labels(&mut args, &self.labels);
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
        "TS.CREATE"
    }
}

// ---------------------------------------------------------------------------
// TS.ALTER
// ---------------------------------------------------------------------------

/// TS.ALTER key \[RETENTION ms\] \[CHUNK_SIZE bytes\]
/// \[DUPLICATE_POLICY policy\] \[LABELS label value ...\]
///
/// Alters an existing TimeSeries key configuration.
pub struct TsAlter {
    key: String,
    retention: Option<u64>,
    chunk_size: Option<u64>,
    duplicate_policy: Option<TsDuplicatePolicy>,
    labels: Vec<(String, String)>,
}

impl TsAlter {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            retention: None,
            chunk_size: None,
            duplicate_policy: None,
            labels: Vec::new(),
        }
    }

    /// Set the retention period in milliseconds.
    pub fn retention(mut self, ms: u64) -> Self {
        self.retention = Some(ms);
        self
    }

    /// Set the chunk size in bytes.
    pub fn chunk_size(mut self, bytes: u64) -> Self {
        self.chunk_size = Some(bytes);
        self
    }

    /// Set the duplicate policy.
    pub fn duplicate_policy(mut self, policy: TsDuplicatePolicy) -> Self {
        self.duplicate_policy = Some(policy);
        self
    }

    /// Add a label key-value pair.
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }
}

impl Command for TsAlter {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TS.ALTER"), bulk(self.key.as_str())];
        if let Some(retention) = self.retention {
            args.push(bulk("RETENTION"));
            args.push(bulk(retention.to_string()));
        }
        if let Some(cs) = self.chunk_size {
            args.push(bulk("CHUNK_SIZE"));
            args.push(bulk(cs.to_string()));
        }
        if let Some(dp) = &self.duplicate_policy {
            args.push(bulk("DUPLICATE_POLICY"));
            args.push(bulk(dp.as_str()));
        }
        push_labels(&mut args, &self.labels);
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
        "TS.ALTER"
    }
}

// ---------------------------------------------------------------------------
// TS.DEL
// ---------------------------------------------------------------------------

/// TS.DEL key from_timestamp to_timestamp
///
/// Deletes samples between two timestamps (inclusive). Returns the number
/// of samples deleted.
pub struct TsDel {
    key: String,
    from: i64,
    to: i64,
}

impl TsDel {
    pub fn new(key: impl Into<String>, from: i64, to: i64) -> Self {
        Self {
            key: key.into(),
            from,
            to,
        }
    }
}

impl Command for TsDel {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("TS.DEL"),
            bulk(self.key.as_str()),
            bulk(self.from.to_string()),
            bulk(self.to.to_string()),
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
        "TS.DEL"
    }
}

// ---------------------------------------------------------------------------
// TS.ADD
// ---------------------------------------------------------------------------

/// Timestamp value for TS.ADD. Use `Auto` for server-assigned timestamps
/// or `Value(ms)` for explicit millisecond timestamps.
#[derive(Debug, Clone, Copy)]
pub enum TsTimestamp {
    /// Let the server assign a timestamp ("*").
    Auto,
    /// Explicit timestamp in milliseconds.
    Value(i64),
}

impl TsTimestamp {
    fn to_bulk(self) -> Frame {
        match self {
            TsTimestamp::Auto => bulk("*"),
            TsTimestamp::Value(ts) => bulk(ts.to_string()),
        }
    }
}

impl From<i64> for TsTimestamp {
    fn from(ts: i64) -> Self {
        TsTimestamp::Value(ts)
    }
}

/// TS.ADD key timestamp value \[RETENTION ms\] \[ENCODING enc\]
/// \[CHUNK_SIZE bytes\] \[ON_DUPLICATE policy\] \[LABELS label value ...\]
///
/// Appends a sample to a TimeSeries key. Returns the timestamp of the
/// added sample.
pub struct TsAdd {
    key: String,
    timestamp: TsTimestamp,
    value: f64,
    retention: Option<u64>,
    encoding: Option<TsEncoding>,
    chunk_size: Option<u64>,
    on_duplicate: Option<TsDuplicatePolicy>,
    labels: Vec<(String, String)>,
}

impl TsAdd {
    pub fn new(key: impl Into<String>, timestamp: impl Into<TsTimestamp>, value: f64) -> Self {
        Self {
            key: key.into(),
            timestamp: timestamp.into(),
            value,
            retention: None,
            encoding: None,
            chunk_size: None,
            on_duplicate: None,
            labels: Vec::new(),
        }
    }

    /// Set the retention period in milliseconds.
    pub fn retention(mut self, ms: u64) -> Self {
        self.retention = Some(ms);
        self
    }

    /// Set the encoding type.
    pub fn encoding(mut self, enc: TsEncoding) -> Self {
        self.encoding = Some(enc);
        self
    }

    /// Set the chunk size in bytes.
    pub fn chunk_size(mut self, bytes: u64) -> Self {
        self.chunk_size = Some(bytes);
        self
    }

    /// Set the on-duplicate policy.
    pub fn on_duplicate(mut self, policy: TsDuplicatePolicy) -> Self {
        self.on_duplicate = Some(policy);
        self
    }

    /// Add a label key-value pair.
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }
}

impl Command for TsAdd {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("TS.ADD"),
            bulk(self.key.as_str()),
            self.timestamp.to_bulk(),
            bulk(self.value.to_string()),
        ];
        if let Some(retention) = self.retention {
            args.push(bulk("RETENTION"));
            args.push(bulk(retention.to_string()));
        }
        if let Some(enc) = &self.encoding {
            args.push(bulk("ENCODING"));
            args.push(bulk(enc.as_str()));
        }
        if let Some(cs) = self.chunk_size {
            args.push(bulk("CHUNK_SIZE"));
            args.push(bulk(cs.to_string()));
        }
        if let Some(dp) = &self.on_duplicate {
            args.push(bulk("ON_DUPLICATE"));
            args.push(bulk(dp.as_str()));
        }
        push_labels(&mut args, &self.labels);
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
        "TS.ADD"
    }
}

// ---------------------------------------------------------------------------
// TS.MADD
// ---------------------------------------------------------------------------

/// TS.MADD key timestamp value \[key timestamp value ...\]
///
/// Appends samples to multiple TimeSeries keys. Returns the raw array of
/// timestamps (or errors per sample).
pub struct TsMAdd {
    samples: Vec<(String, TsTimestamp, f64)>,
}

impl TsMAdd {
    pub fn new() -> Self {
        Self {
            samples: Vec::new(),
        }
    }

    /// Add a sample (key, timestamp, value) to the batch.
    pub fn sample(
        mut self,
        key: impl Into<String>,
        timestamp: impl Into<TsTimestamp>,
        value: f64,
    ) -> Self {
        self.samples.push((key.into(), timestamp.into(), value));
        self
    }
}

impl Default for TsMAdd {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for TsMAdd {
    type Response = Vec<Frame>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TS.MADD")];
        for (key, ts, value) in &self.samples {
            args.push(bulk(key.as_str()));
            args.push(ts.to_bulk());
            args.push(bulk(value.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => Ok(frames),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "TS.MADD"
    }
}

// ---------------------------------------------------------------------------
// TS.INCRBY / TS.DECRBY (shared structure)
// ---------------------------------------------------------------------------

/// TS.INCRBY key value \[TIMESTAMP ts\] \[RETENTION ms\] \[LABELS ...\]
///
/// Increments the value of the latest sample in a TimeSeries key.
/// Creates the key if it does not exist. Returns the timestamp.
pub struct TsIncrBy {
    key: String,
    value: f64,
    timestamp: Option<TsTimestamp>,
    retention: Option<u64>,
    labels: Vec<(String, String)>,
}

impl TsIncrBy {
    pub fn new(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value,
            timestamp: None,
            retention: None,
            labels: Vec::new(),
        }
    }

    /// Set an explicit timestamp.
    pub fn timestamp(mut self, ts: impl Into<TsTimestamp>) -> Self {
        self.timestamp = Some(ts.into());
        self
    }

    /// Set the retention period in milliseconds.
    pub fn retention(mut self, ms: u64) -> Self {
        self.retention = Some(ms);
        self
    }

    /// Add a label key-value pair.
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }
}

impl Command for TsIncrBy {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("TS.INCRBY"),
            bulk(self.key.as_str()),
            bulk(self.value.to_string()),
        ];
        if let Some(ts) = &self.timestamp {
            args.push(bulk("TIMESTAMP"));
            args.push(ts.to_bulk());
        }
        if let Some(retention) = self.retention {
            args.push(bulk("RETENTION"));
            args.push(bulk(retention.to_string()));
        }
        push_labels(&mut args, &self.labels);
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
        "TS.INCRBY"
    }
}

/// TS.DECRBY key value \[TIMESTAMP ts\] \[RETENTION ms\] \[LABELS ...\]
///
/// Decrements the value of the latest sample in a TimeSeries key.
/// Creates the key if it does not exist. Returns the timestamp.
pub struct TsDecrBy {
    key: String,
    value: f64,
    timestamp: Option<TsTimestamp>,
    retention: Option<u64>,
    labels: Vec<(String, String)>,
}

impl TsDecrBy {
    pub fn new(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value,
            timestamp: None,
            retention: None,
            labels: Vec::new(),
        }
    }

    /// Set an explicit timestamp.
    pub fn timestamp(mut self, ts: impl Into<TsTimestamp>) -> Self {
        self.timestamp = Some(ts.into());
        self
    }

    /// Set the retention period in milliseconds.
    pub fn retention(mut self, ms: u64) -> Self {
        self.retention = Some(ms);
        self
    }

    /// Add a label key-value pair.
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }
}

impl Command for TsDecrBy {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("TS.DECRBY"),
            bulk(self.key.as_str()),
            bulk(self.value.to_string()),
        ];
        if let Some(ts) = &self.timestamp {
            args.push(bulk("TIMESTAMP"));
            args.push(ts.to_bulk());
        }
        if let Some(retention) = self.retention {
            args.push(bulk("RETENTION"));
            args.push(bulk(retention.to_string()));
        }
        push_labels(&mut args, &self.labels);
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
        "TS.DECRBY"
    }
}

// ---------------------------------------------------------------------------
// TS.GET
// ---------------------------------------------------------------------------

/// TS.GET key \[LATEST\]
///
/// Returns the last sample of a TimeSeries key as a raw Frame
/// (timestamp-value pair, or empty if the key has no samples).
pub struct TsGet {
    key: String,
    latest: bool,
}

impl TsGet {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            latest: false,
        }
    }

    /// Include the latest (possibly un-compacted) bucket.
    pub fn latest(mut self) -> Self {
        self.latest = true;
        self
    }
}

impl Command for TsGet {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TS.GET"), bulk(self.key.as_str())];
        if self.latest {
            args.push(bulk("LATEST"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "TS.GET"
    }
}

// ---------------------------------------------------------------------------
// TS.MGET
// ---------------------------------------------------------------------------

/// TS.MGET \[LATEST\] \[WITHLABELS\] FILTER filter_expr \[filter_expr ...\]
///
/// Returns the last sample of multiple TimeSeries keys matching the given
/// filter expressions. Returns the raw Frame.
pub struct TsMGet {
    latest: bool,
    withlabels: bool,
    filters: Vec<String>,
}

impl TsMGet {
    pub fn new(filter: impl Into<String>) -> Self {
        Self {
            latest: false,
            withlabels: false,
            filters: vec![filter.into()],
        }
    }

    /// Add an additional filter expression.
    pub fn filter(mut self, expr: impl Into<String>) -> Self {
        self.filters.push(expr.into());
        self
    }

    /// Include the latest (possibly un-compacted) bucket.
    pub fn latest(mut self) -> Self {
        self.latest = true;
        self
    }

    /// Include labels in the response.
    pub fn withlabels(mut self) -> Self {
        self.withlabels = true;
        self
    }
}

impl Command for TsMGet {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TS.MGET")];
        if self.latest {
            args.push(bulk("LATEST"));
        }
        if self.withlabels {
            args.push(bulk("WITHLABELS"));
        }
        args.push(bulk("FILTER"));
        for f in &self.filters {
            args.push(bulk(f.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "TS.MGET"
    }
}

// ---------------------------------------------------------------------------
// TS.RANGE / TS.REVRANGE (shared builder logic)
// ---------------------------------------------------------------------------

/// Common options for TS.RANGE and TS.REVRANGE.
struct TsRangeOptions {
    key: String,
    from: String,
    to: String,
    latest: bool,
    filter_by_ts: Vec<i64>,
    filter_by_value: Option<(f64, f64)>,
    count: Option<i64>,
    aggregation: Option<(TsAggregation, i64)>,
}

/// TS.RANGE key from to \[LATEST\] \[FILTER_BY_TS ts ...\]
/// \[FILTER_BY_VALUE min max\] \[COUNT count\] \[AGGREGATION agg timebucket\]
///
/// Queries a range of samples from a TimeSeries key in chronological
/// order. Returns the raw Frame.
pub struct TsRange {
    opts: TsRangeOptions,
}

impl TsRange {
    /// Create a range query. `from` and `to` are timestamp strings
    /// (use "-" for minimum, "+" for maximum, or a millisecond timestamp).
    pub fn new(key: impl Into<String>, from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            opts: TsRangeOptions {
                key: key.into(),
                from: from.into(),
                to: to.into(),
                latest: false,
                filter_by_ts: Vec::new(),
                filter_by_value: None,
                count: None,
                aggregation: None,
            },
        }
    }

    /// Include the latest (possibly un-compacted) bucket.
    pub fn latest(mut self) -> Self {
        self.opts.latest = true;
        self
    }

    /// Filter results to specific timestamps.
    pub fn filter_by_ts(mut self, timestamps: impl IntoIterator<Item = i64>) -> Self {
        self.opts.filter_by_ts = timestamps.into_iter().collect();
        self
    }

    /// Filter results to a value range.
    pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
        self.opts.filter_by_value = Some((min, max));
        self
    }

    /// Limit the number of returned samples.
    pub fn count(mut self, count: i64) -> Self {
        self.opts.count = Some(count);
        self
    }

    /// Apply an aggregation with the given time bucket (in milliseconds).
    pub fn aggregation(mut self, agg: TsAggregation, time_bucket: i64) -> Self {
        self.opts.aggregation = Some((agg, time_bucket));
        self
    }
}

fn push_range_args(args: &mut Vec<Frame>, opts: &TsRangeOptions) {
    args.push(bulk(opts.key.as_str()));
    args.push(bulk(opts.from.as_str()));
    args.push(bulk(opts.to.as_str()));
    if opts.latest {
        args.push(bulk("LATEST"));
    }
    if !opts.filter_by_ts.is_empty() {
        args.push(bulk("FILTER_BY_TS"));
        for ts in &opts.filter_by_ts {
            args.push(bulk(ts.to_string()));
        }
    }
    if let Some((min, max)) = opts.filter_by_value {
        args.push(bulk("FILTER_BY_VALUE"));
        args.push(bulk(min.to_string()));
        args.push(bulk(max.to_string()));
    }
    if let Some(count) = opts.count {
        args.push(bulk("COUNT"));
        args.push(bulk(count.to_string()));
    }
    if let Some((agg, bucket)) = &opts.aggregation {
        args.push(bulk("AGGREGATION"));
        args.push(bulk(agg.as_str()));
        args.push(bulk(bucket.to_string()));
    }
}

impl Command for TsRange {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TS.RANGE")];
        push_range_args(&mut args, &self.opts);
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "TS.RANGE"
    }
}

/// TS.REVRANGE key from to \[LATEST\] \[FILTER_BY_TS ts ...\]
/// \[FILTER_BY_VALUE min max\] \[COUNT count\] \[AGGREGATION agg timebucket\]
///
/// Queries a range of samples from a TimeSeries key in reverse
/// chronological order. Returns the raw Frame.
pub struct TsRevRange {
    opts: TsRangeOptions,
}

impl TsRevRange {
    /// Create a reverse range query. `from` and `to` are timestamp strings
    /// (use "-" for minimum, "+" for maximum, or a millisecond timestamp).
    pub fn new(key: impl Into<String>, from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            opts: TsRangeOptions {
                key: key.into(),
                from: from.into(),
                to: to.into(),
                latest: false,
                filter_by_ts: Vec::new(),
                filter_by_value: None,
                count: None,
                aggregation: None,
            },
        }
    }

    /// Include the latest (possibly un-compacted) bucket.
    pub fn latest(mut self) -> Self {
        self.opts.latest = true;
        self
    }

    /// Filter results to specific timestamps.
    pub fn filter_by_ts(mut self, timestamps: impl IntoIterator<Item = i64>) -> Self {
        self.opts.filter_by_ts = timestamps.into_iter().collect();
        self
    }

    /// Filter results to a value range.
    pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
        self.opts.filter_by_value = Some((min, max));
        self
    }

    /// Limit the number of returned samples.
    pub fn count(mut self, count: i64) -> Self {
        self.opts.count = Some(count);
        self
    }

    /// Apply an aggregation with the given time bucket (in milliseconds).
    pub fn aggregation(mut self, agg: TsAggregation, time_bucket: i64) -> Self {
        self.opts.aggregation = Some((agg, time_bucket));
        self
    }
}

impl Command for TsRevRange {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TS.REVRANGE")];
        push_range_args(&mut args, &self.opts);
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "TS.REVRANGE"
    }
}

// ---------------------------------------------------------------------------
// TS.MRANGE / TS.MREVRANGE (shared builder logic)
// ---------------------------------------------------------------------------

/// Common options for TS.MRANGE and TS.MREVRANGE.
struct TsMRangeOptions {
    from: String,
    to: String,
    latest: bool,
    withlabels: bool,
    filter_by_ts: Vec<i64>,
    filter_by_value: Option<(f64, f64)>,
    count: Option<i64>,
    aggregation: Option<(TsAggregation, i64)>,
    filters: Vec<String>,
}

fn push_mrange_args(args: &mut Vec<Frame>, opts: &TsMRangeOptions) {
    args.push(bulk(opts.from.as_str()));
    args.push(bulk(opts.to.as_str()));
    if opts.latest {
        args.push(bulk("LATEST"));
    }
    if !opts.filter_by_ts.is_empty() {
        args.push(bulk("FILTER_BY_TS"));
        for ts in &opts.filter_by_ts {
            args.push(bulk(ts.to_string()));
        }
    }
    if let Some((min, max)) = opts.filter_by_value {
        args.push(bulk("FILTER_BY_VALUE"));
        args.push(bulk(min.to_string()));
        args.push(bulk(max.to_string()));
    }
    if opts.withlabels {
        args.push(bulk("WITHLABELS"));
    }
    if let Some(count) = opts.count {
        args.push(bulk("COUNT"));
        args.push(bulk(count.to_string()));
    }
    if let Some((agg, bucket)) = &opts.aggregation {
        args.push(bulk("AGGREGATION"));
        args.push(bulk(agg.as_str()));
        args.push(bulk(bucket.to_string()));
    }
    args.push(bulk("FILTER"));
    for f in &opts.filters {
        args.push(bulk(f.as_str()));
    }
}

/// TS.MRANGE from to \[LATEST\] \[WITHLABELS\] \[FILTER_BY_TS ts ...\]
/// \[FILTER_BY_VALUE min max\] \[COUNT count\] \[AGGREGATION agg timebucket\]
/// FILTER filter_expr \[filter_expr ...\]
///
/// Queries a range of samples from multiple TimeSeries keys matching
/// the given filter expressions. Returns the raw Frame.
pub struct TsMRange {
    opts: TsMRangeOptions,
}

impl TsMRange {
    /// Create a multi-key range query.
    pub fn new(from: impl Into<String>, to: impl Into<String>, filter: impl Into<String>) -> Self {
        Self {
            opts: TsMRangeOptions {
                from: from.into(),
                to: to.into(),
                latest: false,
                withlabels: false,
                filter_by_ts: Vec::new(),
                filter_by_value: None,
                count: None,
                aggregation: None,
                filters: vec![filter.into()],
            },
        }
    }

    /// Add an additional filter expression.
    pub fn filter(mut self, expr: impl Into<String>) -> Self {
        self.opts.filters.push(expr.into());
        self
    }

    /// Include the latest (possibly un-compacted) bucket.
    pub fn latest(mut self) -> Self {
        self.opts.latest = true;
        self
    }

    /// Include labels in the response.
    pub fn withlabels(mut self) -> Self {
        self.opts.withlabels = true;
        self
    }

    /// Filter results to specific timestamps.
    pub fn filter_by_ts(mut self, timestamps: impl IntoIterator<Item = i64>) -> Self {
        self.opts.filter_by_ts = timestamps.into_iter().collect();
        self
    }

    /// Filter results to a value range.
    pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
        self.opts.filter_by_value = Some((min, max));
        self
    }

    /// Limit the number of returned samples per key.
    pub fn count(mut self, count: i64) -> Self {
        self.opts.count = Some(count);
        self
    }

    /// Apply an aggregation with the given time bucket (in milliseconds).
    pub fn aggregation(mut self, agg: TsAggregation, time_bucket: i64) -> Self {
        self.opts.aggregation = Some((agg, time_bucket));
        self
    }
}

impl Command for TsMRange {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TS.MRANGE")];
        push_mrange_args(&mut args, &self.opts);
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "TS.MRANGE"
    }
}

/// TS.MREVRANGE from to \[LATEST\] \[WITHLABELS\] \[FILTER_BY_TS ts ...\]
/// \[FILTER_BY_VALUE min max\] \[COUNT count\] \[AGGREGATION agg timebucket\]
/// FILTER filter_expr \[filter_expr ...\]
///
/// Queries a range of samples from multiple TimeSeries keys in reverse
/// chronological order. Returns the raw Frame.
pub struct TsMRevRange {
    opts: TsMRangeOptions,
}

impl TsMRevRange {
    /// Create a multi-key reverse range query.
    pub fn new(from: impl Into<String>, to: impl Into<String>, filter: impl Into<String>) -> Self {
        Self {
            opts: TsMRangeOptions {
                from: from.into(),
                to: to.into(),
                latest: false,
                withlabels: false,
                filter_by_ts: Vec::new(),
                filter_by_value: None,
                count: None,
                aggregation: None,
                filters: vec![filter.into()],
            },
        }
    }

    /// Add an additional filter expression.
    pub fn filter(mut self, expr: impl Into<String>) -> Self {
        self.opts.filters.push(expr.into());
        self
    }

    /// Include the latest (possibly un-compacted) bucket.
    pub fn latest(mut self) -> Self {
        self.opts.latest = true;
        self
    }

    /// Include labels in the response.
    pub fn withlabels(mut self) -> Self {
        self.opts.withlabels = true;
        self
    }

    /// Filter results to specific timestamps.
    pub fn filter_by_ts(mut self, timestamps: impl IntoIterator<Item = i64>) -> Self {
        self.opts.filter_by_ts = timestamps.into_iter().collect();
        self
    }

    /// Filter results to a value range.
    pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
        self.opts.filter_by_value = Some((min, max));
        self
    }

    /// Limit the number of returned samples per key.
    pub fn count(mut self, count: i64) -> Self {
        self.opts.count = Some(count);
        self
    }

    /// Apply an aggregation with the given time bucket (in milliseconds).
    pub fn aggregation(mut self, agg: TsAggregation, time_bucket: i64) -> Self {
        self.opts.aggregation = Some((agg, time_bucket));
        self
    }
}

impl Command for TsMRevRange {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TS.MREVRANGE")];
        push_mrange_args(&mut args, &self.opts);
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "TS.MREVRANGE"
    }
}

// ---------------------------------------------------------------------------
// TS.INFO
// ---------------------------------------------------------------------------

/// TS.INFO key \[DEBUG\]
///
/// Returns information and statistics about a TimeSeries key. Returns
/// the raw Frame (complex nested structure).
pub struct TsInfo {
    key: String,
    debug: bool,
}

impl TsInfo {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            debug: false,
        }
    }

    /// Include debug information in the response.
    pub fn debug(mut self) -> Self {
        self.debug = true;
        self
    }
}

impl Command for TsInfo {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TS.INFO"), bulk(self.key.as_str())];
        if self.debug {
            args.push(bulk("DEBUG"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "TS.INFO"
    }
}

// ---------------------------------------------------------------------------
// TS.QUERYINDEX
// ---------------------------------------------------------------------------

/// TS.QUERYINDEX filter_expr \[filter_expr ...\]
///
/// Returns the keys matching the given filter expressions.
pub struct TsQueryIndex {
    filters: Vec<String>,
}

impl TsQueryIndex {
    pub fn new(filter: impl Into<String>) -> Self {
        Self {
            filters: vec![filter.into()],
        }
    }

    /// Add an additional filter expression.
    pub fn filter(mut self, expr: impl Into<String>) -> Self {
        self.filters.push(expr.into());
        self
    }
}

impl Command for TsQueryIndex {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TS.QUERYINDEX")];
        for f in &self.filters {
            args.push(bulk(f.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(data)) => Ok(data),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string",
                        actual: format!("{other:?}"),
                    }),
                })
                .collect(),
            Frame::Array(None) => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "TS.QUERYINDEX"
    }
}
