use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

// ---------------------------------------------------------------------------
// Helper: parse an array of integers as Vec<bool>
// ---------------------------------------------------------------------------

fn parse_bool_array(frame: Frame) -> Result<Vec<bool>, RedisError> {
    match frame {
        Frame::Array(Some(frames)) => frames
            .into_iter()
            .map(|f| match f {
                Frame::Integer(n) => Ok(n != 0),
                Frame::Boolean(b) => Ok(b),
                other => Err(RedisError::UnexpectedResponse {
                    expected: "integer or boolean",
                    actual: format!("{other:?}"),
                }),
            })
            .collect(),
        other => Err(RedisError::UnexpectedResponse {
            expected: "array",
            actual: format!("{other:?}"),
        }),
    }
}

// ===========================================================================
// Bloom filter commands
// ===========================================================================

/// BF.ADD key item
///
/// Adds an item to the Bloom filter at `key`. Returns `true` if the item was
/// newly added, `false` if it may have existed previously.
#[derive(Clone)]
pub struct BfAdd {
    key: String,
    item: String,
}

impl BfAdd {
    pub fn new(key: impl Into<String>, item: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for BfAdd {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("BF.ADD"),
            bulk(self.key.as_str()),
            bulk(self.item.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n != 0),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "BF.ADD"
    }
}

/// BF.EXISTS key item
///
/// Checks whether an item may exist in the Bloom filter at `key`.
#[derive(Clone)]
pub struct BfExists {
    key: String,
    item: String,
}

impl BfExists {
    pub fn new(key: impl Into<String>, item: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for BfExists {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("BF.EXISTS"),
            bulk(self.key.as_str()),
            bulk(self.item.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n != 0),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "BF.EXISTS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// BF.MADD key item \[item ...\]
///
/// Adds one or more items to the Bloom filter at `key`. Returns a vector of
/// booleans indicating whether each item was newly added.
#[derive(Clone)]
pub struct BfMAdd {
    key: String,
    items: Vec<String>,
}

impl BfMAdd {
    pub fn new(key: impl Into<String>, items: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            items: items.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for BfMAdd {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("BF.MADD"), bulk(self.key.as_str())];
        for item in &self.items {
            args.push(bulk(item.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_bool_array(frame)
    }

    fn name(&self) -> &str {
        "BF.MADD"
    }
}

/// BF.MEXISTS key item \[item ...\]
///
/// Checks whether one or more items may exist in the Bloom filter at `key`.
#[derive(Clone)]
pub struct BfMExists {
    key: String,
    items: Vec<String>,
}

impl BfMExists {
    pub fn new(key: impl Into<String>, items: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            items: items.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for BfMExists {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("BF.MEXISTS"), bulk(self.key.as_str())];
        for item in &self.items {
            args.push(bulk(item.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_bool_array(frame)
    }

    fn name(&self) -> &str {
        "BF.MEXISTS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// BF.RESERVE key error_rate capacity \[EXPANSION expansion\] \[NONSCALING\]
///
/// Creates an empty Bloom filter with the given error rate and initial
/// capacity.
#[derive(Clone)]
pub struct BfReserve {
    key: String,
    error_rate: f64,
    capacity: i64,
    expansion: Option<i64>,
    nonscaling: bool,
}

impl BfReserve {
    pub fn new(key: impl Into<String>, error_rate: f64, capacity: i64) -> Self {
        Self {
            key: key.into(),
            error_rate,
            capacity,
            expansion: None,
            nonscaling: false,
        }
    }

    /// Set the expansion factor for sub-filters.
    pub fn expansion(mut self, expansion: i64) -> Self {
        self.expansion = Some(expansion);
        self
    }

    /// Prevent the filter from scaling (no additional sub-filters).
    pub fn nonscaling(mut self) -> Self {
        self.nonscaling = true;
        self
    }
}

impl Command for BfReserve {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("BF.RESERVE"),
            bulk(self.key.as_str()),
            bulk(self.error_rate.to_string()),
            bulk(self.capacity.to_string()),
        ];
        if let Some(exp) = self.expansion {
            args.push(bulk("EXPANSION"));
            args.push(bulk(exp.to_string()));
        }
        if self.nonscaling {
            args.push(bulk("NONSCALING"));
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
        "BF.RESERVE"
    }
}

/// BF.INFO key
///
/// Returns information about the Bloom filter at `key` as a raw Frame
/// (key-value pairs).
#[derive(Clone)]
pub struct BfInfo {
    key: String,
}

impl BfInfo {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for BfInfo {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("BF.INFO"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "BF.INFO"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// BF.INSERT key \[CAPACITY cap\] \[ERROR error\] \[EXPANSION exp\]
/// \[NOCREATE\] \[NONSCALING\] ITEMS item \[item ...\]
///
/// Adds one or more items to a Bloom filter, creating it if it does not exist.
/// Supports builder-style configuration.
#[derive(Clone)]
pub struct BfInsert {
    key: String,
    capacity: Option<i64>,
    error: Option<f64>,
    expansion: Option<i64>,
    nocreate: bool,
    nonscaling: bool,
    items: Vec<String>,
}

impl BfInsert {
    pub fn new(key: impl Into<String>, items: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            capacity: None,
            error: None,
            expansion: None,
            nocreate: false,
            nonscaling: false,
            items: items.into_iter().map(Into::into).collect(),
        }
    }

    /// Set the desired capacity for auto-created filters.
    pub fn capacity(mut self, cap: i64) -> Self {
        self.capacity = Some(cap);
        self
    }

    /// Set the desired error rate for auto-created filters.
    pub fn error(mut self, error: f64) -> Self {
        self.error = Some(error);
        self
    }

    /// Set the expansion factor for auto-created filters.
    pub fn expansion(mut self, exp: i64) -> Self {
        self.expansion = Some(exp);
        self
    }

    /// Do not create the filter if it does not exist.
    pub fn nocreate(mut self) -> Self {
        self.nocreate = true;
        self
    }

    /// Prevent the auto-created filter from scaling.
    pub fn nonscaling(mut self) -> Self {
        self.nonscaling = true;
        self
    }
}

impl Command for BfInsert {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("BF.INSERT"), bulk(self.key.as_str())];
        if let Some(cap) = self.capacity {
            args.push(bulk("CAPACITY"));
            args.push(bulk(cap.to_string()));
        }
        if let Some(err) = self.error {
            args.push(bulk("ERROR"));
            args.push(bulk(err.to_string()));
        }
        if let Some(exp) = self.expansion {
            args.push(bulk("EXPANSION"));
            args.push(bulk(exp.to_string()));
        }
        if self.nocreate {
            args.push(bulk("NOCREATE"));
        }
        if self.nonscaling {
            args.push(bulk("NONSCALING"));
        }
        args.push(bulk("ITEMS"));
        for item in &self.items {
            args.push(bulk(item.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_bool_array(frame)
    }

    fn name(&self) -> &str {
        "BF.INSERT"
    }
}

// ===========================================================================
// Cuckoo filter commands
// ===========================================================================

/// CF.ADD key item
///
/// Adds an item to the Cuckoo filter at `key`. Returns `true` if the item was
/// successfully added.
#[derive(Clone)]
pub struct CfAdd {
    key: String,
    item: String,
}

impl CfAdd {
    pub fn new(key: impl Into<String>, item: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for CfAdd {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CF.ADD"),
            bulk(self.key.as_str()),
            bulk(self.item.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n != 0),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CF.ADD"
    }
}

/// CF.ADDNX key item
///
/// Adds an item to the Cuckoo filter only if it does not already exist.
/// Returns `true` if the item was added, `false` if it may already exist.
#[derive(Clone)]
pub struct CfAddNx {
    key: String,
    item: String,
}

impl CfAddNx {
    pub fn new(key: impl Into<String>, item: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for CfAddNx {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CF.ADDNX"),
            bulk(self.key.as_str()),
            bulk(self.item.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n != 0),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CF.ADDNX"
    }
}

/// CF.EXISTS key item
///
/// Checks whether an item may exist in the Cuckoo filter at `key`.
#[derive(Clone)]
pub struct CfExists {
    key: String,
    item: String,
}

impl CfExists {
    pub fn new(key: impl Into<String>, item: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for CfExists {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CF.EXISTS"),
            bulk(self.key.as_str()),
            bulk(self.item.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n != 0),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CF.EXISTS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// CF.MEXISTS key item \[item ...\]
///
/// Checks whether one or more items may exist in the Cuckoo filter at `key`.
#[derive(Clone)]
pub struct CfMExists {
    key: String,
    items: Vec<String>,
}

impl CfMExists {
    pub fn new(key: impl Into<String>, items: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            items: items.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for CfMExists {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("CF.MEXISTS"), bulk(self.key.as_str())];
        for item in &self.items {
            args.push(bulk(item.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_bool_array(frame)
    }

    fn name(&self) -> &str {
        "CF.MEXISTS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// CF.DEL key item
///
/// Deletes an item from the Cuckoo filter at `key`. Returns `true` if the
/// item was found and deleted, `false` otherwise.
#[derive(Clone)]
pub struct CfDel {
    key: String,
    item: String,
}

impl CfDel {
    pub fn new(key: impl Into<String>, item: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for CfDel {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CF.DEL"),
            bulk(self.key.as_str()),
            bulk(self.item.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n != 0),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CF.DEL"
    }
}

/// CF.COUNT key item
///
/// Returns the number of times an item may be in the Cuckoo filter.
#[derive(Clone)]
pub struct CfCount {
    key: String,
    item: String,
}

impl CfCount {
    pub fn new(key: impl Into<String>, item: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            item: item.into(),
        }
    }
}

impl Command for CfCount {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CF.COUNT"),
            bulk(self.key.as_str()),
            bulk(self.item.as_str()),
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
        "CF.COUNT"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// CF.RESERVE key capacity \[BUCKETSIZE size\] \[MAXITERATIONS iter\]
/// \[EXPANSION exp\]
///
/// Creates an empty Cuckoo filter with the given capacity.
#[derive(Clone)]
pub struct CfReserve {
    key: String,
    capacity: i64,
    bucketsize: Option<i64>,
    maxiterations: Option<i64>,
    expansion: Option<i64>,
}

impl CfReserve {
    pub fn new(key: impl Into<String>, capacity: i64) -> Self {
        Self {
            key: key.into(),
            capacity,
            bucketsize: None,
            maxiterations: None,
            expansion: None,
        }
    }

    /// Set the number of items per bucket.
    pub fn bucketsize(mut self, size: i64) -> Self {
        self.bucketsize = Some(size);
        self
    }

    /// Set the maximum number of cuckoo kicks before declaring failure.
    pub fn maxiterations(mut self, iter: i64) -> Self {
        self.maxiterations = Some(iter);
        self
    }

    /// Set the expansion factor when the filter is full.
    pub fn expansion(mut self, exp: i64) -> Self {
        self.expansion = Some(exp);
        self
    }
}

impl Command for CfReserve {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("CF.RESERVE"),
            bulk(self.key.as_str()),
            bulk(self.capacity.to_string()),
        ];
        if let Some(bs) = self.bucketsize {
            args.push(bulk("BUCKETSIZE"));
            args.push(bulk(bs.to_string()));
        }
        if let Some(mi) = self.maxiterations {
            args.push(bulk("MAXITERATIONS"));
            args.push(bulk(mi.to_string()));
        }
        if let Some(exp) = self.expansion {
            args.push(bulk("EXPANSION"));
            args.push(bulk(exp.to_string()));
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
        "CF.RESERVE"
    }
}

/// CF.INFO key
///
/// Returns information about the Cuckoo filter at `key` as a raw Frame.
#[derive(Clone)]
pub struct CfInfo {
    key: String,
}

impl CfInfo {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for CfInfo {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CF.INFO"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "CF.INFO"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// CF.INSERT key \[CAPACITY cap\] \[NOCREATE\] ITEMS item \[item ...\]
///
/// Adds one or more items to a Cuckoo filter, creating it if it does not
/// exist. Returns a vector of booleans.
#[derive(Clone)]
pub struct CfInsert {
    key: String,
    capacity: Option<i64>,
    nocreate: bool,
    items: Vec<String>,
}

impl CfInsert {
    pub fn new(key: impl Into<String>, items: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            capacity: None,
            nocreate: false,
            items: items.into_iter().map(Into::into).collect(),
        }
    }

    /// Set the desired capacity for auto-created filters.
    pub fn capacity(mut self, cap: i64) -> Self {
        self.capacity = Some(cap);
        self
    }

    /// Do not create the filter if it does not exist.
    pub fn nocreate(mut self) -> Self {
        self.nocreate = true;
        self
    }
}

impl Command for CfInsert {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("CF.INSERT"), bulk(self.key.as_str())];
        if let Some(cap) = self.capacity {
            args.push(bulk("CAPACITY"));
            args.push(bulk(cap.to_string()));
        }
        if self.nocreate {
            args.push(bulk("NOCREATE"));
        }
        args.push(bulk("ITEMS"));
        for item in &self.items {
            args.push(bulk(item.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_bool_array(frame)
    }

    fn name(&self) -> &str {
        "CF.INSERT"
    }
}

/// CF.INSERTNX key \[CAPACITY cap\] \[NOCREATE\] ITEMS item \[item ...\]
///
/// Adds one or more items to a Cuckoo filter only if they do not already
/// exist, creating the filter if needed. Returns a vector of booleans.
#[derive(Clone)]
pub struct CfInsertNx {
    key: String,
    capacity: Option<i64>,
    nocreate: bool,
    items: Vec<String>,
}

impl CfInsertNx {
    pub fn new(key: impl Into<String>, items: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            capacity: None,
            nocreate: false,
            items: items.into_iter().map(Into::into).collect(),
        }
    }

    /// Set the desired capacity for auto-created filters.
    pub fn capacity(mut self, cap: i64) -> Self {
        self.capacity = Some(cap);
        self
    }

    /// Do not create the filter if it does not exist.
    pub fn nocreate(mut self) -> Self {
        self.nocreate = true;
        self
    }
}

impl Command for CfInsertNx {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("CF.INSERTNX"), bulk(self.key.as_str())];
        if let Some(cap) = self.capacity {
            args.push(bulk("CAPACITY"));
            args.push(bulk(cap.to_string()));
        }
        if self.nocreate {
            args.push(bulk("NOCREATE"));
        }
        args.push(bulk("ITEMS"));
        for item in &self.items {
            args.push(bulk(item.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_bool_array(frame)
    }

    fn name(&self) -> &str {
        "CF.INSERTNX"
    }
}

// ---------------------------------------------------------------------------
// Cardinality and chunked dump/restore (filter migration)
// ---------------------------------------------------------------------------

/// Parse a `BF.SCANDUMP` / `CF.SCANDUMP` reply: a two-element array of the next
/// iterator (`0` when the dump is complete) and the binary chunk for this step.
fn parse_scandump(frame: Frame) -> Result<(u64, Bytes), RedisError> {
    match frame {
        Frame::Array(Some(items)) if items.len() == 2 => {
            let mut it = items.into_iter();
            let iterator = match it.next() {
                Some(Frame::Integer(n)) => n as u64,
                other => {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "integer iterator",
                        actual: format!("{other:?}"),
                    });
                }
            };
            let chunk = match it.next() {
                Some(Frame::BulkString(Some(data))) => data,
                Some(Frame::BulkString(None)) | Some(Frame::Null) => Bytes::new(),
                other => {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "bulk chunk",
                        actual: format!("{other:?}"),
                    });
                }
            };
            Ok((iterator, chunk))
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "two-element array",
            actual: format!("{other:?}"),
        }),
    }
}

/// BF.CARD key
///
/// Returns the cardinality of the Bloom filter at `key` -- the number of items
/// that have been added. Returns `0` if the key does not exist.
#[derive(Clone)]
pub struct BfCard {
    key: String,
}

impl BfCard {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for BfCard {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("BF.CARD"), bulk(self.key.as_str())])
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
        "BF.CARD"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// BF.SCANDUMP key iterator
///
/// Begins or continues an incremental save of the Bloom filter at `key`. Start
/// with iterator `0`; each call returns `(next_iterator, chunk)`. A returned
/// iterator of `0` signals the dump is complete. Replay the chunks into another
/// server with [`BfLoadChunk`] to migrate a filter.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_commands::{BfLoadChunk, BfScanDump};
/// use redis_tower_core::RedisConnection;
///
/// let mut src = RedisConnection::connect("127.0.0.1:6379").await?;
/// let mut dst = RedisConnection::connect("127.0.0.1:6380").await?;
///
/// let mut iter = 0;
/// loop {
///     let (next, chunk) = src.execute(BfScanDump::new("filter", iter)).await?;
///     if next == 0 {
///         break;
///     }
///     dst.execute(BfLoadChunk::new("filter", next, chunk)).await?;
///     iter = next;
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct BfScanDump {
    key: String,
    iterator: u64,
}

impl BfScanDump {
    pub fn new(key: impl Into<String>, iterator: u64) -> Self {
        Self {
            key: key.into(),
            iterator,
        }
    }
}

impl Command for BfScanDump {
    type Response = (u64, Bytes);

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("BF.SCANDUMP"),
            bulk(self.key.as_str()),
            bulk(self.iterator.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_scandump(frame)
    }

    fn name(&self) -> &str {
        "BF.SCANDUMP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// BF.LOADCHUNK key iterator data
///
/// Restores a chunk previously produced by [`BfScanDump`]. Call once per chunk,
/// in iteration order, to rebuild a Bloom filter on another server.
#[derive(Clone)]
pub struct BfLoadChunk {
    key: String,
    iterator: u64,
    data: Bytes,
}

impl BfLoadChunk {
    pub fn new(key: impl Into<String>, iterator: u64, data: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            iterator,
            data: data.into(),
        }
    }
}

impl Command for BfLoadChunk {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("BF.LOADCHUNK"),
            bulk(self.key.as_str()),
            bulk(self.iterator.to_string()),
            bulk(self.data.as_ref()),
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
        "BF.LOADCHUNK"
    }
}

/// CF.SCANDUMP key iterator
///
/// The Cuckoo-filter counterpart of [`BfScanDump`]. Start with iterator `0`;
/// each call returns `(next_iterator, chunk)`, with a returned iterator of `0`
/// marking the end. Restore with [`CfLoadChunk`].
#[derive(Clone)]
pub struct CfScanDump {
    key: String,
    iterator: u64,
}

impl CfScanDump {
    pub fn new(key: impl Into<String>, iterator: u64) -> Self {
        Self {
            key: key.into(),
            iterator,
        }
    }
}

impl Command for CfScanDump {
    type Response = (u64, Bytes);

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CF.SCANDUMP"),
            bulk(self.key.as_str()),
            bulk(self.iterator.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_scandump(frame)
    }

    fn name(&self) -> &str {
        "CF.SCANDUMP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// CF.LOADCHUNK key iterator data
///
/// Restores a chunk previously produced by [`CfScanDump`]. Call once per chunk,
/// in iteration order, to rebuild a Cuckoo filter on another server.
#[derive(Clone)]
pub struct CfLoadChunk {
    key: String,
    iterator: u64,
    data: Bytes,
}

impl CfLoadChunk {
    pub fn new(key: impl Into<String>, iterator: u64, data: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            iterator,
            data: data.into(),
        }
    }
}

impl Command for CfLoadChunk {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CF.LOADCHUNK"),
            bulk(self.key.as_str()),
            bulk(self.iterator.to_string()),
            bulk(self.data.as_ref()),
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
        "CF.LOADCHUNK"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    #[test]
    fn idempotency_flags() {
        // Read-only commands are safe to retry.
        assert!(BfExists::new("k", "i").idempotent());
        // Mutating commands keep the default (false).
        assert!(!BfAdd::new("k", "i").idempotent());
    }

    #[test]
    fn bf_card_to_frame() {
        assert_eq!(
            BfCard::new("k").to_frame(),
            array(vec![bulk("BF.CARD"), bulk("k")])
        );
        assert!(BfCard::new("k").idempotent());
    }

    #[test]
    fn bf_card_parse_integer() {
        assert_eq!(
            BfCard::new("k").parse_response(Frame::Integer(3)).unwrap(),
            3
        );
    }

    #[test]
    fn bf_scandump_to_frame_and_idempotent() {
        assert_eq!(
            BfScanDump::new("k", 0).to_frame(),
            array(vec![bulk("BF.SCANDUMP"), bulk("k"), bulk("0")])
        );
        assert!(BfScanDump::new("k", 0).idempotent());
    }

    #[test]
    fn cf_scandump_to_frame() {
        assert_eq!(
            CfScanDump::new("k", 5).to_frame(),
            array(vec![bulk("CF.SCANDUMP"), bulk("k"), bulk("5")])
        );
        assert!(CfScanDump::new("k", 5).idempotent());
    }

    #[test]
    fn scandump_parse_iterator_and_chunk() {
        let frame = array(vec![Frame::Integer(7), bulk("chunkdata")]);
        let (iter, data) = parse_scandump(frame).unwrap();
        assert_eq!(iter, 7);
        assert_eq!(&data[..], b"chunkdata");
    }

    #[test]
    fn scandump_parse_complete_empty_chunk() {
        let frame = array(vec![Frame::Integer(0), Frame::BulkString(None)]);
        let (iter, data) = parse_scandump(frame).unwrap();
        assert_eq!(iter, 0);
        assert!(data.is_empty());
    }

    #[test]
    fn bf_loadchunk_to_frame() {
        let cmd = BfLoadChunk::new("k", 7, Bytes::from_static(b"chunkdata"));
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("BF.LOADCHUNK"),
                bulk("k"),
                bulk("7"),
                bulk("chunkdata"),
            ])
        );
    }

    #[test]
    fn cf_loadchunk_to_frame() {
        let cmd = CfLoadChunk::new("k", 7, Bytes::from_static(b"data"));
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("CF.LOADCHUNK"),
                bulk("k"),
                bulk("7"),
                bulk("data"),
            ])
        );
    }
}
