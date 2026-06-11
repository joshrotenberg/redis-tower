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
